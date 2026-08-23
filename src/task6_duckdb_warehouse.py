# ============================================================================
# ANALYSIS LAYER: load pipeline outputs into a DuckDB warehouse (INCREMENTAL)
# ============================================================================
# Builds a structured local DuckDB warehouse and loads it INCREMENTALLY via
# INSERT ... ON CONFLICT (upsert): new rows are INSERTed, existing rows (matched
# on the primary key) are UPDATEd. So a change to a source record propagates on
# the next run WITHOUT rebuilding the whole table. Tables use
# CREATE TABLE IF NOT EXISTS so data persists across runs.
#
#   Database : CCT_Database   (a single .duckdb file, attached by name)
#   Schema   : CCT_Schema
#   Tables   : fact_service_requests  (PK: notification_number)
#              dim_hexagons           (PK: index)
#              atlantis_wind_sample   (PK: notification_number)
#
# TYPE NOTES:
#   - notification_number / reference_number are kept as VARCHAR. They LOOK
#     numeric but have LEADING ZEROS (e.g. "000400583534"); storing them as an
#     integer would drop the zeros and corrupt the identifier / break the join
#     to the source. (DuckDB VARCHAR is Unicode; there is no NVARCHAR.)
#   - creation_timestamp / completion_timestamp are real datetimes with a
#     timezone offset (e.g. "...+02:00"), so they are TIMESTAMPTZ.
# ============================================================================


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 1: imports, logging, config, robust paths
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os                                                # path resolution (run from anywhere)
import io                                                # in-memory bytes buffer for downloads
import gzip                                              # decompress .csv.gz files
import json                                              # parse S3 SELECT JSON output
import time                                              # measure timings
import logging                                           # structured logging
import boto3                                             # AWS SDK / S3 access
import pandas as pd                                      # dataframes (staging before upsert)
import duckdb                                            # embedded analytical warehouse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root

# --- warehouse identity ---
DB_PATH = os.path.join(ROOT, "data", "CCT_Database.duckdb")  # the physical warehouse file
DB_NAME = "CCT_Database"                                     # logical database name (ATTACH alias)
SCHEMA = "CCT_Schema"                                        # schema inside the database
FQ = f"{DB_NAME}.{SCHEMA}"                                   # fully-qualified prefix

# --- S3 source config ---
BUCKET = "cct-ds-code-challenge-input-data"
SR_HEX_KEY = "sr_hex.csv.gz"                             # service requests WITH the h3 index
BIG_KEY = "city-hex-polygons-8-10.geojson"              # multi-resolution hexagon source
REGION = "af-south-1"
PROFILE = "cct"

ATLANTIS_CSV = os.path.join(ROOT, "data", "atlantis_subsample_wind.csv")  # Task 5 output

# --- explicit fact-table schema (column -> DuckDB type); PK declared in ensure_table ---
FACT_COLUMNS = {
    "notification_number": "VARCHAR",       # identifier with leading zeros -> text
    "reference_number": "VARCHAR",          # identifier -> text
    "creation_timestamp": "TIMESTAMPTZ",    # real datetime with tz offset
    "completion_timestamp": "TIMESTAMPTZ",  # real datetime with tz offset
    "directorate": "VARCHAR",
    "department": "VARCHAR",
    "branch": "VARCHAR",
    "section": "VARCHAR",
    "code_group": "VARCHAR",
    "code": "VARCHAR",
    "cause_code_group": "VARCHAR",
    "cause_code": "VARCHAR",
    "official_suburb": "VARCHAR",
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "h3_level8_index": "VARCHAR",
}
# columns that must be parsed from text to real datetimes before the upsert
FACT_TIMESTAMP_COLS = ["creation_timestamp", "completion_timestamp"]

# --- explicit hexagon dimension schema ---
HEX_COLUMNS = {
    "index": "VARCHAR",          # the H3 index string
    "centroid_lat": "DOUBLE",
    "centroid_lon": "DOUBLE",
    "resolution": "BIGINT",
}


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 2: S3 helpers - client, gzip CSV reader, and S3 SELECT for hexagons
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_s3_client():                                     # build an S3 client via the 'cct' profile
    return boto3.Session(profile_name=PROFILE).client("s3", region_name=REGION)


def read_csv_gz_from_s3(s3, key):                        # download + decompress + parse a .csv.gz
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    raw = obj["Body"].read()
    with gzip.open(io.BytesIO(raw), "rt") as f:
        # keep identifier columns as strings so leading zeros are preserved
        return pd.read_csv(f, dtype={"notification_number": str, "reference_number": str})


def s3_select_hexagons(s3):                              # S3 SELECT only the resolution-8 hexagons
    query = ("SELECT s.properties.index, s.properties.centroid_lat, "
             "s.properties.centroid_lon, s.properties.resolution "
             "FROM S3Object[*].features[*] s WHERE s.properties.resolution = 8")
    resp = s3.select_object_content(
        Bucket=BUCKET, Key=BIG_KEY, ExpressionType="SQL", Expression=query,
        InputSerialization={"JSON": {"Type": "DOCUMENT"}},
        OutputSerialization={"JSON": {}},
    )
    records = ""
    for event in resp["Payload"]:
        if "Records" in event:
            records += event["Records"]["Payload"].decode("utf-8")
    rows = [json.loads(line) for line in records.splitlines() if line.strip()]
    return pd.DataFrame(rows)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 3: schema setup + a generic incremental UPSERT helper
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def ensure_schema(con):                                  # create the schema (safe to re-run)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{SCHEMA}")
    logger.info(f"Schema ready: {DB_NAME}.{SCHEMA}")


def ensure_table(con, table, columns, key):              # create a table WITH a primary key if absent
    # Build "col TYPE" definitions, marking the key column PRIMARY KEY.
    col_defs = ",\n            ".join(
        f'"{name}" {sqltype}{" PRIMARY KEY" if name == key else ""}'
        for name, sqltype in columns.items()
    )
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {FQ}.{table} (
            {col_defs}
        )
    """)                                                 # IF NOT EXISTS -> keeps data across runs


def upsert(con, table, key, df):                         # generic incremental upsert
    before = con.execute(f"SELECT COUNT(*) FROM {FQ}.{table}").fetchone()[0]  # rows before
    cols = list(df.columns)                              # columns coming from the source
    non_key = [c for c in cols if c != key]              # everything except the key gets updated
    col_list = ", ".join(f'"{c}"' for c in cols)         # column list for INSERT
    set_list = ", ".join(f'"{c}" = excluded."{c}"' for c in non_key)  # UPDATE ... = incoming value

    con.register("staging_df", df)                       # expose the source DataFrame to SQL
    # matched keys -> UPDATE non-key columns; new keys -> INSERT
    con.execute(f"""
        INSERT INTO {FQ}.{table} ({col_list})
        SELECT {col_list} FROM staging_df
        ON CONFLICT ("{key}") DO UPDATE SET {set_list}
    """)
    con.unregister("staging_df")                         # detach the temporary view

    after = con.execute(f"SELECT COUNT(*) FROM {FQ}.{table}").fetchone()[0]   # rows after
    inserted = after - before                            # net new rows
    updated = len(df) - inserted                         # the rest matched existing keys (updates)
    logger.info(f"Upsert {FQ}.{table}: {inserted} inserted, {updated} updated "
                f"(table now {after} rows)")


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 4: build the warehouse incrementally (create-if-absent, then upsert)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def build_warehouse(con, s3):
    ensure_schema(con)

    # --- FACT table: service requests (upsert on notification_number) ---
    t0 = time.time()
    sr = read_csv_gz_from_s3(s3, SR_HEX_KEY)              # fresh source snapshot
    sr = sr[[c for c in FACT_COLUMNS if c in sr.columns]]  # keep only modelled columns
    for col in FACT_TIMESTAMP_COLS:                      # parse text -> real datetimes (for TIMESTAMPTZ)
        if col in sr.columns:
            sr[col] = pd.to_datetime(sr[col], utc=True, errors="coerce")
    ensure_table(con, "fact_service_requests", FACT_COLUMNS, key="notification_number")
    upsert(con, "fact_service_requests", "notification_number", sr)
    logger.info(f"fact_service_requests loaded in {time.time()-t0:.2f}s")

    # --- DIMENSION table: hexagons (upsert on index) ---
    t1 = time.time()
    hexes = s3_select_hexagons(s3)
    hexes = hexes[[c for c in HEX_COLUMNS if c in hexes.columns]]
    ensure_table(con, "dim_hexagons", HEX_COLUMNS, key="index")
    upsert(con, "dim_hexagons", "index", hexes)
    logger.info(f"dim_hexagons loaded in {time.time()-t1:.2f}s")

    # --- Atlantis + wind sample (upsert on notification_number, if file exists) ---
    if os.path.exists(ATLANTIS_CSV):
        t2 = time.time()
        atl = pd.read_csv(ATLANTIS_CSV,
                          dtype={"notification_number": str, "reference_number": str})
        # derive an explicit schema from the DataFrame (Task 5 columns can vary)
        atl_columns = {c: _duck_type(c, atl[c]) for c in atl.columns}
        ensure_table(con, "atlantis_wind_sample", atl_columns, key="notification_number")
        upsert(con, "atlantis_wind_sample", "notification_number", atl)
        logger.info(f"atlantis_wind_sample loaded in {time.time()-t2:.2f}s")
    else:
        logger.warning(f"{ATLANTIS_CSV} not found - run task5 first to include this table")


def _duck_type(name, series):                            # map a column to a DuckDB type
    # identifiers stay text (leading zeros); *_timestamp columns are datetimes
    if name in ("notification_number", "reference_number"):
        return "VARCHAR"
    if name.endswith("timestamp"):
        return "TIMESTAMPTZ"
    dt = str(series.dtype)
    if "int" in dt:
        return "BIGINT"
    if "float" in dt:
        return "DOUBLE"
    if "bool" in dt:
        return "BOOLEAN"
    return "VARCHAR"


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 5: analytical queries (timestamps are now real TIMESTAMPTZ - no casting)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def run_analysis(con):
    logger.info("Top 10 directorates by request count:")
    print(con.execute(f"""
        SELECT directorate, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        GROUP BY directorate
        ORDER BY request_count DESC
        LIMIT 10
    """).df().to_string(index=False))

    logger.info("Completion time per directorate (hours) - top 10 by volume:")
    print(con.execute(f"""
        SELECT
            directorate,
            COUNT(*) AS completed_requests,
            ROUND(MEDIAN(DATE_DIFF('hour', creation_timestamp, completion_timestamp)), 1) AS median_hours,
            ROUND(QUANTILE_CONT(DATE_DIFF('hour', creation_timestamp, completion_timestamp), 0.8), 1) AS p80_hours
        FROM {FQ}.fact_service_requests
        WHERE completion_timestamp IS NOT NULL
          AND creation_timestamp IS NOT NULL
          AND directorate IS NOT NULL
        GROUP BY directorate
        ORDER BY completed_requests DESC
        LIMIT 10
    """).df().to_string(index=False))

    logger.info("Top 10 suburbs city-wide by request count:")
    print(con.execute(f"""
        SELECT official_suburb, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE official_suburb IS NOT NULL
        GROUP BY official_suburb
        ORDER BY request_count DESC
        LIMIT 10
    """).df().to_string(index=False))


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 6: orchestrate - attach the named database, build incrementally, analyse
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
    os.makedirs(os.path.join(ROOT, "data"), exist_ok=True)
    s3 = get_s3_client()
    con = duckdb.connect()                               # root (in-memory) connection
    try:
        con.execute(f"ATTACH '{DB_PATH}' AS {DB_NAME}")  # attach the warehouse file by name
        build_warehouse(con, s3)                         # create-if-absent + incremental upsert
        run_analysis(con)                                # example analytics
        logger.info(f"Warehouse ready: {FQ} (file: {DB_PATH})")
    finally:
        con.close()


if __name__ == "__main__":
    main()
