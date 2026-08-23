# ============================================================================
# ANALYSIS LAYER: load the pipeline outputs into a structured DuckDB warehouse
# ============================================================================
# Completes the DE flow (Extract -> Transform -> Load -> Analyse) by loading the
# assessment datasets into a local DuckDB warehouse with a proper structure:
#
#   Database : CCT_Database   (the .duckdb file, attached under this logical name)
#   Schema   : CCT_Schema
#   Tables   : CCT_Database.CCT_Schema.fact_service_requests  (~942K requests)
#              CCT_Database.CCT_Schema.dim_hexagons           (3,832 hexagons)
#              CCT_Database.CCT_Schema.atlantis_wind_sample   (7,213 rows)
#
# DuckDB is an embedded warehouse: no server, just a single local file. We use
# ATTACH ... AS CCT_Database so the logical database name is explicit and not
# just derived from the file path.
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
import pandas as pd                                      # dataframes (staging before load)
import duckdb                                            # embedded analytical warehouse

logging.basicConfig(                                     # configure logging once
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root

# --- warehouse identity ---
DB_PATH = os.path.join(ROOT, "data", "CCT_Database.duckdb")  # the physical warehouse file
DB_NAME = "CCT_Database"                                     # logical database name (ATTACH alias)
SCHEMA = "CCT_Schema"                                        # schema inside the database
FQ = f"{DB_NAME}.{SCHEMA}"                                   # fully-qualified prefix for tables

# --- S3 source config ---
BUCKET = "cct-ds-code-challenge-input-data"              # the assessment data bucket
SR_HEX_KEY = "sr_hex.csv.gz"                             # service requests WITH the h3 index
BIG_KEY = "city-hex-polygons-8-10.geojson"              # multi-resolution hexagon source
REGION = "af-south-1"                                    # bucket region
PROFILE = "cct"                                          # local AWS profile holding the creds

ATLANTIS_CSV = os.path.join(ROOT, "data", "atlantis_subsample_wind.csv")  # Task 5 output


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 2: S3 helpers - client, gzip CSV reader, and S3 SELECT for hexagons
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_s3_client():                                     # build an S3 client via the 'cct' profile
    return boto3.Session(profile_name=PROFILE).client("s3", region_name=REGION)


def read_csv_gz_from_s3(s3, key):                        # download + decompress + parse a .csv.gz
    obj = s3.get_object(Bucket=BUCKET, Key=key)          # fetch the S3 object
    raw = obj["Body"].read()                             # read its bytes into memory
    with gzip.open(io.BytesIO(raw), "rt") as f:          # decompress in memory (text mode)
        return pd.read_csv(f)                            # parse into a DataFrame


def s3_select_hexagons(s3):                              # S3 SELECT only the resolution-8 hexagons
    # Filter server-side so we never download the full 108 MB file.
    query = ("SELECT s.properties.index, s.properties.centroid_lat, "
             "s.properties.centroid_lon, s.properties.resolution "
             "FROM S3Object[*].features[*] s WHERE s.properties.resolution = 8")
    resp = s3.select_object_content(
        Bucket=BUCKET, Key=BIG_KEY, ExpressionType="SQL", Expression=query,
        InputSerialization={"JSON": {"Type": "DOCUMENT"}},   # input is a JSON document
        OutputSerialization={"JSON": {}},                    # stream results as JSON
    )
    records = ""                                         # accumulate the streamed result chunks
    for event in resp["Payload"]:                        # iterate the event stream
        if "Records" in event:                           # a chunk of matching data
            records += event["Records"]["Payload"].decode("utf-8")
    rows = [json.loads(line) for line in records.splitlines() if line.strip()]  # parse each line
    return pd.DataFrame(rows)                            # -> DataFrame: index, centroid_lat/lon, resolution


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 3: build the warehouse - create the schema and load the three tables
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def build_warehouse(con, s3):                            # con = a DuckDB connection (DB already attached)
    # Create the schema inside CCT_Database (safe to re-run).
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{SCHEMA}")
    logger.info(f"Schema ready: {DB_NAME}.{SCHEMA}")

    # --- FACT table: H3-enriched service requests (from the ground-truth file) ---
    t0 = time.time()
    sr = read_csv_gz_from_s3(s3, SR_HEX_KEY)             # download the requests + h3 index
    con.execute(f"DROP TABLE IF EXISTS {FQ}.fact_service_requests")  # fresh table each run
    con.register("sr_df", sr)                            # expose the DataFrame to SQL
    con.execute(f"CREATE TABLE {FQ}.fact_service_requests AS SELECT * FROM sr_df")  # load
    con.unregister("sr_df")                              # detach the temporary view
    logger.info(f"Loaded {FQ}.fact_service_requests ({len(sr)} rows) in {time.time()-t0:.2f}s")

    # --- DIMENSION table: resolution-8 hexagons (index + centroid location) ---
    t1 = time.time()
    hexes = s3_select_hexagons(s3)                       # S3 SELECT the hexagons
    con.execute(f"DROP TABLE IF EXISTS {FQ}.dim_hexagons")
    con.register("hex_df", hexes)
    con.execute(f"CREATE TABLE {FQ}.dim_hexagons AS SELECT * FROM hex_df")
    con.unregister("hex_df")
    logger.info(f"Loaded {FQ}.dim_hexagons ({len(hexes)} rows) in {time.time()-t1:.2f}s")

    # --- Atlantis + wind sample (Task 5 output, loaded if the file exists) ---
    if os.path.exists(ATLANTIS_CSV):
        t2 = time.time()
        atl = pd.read_csv(ATLANTIS_CSV)                  # read the Task 5 augmented subsample
        con.execute(f"DROP TABLE IF EXISTS {FQ}.atlantis_wind_sample")
        con.register("atl_df", atl)
        con.execute(f"CREATE TABLE {FQ}.atlantis_wind_sample AS SELECT * FROM atl_df")
        con.unregister("atl_df")
        logger.info(f"Loaded {FQ}.atlantis_wind_sample ({len(atl)} rows) in {time.time()-t2:.2f}s")
    else:
        logger.warning(f"{ATLANTIS_CSV} not found - run task5 first to include this table")


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 4: analytical queries (all tables referenced as CCT_Database.CCT_Schema.*)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def run_analysis(con):
    # 1) Top 10 hexagons by request count (skip the "0" no-geolocation bucket)
    logger.info("Top 10 hexagons by request count:")
    q1 = con.execute(f"""
        SELECT h3_level8_index, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE h3_level8_index <> '0'
        GROUP BY h3_level8_index
        ORDER BY request_count DESC
        LIMIT 10
    """).df()
    print(q1.to_string(index=False))

    # 2) Requests per directorate (which parts of the City get the most requests)
    logger.info("Top 10 directorates by request count:")
    q2 = con.execute(f"""
        SELECT directorate, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        GROUP BY directorate
        ORDER BY request_count DESC
        LIMIT 10
    """).df()
    print(q2.to_string(index=False))

    # 3) Join FACT -> DIM to attach hexagon centroids (busiest hexagons, located)
    logger.info("Top 5 busiest hexagons with their centroid location:")
    q3 = con.execute(f"""
        SELECT f.h3_level8_index,
               COUNT(*) AS request_count,
               d.centroid_lat, d.centroid_lon
        FROM {FQ}.fact_service_requests f
        JOIN {FQ}.dim_hexagons d ON f.h3_level8_index = d."index"
        GROUP BY f.h3_level8_index, d.centroid_lat, d.centroid_lon
        ORDER BY request_count DESC
        LIMIT 5
    """).df()
    print(q3.to_string(index=False))

    # 4) Time-to-complete per directorate (median & 80th percentile, in hours)
    #    A core service-delivery KPI: how long requests take to resolve.
    logger.info("Completion time per directorate (hours) - top 10 by volume:")
    q4 = con.execute(f"""
        SELECT
            directorate,
            COUNT(*) AS completed_requests,
            ROUND(MEDIAN(
                DATE_DIFF('hour',
                          CAST(creation_timestamp AS TIMESTAMP),
                          CAST(completion_timestamp AS TIMESTAMP))
            ), 1) AS median_hours,
            ROUND(QUANTILE_CONT(
                DATE_DIFF('hour',
                          CAST(creation_timestamp AS TIMESTAMP),
                          CAST(completion_timestamp AS TIMESTAMP)), 0.8
            ), 1) AS p80_hours
        FROM {FQ}.fact_service_requests
        WHERE completion_timestamp IS NOT NULL
          AND creation_timestamp IS NOT NULL
          AND directorate IS NOT NULL
        GROUP BY directorate
        ORDER BY completed_requests DESC
        LIMIT 10
    """).df()
    print(q4.to_string(index=False))

    # 5) City-wide top 10 suburbs by request volume (demand hotspots)
    logger.info("Top 10 suburbs city-wide by request count:")
    q5 = con.execute(f"""
        SELECT official_suburb, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE official_suburb IS NOT NULL
        GROUP BY official_suburb
        ORDER BY request_count DESC
        LIMIT 10
    """).df()
    print(q5.to_string(index=False))

    # 6) Requests per month (temporal trend - e.g. the COVID-19 lockdown dip)
    logger.info("Requests per month:")
    q6 = con.execute(f"""
        SELECT
            DATE_TRUNC('month', CAST(creation_timestamp AS TIMESTAMP)) AS month,
            COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE creation_timestamp IS NOT NULL
        GROUP BY month
        ORDER BY month
    """).df()
    print(q6.to_string(index=False))

    # 7) Atlantis sample: requests grouped by wind direction band (uses the wind
    #    augmentation from Task 5 to explore any request/weather relationship)
    logger.info("Atlantis sample: request count by wind direction band:")
    q7 = con.execute(f"""
        SELECT
            CASE
                WHEN wind_direction < 45  OR wind_direction >= 315 THEN 'N'
                WHEN wind_direction < 135 THEN 'E'
                WHEN wind_direction < 225 THEN 'S'
                ELSE 'W'
            END AS wind_band,
            COUNT(*) AS request_count,
            ROUND(AVG(wind_speed), 1) AS avg_wind_speed
        FROM {FQ}.atlantis_wind_sample
        WHERE wind_direction IS NOT NULL
        GROUP BY wind_band
        ORDER BY request_count DESC
    """).df()
    print(q7.to_string(index=False))


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 5: orchestrate - attach the named database, build, then analyse
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
    os.makedirs(os.path.join(ROOT, "data"), exist_ok=True)   # ensure data/ exists
    s3 = get_s3_client()                                     # S3 client
    con = duckdb.connect()                                   # root (in-memory) connection
    try:
        # ATTACH the warehouse file under the logical name CCT_Database so all
        # tables live at CCT_Database.CCT_Schema.* regardless of the file path.
        con.execute(f"ATTACH '{DB_PATH}' AS {DB_NAME}")
        build_warehouse(con, s3)                             # create schema + load tables
        run_analysis(con)                                    # run the analytical queries
        logger.info(f"Warehouse ready: {FQ} (file: {DB_PATH})")
    finally:
        con.close()                                          # always close the connection


if __name__ == "__main__":                                   # run only when executed directly
    main()
