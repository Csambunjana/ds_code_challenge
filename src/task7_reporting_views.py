# ============================================================================
# REPORTING VIEWS: curated views for BI / reporting tools
# ============================================================================
# Reporting tools (Power BI, Tableau, Metabase, etc.) connect to these VIEWS
# rather than the raw tables. Views give:
#   - a STABLE interface (dashboards don't break when underlying tables change),
#   - a SINGLE source of truth for business logic (KPI definitions, joins),
#   - a SIMPLER surface for analysts (query a view, not complex SQL).
#
# The views are created inside the existing warehouse built by
# task6_duckdb_warehouse.py (database CCT_Database, schema CCT_Schema).
# Run task6 FIRST so the tables exist, then run this script.
#
# Views carry NO LIMIT - the reporting tool decides how to filter/sort/page.
# ============================================================================


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 1: imports, logging, config, robust paths
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os                                                # path resolution (run from anywhere)
import logging                                           # structured logging
import duckdb                                            # embedded analytical warehouse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root

DB_PATH = os.path.join(ROOT, "data", "CCT_Database.duckdb")  # the warehouse file built by task6
DB_NAME = "CCT_Database"                                     # logical database name (ATTACH alias)
SCHEMA = "CCT_Schema"                                        # schema holding the tables + views
FQ = f"{DB_NAME}.{SCHEMA}"                                   # fully-qualified prefix


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 2: create the reporting views
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def create_reporting_views(con):                         # con = a DuckDB connection (DB attached)
    # --- request count per hexagon, with centroid location (for map visuals) ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_requests_by_hexagon AS
        SELECT f.h3_level8_index,
               COUNT(*) AS request_count,
               d.centroid_lat,
               d.centroid_lon
        FROM {FQ}.fact_service_requests f
        JOIN {FQ}.dim_hexagons d ON f.h3_level8_index = d."index"
        WHERE f.h3_level8_index <> '0'
        GROUP BY f.h3_level8_index, d.centroid_lat, d.centroid_lon
    """)                                                 # excludes the "0" no-geolocation bucket

    # --- request volume per directorate ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_requests_by_directorate AS
        SELECT directorate, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        GROUP BY directorate
    """)

    # --- time-to-complete KPI (median & 80th percentile hours) per directorate ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_completion_time_by_directorate AS
        SELECT
            directorate,
            COUNT(*) AS completed_requests,
            ROUND(MEDIAN(DATE_DIFF('hour',
                CAST(creation_timestamp AS TIMESTAMP),
                CAST(completion_timestamp AS TIMESTAMP))), 1) AS median_hours,
            ROUND(QUANTILE_CONT(DATE_DIFF('hour',
                CAST(creation_timestamp AS TIMESTAMP),
                CAST(completion_timestamp AS TIMESTAMP)), 0.8), 1) AS p80_hours
        FROM {FQ}.fact_service_requests
        WHERE completion_timestamp IS NOT NULL
          AND creation_timestamp IS NOT NULL
          AND directorate IS NOT NULL
        GROUP BY directorate
    """)                                                 # one source of truth for the completion KPI

    # --- request volume per suburb (demand hotspots) ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_requests_by_suburb AS
        SELECT official_suburb, COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE official_suburb IS NOT NULL
        GROUP BY official_suburb
    """)

    # --- monthly request trend ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_requests_by_month AS
        SELECT DATE_TRUNC('month', CAST(creation_timestamp AS TIMESTAMP)) AS month,
               COUNT(*) AS request_count
        FROM {FQ}.fact_service_requests
        WHERE creation_timestamp IS NOT NULL
        GROUP BY month
    """)

    # --- Atlantis requests by wind direction band (uses Task 5 wind augmentation) ---
    con.execute(f"""
        CREATE OR REPLACE VIEW {FQ}.vw_atlantis_wind_bands AS
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
    """)

    logger.info(f"Created 6 reporting views in {FQ} (prefixed vw_)")


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 3: list the views + preview one (as a reporting tool would query it)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def list_and_preview_views(con):
    # list every view in the schema
    views = con.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{SCHEMA}' AND table_type = 'VIEW'
        ORDER BY table_name
    """).df()
    logger.info("Reporting views available:")
    print(views.to_string(index=False))

    # preview one view exactly like a dashboard tile would query it
    logger.info("Preview of vw_completion_time_by_directorate (top 5 by volume):")
    preview = con.execute(f"""
        SELECT * FROM {FQ}.vw_completion_time_by_directorate
        ORDER BY completed_requests DESC
        LIMIT 5
    """).df()
    print(preview.to_string(index=False))


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 4: orchestrate - attach the warehouse, create views, preview
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
    # Fail early with a clear message if the warehouse hasn't been built yet.
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"{DB_PATH} not found - run src/task6_duckdb_warehouse.py first.")

    con = duckdb.connect()                               # root (in-memory) connection
    try:
        con.execute(f"ATTACH '{DB_PATH}' AS {DB_NAME}")  # attach the existing warehouse by name
        create_reporting_views(con)                      # (re)create the reporting views
        list_and_preview_views(con)                      # confirm + preview
        logger.info(f"Reporting views ready in {FQ}")
    finally:
        con.close()                                      # always close the connection


if __name__ == "__main__":                               # run only when executed directly
    main()
