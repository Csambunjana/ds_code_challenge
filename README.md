# City of Cape Town — Data Science Unit Code Challenge (Data Engineering)

This repository contains my submission for the **Data Engineering** track of the
City of Cape Town Data Science Unit code challenge. It implements the three
Data Engineering tasks (1, 2 and 5), each with logging, timing, data-quality
validation and unit tests.

---

## Overview of what I built

| Task | Description | Key result |
|------|-------------|------------|
| **1. Data Extraction** | Read H3 resolution-8 hexagons from the 108 MB `city-hex-polygons-8-10.geojson` using **AWS S3 SELECT** (server-side filtering, no full download). Validate against `city-hex-polygons-8.geojson` and compute a schema conformance score. | 3,832 hexagons extracted; exact match vs ground truth; conformance 1.00 |
| **2. Initial Data Transformation** | Assign each service request in `sr.csv.gz` to a single H3 resolution-8 hexagon (computed from its lat/lon). Empty coordinates → index `0`. Validate against `sr_hex.csv.gz`; log join failures with a justified threshold. | 941,634 requests; **100% match** vs `sr_hex`; 0 true join failures |
| **5. Further Data Transformations** | Subsample requests within 1 arc-minute of an Atlantis suburb centroid (computed programmatically), augment with 2020 wind data (resilient fetch), and anonymise (location ~500 m, time ~6 h, PII removed, singletons quarantined). | 7,213 requests subsampled; wind joined 100%; anonymised + review split |
| **Analysis layer (bonus)** | Load the transformed data into a local **DuckDB** warehouse (`CCT_Database` / `CCT_Schema`) and expose curated `vw_*` views for reporting tools. Completes the full ETL/ELT flow. | 3 tables + 6 reporting views |

---

## Project structure

```
ds_code_challenge/
├── conf/
│   └── schema.json                 # expected schema + thresholds for Task 1 validation
├── src/
│   ├── task1_extraction.py         # Task 1: S3 SELECT extraction + validation + conformance
│   ├── task2_transformation.py     # Task 2: H3 assignment + join validation
│   ├── task5_transformations.py    # Task 5: subsample + wind augmentation + anonymisation
│   ├── task6_duckdb_warehouse.py   # Analysis layer: load data into DuckDB warehouse
│   └── task7_reporting_views.py    # Analysis layer: create vw_* reporting views
├── tests/
│   ├── test_task1.py
│   ├── test_task2.py
│   └── test_task5.py
├── data/                           # generated outputs (git-ignored)
├── requirements.txt
├── REPORT.md                       # anonymisation justification (Task 5.3)
├── AI_log.md                       # log of AI assistance used
└── README.md
```

---

## Setup

### 1. Install dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Configure AWS credentials

The challenge data lives in the `cct-ds-code-challenge-input-data` bucket in the
`af-south-1` region. Using the read-only credentials provided with the
assessment, create a named AWS profile called `cct`:

```bash
aws configure set aws_access_key_id     <ACCESS_KEY>  --profile cct
aws configure set aws_secret_access_key <SECRET_KEY>  --profile cct
aws configure set region                af-south-1    --profile cct
```

> All scripts reference the `cct` profile explicitly, so no credentials are
> stored in code. Confirm access with:
> `aws s3 ls s3://cct-ds-code-challenge-input-data/ --profile cct`

---

## How to run

All scripts are run from the **repository root** and are self-contained — each
downloads its own inputs and writes outputs to `data/`.

### Task 1 — Data Extraction (S3 SELECT)

```bash
python3 src/task1_extraction.py
```

Extracts the resolution-8 hexagons from `city-hex-polygons-8-10.geojson` via S3
SELECT, validates the extracted index set against `city-hex-polygons-8.geojson`,
and computes a **non-binary schema conformance score** against the schema in
`conf/schema.json` (pass threshold 0.95). Timings are logged for each step.

### Task 2 — Initial Data Transformation (H3 join)

```bash
python3 src/task2_transformation.py
```

Reads `sr.csv.gz`, computes each request's H3 resolution-8 index from its
latitude/longitude (empty coordinates → `0`), and validates the result against
`sr_hex.csv.gz`. It distinguishes **expected missing** geolocation (requests
that never had coordinates) from **true join failures** (coordinates present but
unmappable) and enforces the error threshold on the latter — see the threshold
motivation below.

### Task 5 — Further Data Transformations (Atlantis)

```bash
python3 src/task5_transformations.py
```

Runs the full 5.1 → 5.2 → 5.3 pipeline:
1. **5.1** downloads an Atlantis suburb boundary (OpenStreetMap), computes its
   centroid with `shapely`, and subsamples all requests within 1 arc-minute.
2. **5.2** augments the subsample with 2020 hourly wind speed/direction using a
   resilient fetch (retry + backoff + timeout + local cache).
3. **5.3** anonymises the result (H3 res-9 ~500 m, 6-hour time buckets, PII
   removed) and quarantines potentially-identifying singletons for manual review.

Outputs are written to `data/` (`atlantis_subsample.csv`,
`atlantis_subsample_wind.csv`, `atlantis_anonymised.csv`,
`atlantis_manual_review.csv`).

### Analysis Layer — DuckDB Warehouse & Reporting Views (bonus)

To finish the entire ETL/ELT flow (Extract → Transform → Load → Analyse →
Serve), the transformed data is loaded into a local **DuckDB** warehouse and
exposed through curated reporting views. I considered cloud warehouses such as
Azure Synapse and Amazon Redshift, but chose **DuckDB** so the analysis layer
is fully self-contained and reproducible from a public GitHub clone — no
account, credentials, networking or cost required. DuckDB is an open-source,
in-process analytical database (a single local file).

- DuckDB downloads / installation: https://duckdb.org/docs/installation/
- Python API: `pip install duckdb`

**Warehouse structure:** database `CCT_Database`, schema `CCT_Schema`, tables
`fact_service_requests` / `dim_hexagons` / `atlantis_wind_sample`, and six
`vw_*` reporting views that BI tools (Power BI, Tableau, Metabase) connect to.

```bash
python3 src/task6_duckdb_warehouse.py    # build the warehouse tables
python3 src/task7_reporting_views.py     # create the reporting views
```

Browse the views the way a reporting tool would (read-only, no write lock):

```bash
~/.duckdb/cli/latest/duckdb -readonly data/CCT_Database.duckdb
```
```sql
.mode box
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'CCT_Schema' AND table_type = 'VIEW';
SELECT * FROM CCT_Database.CCT_Schema.vw_completion_time_by_directorate
ORDER BY completed_requests DESC LIMIT 5;
```

### Run the tests

```bash
python3 -m pytest tests/ -v
```

---

## Key design decisions

- **Task 2 join-error threshold.** ~22.5% of requests legitimately have no
  coordinates (call-centre logged or area-wide issues) and are correctly
  assigned index `0`. Treating these as failures would falsely fail a normal
  run, so I separate **expected missing** from **true join failures**
  (coordinates present but unmappable) and set the error threshold (1%) on true
  failures — which are ~0% in practice. A meaningful rate of true failures would
  indicate a logic or data-corruption problem worth aborting for.

- **Schema conformance is non-binary.** Task 1 scores the *proportion* of
  validation checks that pass rather than failing on the first bad record. The
  schema and 0.95 threshold live in `conf/schema.json` so they are explicit and
  tunable.

- **External data sources.** The City's own GIS and air-quality endpoints were
  unreachable at build time, so I use documented, programmatically-downloaded
  alternatives — OpenStreetMap (Nominatim) for the suburb boundary and
  Open-Meteo for 2020 Atlantis wind data. Both are fetched in-script.

- **Resilient dependency handling (Task 5.2).** The wind download uses retries
  with exponential backoff, per-attempt timeouts, and an on-disk cache, so
  re-runs do not depend on a flaky endpoint being available.

- **Reproducibility.** Paths are resolved relative to the script location, so
  every script runs correctly regardless of the current working directory.

See `REPORT.md` for the Task 5.3 anonymisation justification and `AI_log.md`
for a log of AI assistance used during development.
