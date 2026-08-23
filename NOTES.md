# Project Notes — How This Assessment Was Built

A short, plain-language walkthrough of everything done to complete the Data
Engineering tasks, in the order it happened. Meant as an easy-to-follow guide
for anyone picking up this repo.

============================================================================
## 0. Environment Setup
============================================================================

- Forked the City's repo and cloned it locally.
- Created the folder structure: `src/` (code), `tests/` (tests), `conf/`
  (config), `data/` (generated outputs, git-ignored).
- Added `requirements.txt` (boto3, pandas, geopandas, shapely, h3, pyproj,
  requests, pytest) and installed everything.
- Configured a dedicated AWS profile named `cct` (read-only creds provided with
  the assessment) so no credentials live in the code.
- Confirmed access to the `cct-ds-code-challenge-input-data` bucket in
  `af-south-1`.
- Added `.gitignore` (keeps creds, data, caches out of the repo).

============================================================================
## 1. Task 1 — Data Extraction (AWS S3 SELECT)
============================================================================

**Goal:** read only the H3 resolution-8 hexagons from the 108 MB
`city-hex-polygons-8-10.geojson` without downloading the whole file.

**What we did:**
- Used **S3 SELECT** to filter the file server-side with SQL
  (`WHERE resolution = 8`), returning only ~3,832 features.
- **Validated** the result by S3-SELECTing the smaller `city-hex-polygons-8.geojson`
  (which is res-8 only) and comparing the two sets of H3 indices — they matched
  exactly (0 missing, 0 extra).
- Built a **non-binary schema conformance score**: each feature is checked
  against an expected schema (types, value ranges, H3 index prefix, resolution),
  and we compute the fraction of checks that pass. The schema and its 0.95
  threshold live in `conf/schema.json`.
- **Logged timings** for extraction and validation.

**Result:** 3,832 hexagons, exact validation match, conformance score 1.00.
**File:** `src/task1_extraction.py`

============================================================================
## 2. Task 2 — Initial Data Transformation (H3 Join)
============================================================================

**Goal:** assign each service request in `sr.csv.gz` to one H3 resolution-8
hexagon, and validate against `sr_hex.csv.gz`.

**What we did:**
- Downloaded and read `sr.csv.gz` (~941,634 requests).
- Computed each request's hexagon with `h3.latlng_to_cell(lat, lon, 8)`.
  Requests with no coordinates get index `0`.
- **Validated** by joining our computed index to the provided `sr_hex.csv.gz`
  on `notification_number` — a **100% match** (941,634 / 941,634).
- Handled the **join-error threshold** thoughtfully: separated
  *expected missing* (no coordinates — legitimate, ~22.5%) from *true failures*
  (coordinates present but unmappable). The threshold (1%) is applied to true
  failures only, which are 0% in practice.
- **Logged timings** and both failure metrics.

**Result:** 100% validation match, 0 true join failures.
**File:** `src/task2_transformation.py`

============================================================================
## 3. Task 5 — Further Data Transformations (Atlantis)
============================================================================

**Goal:** subsample near Atlantis, augment with wind data, then anonymise.

### 3.1 Subsample (centroid + radius)
- Downloaded an Atlantis suburb boundary from **OpenStreetMap (Nominatim)**.
- Computed its **centroid programmatically** with `shapely` (not hardcoded).
- Kept all requests within **1 arc-minute** (~1.85 km) of that centroid → 7,213.

### 3.2 Wind Augmentation (resilient fetch)
- Fetched **2020 hourly wind speed/direction** for Atlantis and joined it to the
  subsample by the hour each request was created (100% joined).
- Wrapped the download in a **resilient fetch**: retries with exponential
  backoff, per-attempt timeouts, and a local cache — so re-runs don't depend on
  a flaky endpoint. Used Open-Meteo as a documented fallback because the City's
  air-quality endpoint was unreachable.

### 3.3 Anonymisation
- **Location →** H3 resolution-9 index (~500 m precision), exact lat/lon removed.
- **Time →** floored to 6-hour buckets, exact timestamps removed.
- **Identifiers →** dropped `notification_number` and `reference_number`.
- **Singletons →** records alone in their (6h bucket, res-9 cell) group are
  quarantined into a separate file for manual review; the rest are published.

**Result:** 3,036 publishable + 4,177 quarantined for review.
**File:** `src/task5_transformations.py`; justification in `REPORT.md`.

============================================================================
## 4. Testing
============================================================================

- Wrote **pytest unit tests** for the pure logic of each task (schema scoring,
  validation, H3 assignment, join metrics, subsample, anonymisation).
- Tests are fast and offline (no AWS needed).
- **14 tests, all passing.**
- Run with: `python3 -m pytest tests/ -v`
- **Files:** `tests/test_task1.py`, `tests/test_task2.py`, `tests/test_task5.py`

============================================================================
## 5. Documentation & Reproducibility
============================================================================

- **README.md** — focused on the Data Engineering work, with setup and run
  steps for each task, plus the key design decisions.
- **REPORT.md** — the anonymisation justification (< 500 words).
- **AI_log.md** — how AI assistance was used, the model, a note on token usage,
  and examples where I instructed/corrected the AI.
- **Robust paths** — every script resolves paths relative to its own location,
  so it runs from any working directory.
- **Fresh-clone test** — cloned the repo into a clean folder, installed
  requirements, and ran the tests — all 14 passed, proving the repo clones and
  runs with no human interaction.

============================================================================
## 6. Key Lessons / Decisions (for quick recall)
============================================================================

- **S3 SELECT** avoids downloading a 108 MB file — filter at the source.
- **Thresholds should be evidence-based.** The Task 2 threshold was redesigned
  after real data showed 22.5% legitimately-missing coordinates.
- **Conformance is a score, not pass/fail** — tolerant of minor noise, tunable
  via config.
- **Flaky dependencies** are handled with retries + backoff + timeout + cache +
  a documented fallback.
- **Anonymisation is a privacy/utility trade-off** — 58% of records were
  singletons at ~500 m / 6 h precision, so they were held for manual review
  rather than published.
