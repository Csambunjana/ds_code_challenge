# ============================================================================
# TASK 2: Initial Data Transformation
# ============================================================================
# Assign each service request (from sr.csv.gz) to a single H3 resolution-8
# hexagon, using the H3 index computed from its latitude/longitude.
# Empty coordinates -> index "0". Validate against sr_hex.csv.gz.
#
# Join-error handling: we distinguish EXPECTED missing geolocation (requests
# that never had coordinates -> correctly index "0") from TRUE join failures
# (requests that HAVE coordinates but fail to map to a hexagon). The error
# threshold is enforced on TRUE failures, which should be ~0%.
# ============================================================================


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 1: imports, logging, config, robust paths
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os                                                # path resolution (run from anywhere)
import io                                                # in-memory bytes buffer for the download
import gzip                                              # sr.csv is gzip-compressed
import time                                              # measure operation timings
import logging                                           # structured logging
import boto3                                             # AWS SDK / S3 access
import pandas as pd                                      # dataframes
import h3                                                # Uber H3 spatial indexing (v4 API)

logging.basicConfig(                                     # configure logging once
    level=logging.INFO,                                  # show INFO and above
    format="%(asctime)s [%(levelname)s] %(message)s",    # timestamp + level + message
)
logger = logging.getLogger(__name__)                     # logger for this module

# Repo root = one level up from this file's folder (src/), for stable paths.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BUCKET = "cct-ds-code-challenge-input-data"              # the assessment data bucket
SR_KEY = "sr.csv.gz"                                     # service requests (NO h3 column)
SR_HEX_KEY = "sr_hex.csv.gz"                             # ground truth (HAS h3_level8_index)
REGION = "af-south-1"                                    # bucket region
PROFILE = "cct"                                          # local AWS profile with the creds

# --- Justified join-error threshold ---
# Applied to TRUE join failures only: records that HAVE coordinates but fail to
# map to an H3 cell. A well-formed coordinate should essentially always produce
# a valid H3 index, so this rate should be ~0%. We allow a tiny 1% margin for
# malformed/out-of-range coordinates; anything above that signals a real bug or
# data-corruption issue and we abort. (Expected-missing coords -- ~22.5% of the
# data that legitimately never had a location -- are NOT counted as failures.)
JOIN_ERROR_THRESHOLD = 0.01


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 2: S3 client + read a gzipped CSV from S3 into a DataFrame
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def get_s3_client():                                     # build an S3 client using the 'cct' profile
    return boto3.Session(profile_name=PROFILE).client("s3", region_name=REGION)


def read_csv_gz_from_s3(s3, key):                        # download + read a .csv.gz object
    obj = s3.get_object(Bucket=BUCKET, Key=key)          # fetch the S3 object
    raw = obj["Body"].read()                             # read all its bytes into memory
    with gzip.open(io.BytesIO(raw), "rt") as f:          # decompress in memory (text mode)
        return pd.read_csv(f)                            # parse the CSV into a DataFrame


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 3: the core transform - assign an H3 level-8 index to each row
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def assign_h3_index(df):                                 # add an h3_level8_index column
    def _one(lat, lon):                                  # compute the index for one row
        if pd.isna(lat) or pd.isna(lon):                 # missing coordinate?
            return "0"                                    # -> index "0" (per the brief)
        try:
            return h3.latlng_to_cell(lat, lon, 8)        # H3 v4: (lat, lon) -> resolution-8 cell
        except Exception:
            return "0"                                    # any failure -> treat as unjoinable
    df = df.copy()                                       # work on a copy (don't mutate caller's df)
    df["h3_level8_index"] = [                            # build the new column row-by-row
        _one(lat, lon) for lat, lon in zip(df["latitude"], df["longitude"])
    ]
    return df                                            # return the augmented DataFrame


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 4: join metrics - separate EXPECTED missing from TRUE join failures
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def join_metrics(df):                                    # measure the two kinds of "no index"
    has_coords = df["latitude"].notna() & df["longitude"].notna()  # rows that HAVE coordinates
    # expected-missing: no coordinates at all -> index "0" is correct, NOT a failure
    expected_missing = int((~has_coords).sum())
    # true failure: HAD coordinates but still ended up as "0" (a real problem)
    true_failures = int(((df["h3_level8_index"] == "0") & has_coords).sum())
    total = len(df)
    return {
        "expected_missing": expected_missing,
        "expected_missing_rate": expected_missing / total if total else 0.0,
        "true_failures": true_failures,
        "true_failure_rate": true_failures / total if total else 0.0,
    }


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 5: validation against the ground-truth sr_hex.csv.gz
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def validate_against_truth(computed_df, truth_df):       # compare our h3 index to the provided one
    key = "notification_number"                          # unique per service request
    merged = computed_df[[key, "h3_level8_index"]].merge(   # join our result...
        truth_df[[key, "h3_level8_index"]],                 # ...to the ground truth
        on=key, suffixes=("_computed", "_truth"),
    )
    # A row matches when our computed index equals the provided index.
    matches = int((merged["h3_level8_index_computed"] == merged["h3_level8_index_truth"]).sum())
    total = len(merged)                                  # rows we could compare
    match_rate = matches / total if total else 0.0       # fraction that agree
    return matches, total, match_rate


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Piece 6: orchestrate - read, transform, measure, enforce threshold, validate
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():                                              # entry point
    s3 = get_s3_client()                                 # S3 client

    # --- read the raw service requests ---
    t0 = time.time()
    sr = read_csv_gz_from_s3(s3, SR_KEY)                 # ~942K rows, no h3 column
    logger.info(f"Read {len(sr)} service requests in {time.time()-t0:.2f}s")

    # --- assign the H3 index (the transform) ---
    t1 = time.time()
    sr = assign_h3_index(sr)                             # add h3_level8_index
    logger.info(f"Assigned H3 indices in {time.time()-t1:.2f}s")

    # --- log join metrics + enforce threshold on TRUE failures ---
    m = join_metrics(sr)
    logger.info(f"Expected missing (no coords): {m['expected_missing']} "
                f"({m['expected_missing_rate']:.2%})")
    logger.info(f"True join failures (coords but no index): {m['true_failures']} "
                f"({m['true_failure_rate']:.2%})")
    if m["true_failure_rate"] > JOIN_ERROR_THRESHOLD:    # real failures too high -> abort
        raise SystemExit(f"True join failure rate {m['true_failure_rate']:.2%} "
                         f"exceeds threshold {JOIN_ERROR_THRESHOLD:.0%} - aborting.")

    # --- validate against the provided ground truth ---
    t2 = time.time()
    truth = read_csv_gz_from_s3(s3, SR_HEX_KEY)          # the file that already has h3 indices
    matches, total, match_rate = validate_against_truth(sr, truth)
    logger.info(f"Validation: {matches}/{total} indices match truth "
                f"({match_rate:.2%}) in {time.time()-t2:.2f}s")


if __name__ == "__main__":                               # run only when executed directly
    main()                                               # kick off the pipeline
