# ============================================================================
# TASK 1: Data Extraction via AWS S3 SELECT
# ============================================================================
# Reads H3 resolution-8 hexagons from the large city-hex-polygons-8-10.geojson
# using S3 SELECT (server-side filtering), validates against the res-8 file,
# and computes a schema conformance score. Logs timings throughout.
# ============================================================================

import json                                              # parse S3 SELECT JSON output + config
import time                                              # measure operation timings
import logging                                           # structured logging
import boto3                                             # AWS SDK / S3 SELECT
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- logging setup: timestamped, INFO level ---
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- constants for the S3 source ---
BUCKET = "cct-ds-code-challenge-input-data"              # the assessment data bucket
BIG_KEY = "city-hex-polygons-8-10.geojson"               # multi-resolution source file
SMALL_KEY = "city-hex-polygons-8.geojson"                # res-8-only validation file
REGION = "af-south-1"                                    # bucket region
PROFILE = "cct"                                          # local AWS profile with the creds


# Load the expected-schema config that drives the conformance score.
def load_schema(path=None):
    if path is None:
        path = os.path.join(ROOT, "conf", "schema.json")
    with open(path) as f:
        return json.load(f)


# Return a boto3 S3 client using our named profile + region.
def get_s3_client():
    session = boto3.Session(profile_name=PROFILE)        # use the 'cct' profile (no keys in code)
    return session.client("s3", region_name=REGION)      # S3 client in af-south-1


# Run S3 SELECT on a GeoJSON object, filtering features by resolution.
def s3_select_features(s3, key, resolution):
    # SQL runs SERVER-SIDE in S3, so only matching features are returned.
    query = (
        "SELECT s.properties, s.geometry "
        "FROM S3Object[*].features[*] s "
        f"WHERE s.properties.resolution = {resolution}"
    )
    resp = s3.select_object_content(                     # start the S3 SELECT
        Bucket=BUCKET,
        Key=key,
        ExpressionType="SQL",
        Expression=query,
        InputSerialization={"JSON": {"Type": "DOCUMENT"}},   # input is a JSON document
        OutputSerialization={"JSON": {}},                    # stream results as JSON
    )
    # S3 SELECT streams results in "events"; collect the record payloads.
    records = ""
    for event in resp["Payload"]:                        # iterate the event stream
        if "Records" in event:                           # a chunk of result data
            records += event["Records"]["Payload"].decode("utf-8")
    # Each line is one JSON object; parse them into a list of dicts.
    features = [json.loads(line) for line in records.splitlines() if line.strip()]
    return features

#++++++++++++++++++++++++++++++++++
#Validation against the small file
#++++++++++++++++++++++++++++++++++

# S3 SELECT ALL features from the small (res-8-only) file for validation.
def s3_select_all_features(s3, key):
    query = "SELECT s.properties, s.geometry FROM S3Object[*].features[*] s"  # no filter: file is already res-8
    resp = s3.select_object_content(
        Bucket=BUCKET,
        Key=key,
        ExpressionType="SQL",
        Expression=query,
        InputSerialization={"JSON": {"Type": "DOCUMENT"}},
        OutputSerialization={"JSON": {}},
    )
    records = ""
    for event in resp["Payload"]:
        if "Records" in event:
            records += event["Records"]["Payload"].decode("utf-8")
    return [json.loads(line) for line in records.splitlines() if line.strip()]


# Validate extracted features against the ground-truth res-8 file by comparing index sets.
def validate_against_small(extracted, ground_truth):
    # Pull the set of H3 index strings from each dataset.
    extracted_idx = {f["properties"]["index"] for f in extracted}      # from big file (res-8 filtered)
    truth_idx = {f["properties"]["index"] for f in ground_truth}       # from small file

    missing = truth_idx - extracted_idx        # in truth but NOT extracted (we missed some)
    extra = extracted_idx - truth_idx          # extracted but NOT in truth (we got wrong ones)

    matches = extracted_idx == truth_idx        # exact set equality?
    logger.info(f"Validation: extracted={len(extracted_idx)}, truth={len(truth_idx)}, "
                f"missing={len(missing)}, extra={len(extra)}, exact_match={matches}")
    return matches, missing, extra

# Check ONE feature against the schema; return (checks_passed, checks_total).
def score_feature(feature, schema):
    props = feature.get("properties", {})               # the feature's properties dict
    req = schema["required_properties"]                  # expected props + their types
    con = schema["constraints"]                          # value constraints
    passed = 0                                           # count of checks this feature passed
    total = 0                                            # count of checks run

    # 1) required properties exist + correct type
    for name, expected_type in req.items():
        total += 1                                       # one check per required property
        value = props.get(name)                          # the actual value (or None if missing)
        if value is None:
            continue                                     # missing -> check fails (passed not incremented)
        # map JSON type names to Python types
        ok_type = (isinstance(value, str) if expected_type == "string"
                   else isinstance(value, (int, float)))
        if ok_type:
            passed += 1                                  # type matches -> pass

    # 2) resolution must equal the expected value
    total += 1
    if props.get("resolution") == con["resolution_expected_value"]:
        passed += 1

    # 3) centroid_lat within range
    total += 1
    lat = props.get("centroid_lat")
    lo, hi = con["centroid_lat_range"]
    if lat is not None and lo <= lat <= hi:
        passed += 1

    # 4) centroid_lon within range
    total += 1
    lon = props.get("centroid_lon")
    lo, hi = con["centroid_lon_range"]
    if lon is not None and lo <= lon <= hi:
        passed += 1

    # 5) index has the expected prefix
    total += 1
    idx = props.get("index", "")
    if isinstance(idx, str) and idx.startswith(con["index_prefix"]):
        passed += 1

    return passed, total


# Compute the overall conformance score across all features and compare to threshold.
def conformance_score(features, schema):
    total_passed = 0                                     # sum of passed checks across all features
    total_checks = 0                                     # sum of all checks run
    for f in features:                                   # score every feature
        p, t = score_feature(f, schema)
        total_passed += p
        total_checks += t
    score = total_passed / total_checks if total_checks else 0.0   # fraction passed (0..1)
    threshold = schema["conformance_threshold"]          # the pass bar from config
    return score, threshold


def main():
    schema = load_schema()                               # load expected schema config (conf/schema.json)
    s3 = get_s3_client()                                 # build S3 client using the 'cct' profile

    # --- Extract res-8 from the big file (S3 SELECT) ---
    t0 = time.time()                                     # start timer for the extraction
    features = s3_select_features(s3, BIG_KEY, 8)        # S3 SELECT: pull only resolution-8 features
    logger.info(f"S3 SELECT returned {len(features)} res-8 features in {time.time()-t0:.2f}s")  # log count + elapsed time

    # --- Validate against the small ground-truth file ---
    t1 = time.time()                                     # start timer for validation
    ground_truth = s3_select_all_features(s3, SMALL_KEY) # read the res-8-only file (the ground truth)
    matches, missing, extra = validate_against_small(features, ground_truth)  # compare index sets
    logger.info(f"Validation completed in {time.time()-t1:.2f}s")  # log validation elapsed time

    # --- Schema conformance score ---
    t2 = time.time()                                     # timer for conformance scoring
    score, threshold = conformance_score(features, schema)   # compute the non-binary score
    logger.info(f"Conformance score: {score:.4f} (threshold {threshold}) in {time.time()-t2:.2f}s")

    if score < threshold:                                # below the configured bar
        logger.error(f"Conformance {score:.4f} below threshold {threshold} - data quality FAILED")
    else:
        logger.info("Conformance check PASSED")

    if not matches:                                      # if the extracted set != ground-truth set
        logger.warning(f"Index sets differ! missing={len(missing)}, extra={len(extra)}")  # warn with the diff counts


if __name__ == "__main__":                               # run only when executed directly
    main()                                               # kick off the pipeline
