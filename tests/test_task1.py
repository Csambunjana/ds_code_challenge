# ============================================================================
# Unit tests for Task 1 (schema conformance + validation logic)
# Run with:  python3 -m pytest tests/ -v
# ============================================================================

import os
import sys

# make the src/ package importable when running pytest from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.task1_extraction import (                       # functions under test
    score_feature,
    conformance_score,
    validate_against_small,
)

# A minimal schema matching conf/schema.json's shape, for isolated testing.
SCHEMA = {
    "required_properties": {
        "index": "string",
        "centroid_lat": "number",
        "centroid_lon": "number",
        "resolution": "number",
    },
    "constraints": {
        "resolution_expected_value": 8,
        "centroid_lat_range": [-35.0, -33.0],
        "centroid_lon_range": [18.0, 19.5],
        "index_prefix": "88",
    },
    "conformance_threshold": 0.95,
}

# A fully-valid feature: every check should pass.
GOOD = {"properties": {"index": "88ad361801fffff", "centroid_lat": -33.9,
                       "centroid_lon": 18.6, "resolution": 8}}


# A perfectly valid feature should pass all of its checks.
def test_score_feature_all_pass():
    passed, total = score_feature(GOOD, SCHEMA)
    assert passed == total                               # all checks passed
    assert total > 0                                     # checks actually ran


# A wrong resolution should fail at least one check.
def test_score_feature_bad_resolution():
    bad = {"properties": dict(GOOD["properties"], resolution=9)}
    passed, total = score_feature(bad, SCHEMA)
    assert passed < total


# Latitude outside Cape Town's range should fail the lat check.
def test_score_feature_bad_lat():
    bad = {"properties": dict(GOOD["properties"], centroid_lat=10.0)}
    passed, total = score_feature(bad, SCHEMA)
    assert passed < total


# A wrong index prefix should fail the prefix check.
def test_score_feature_bad_prefix():
    bad = {"properties": dict(GOOD["properties"], index="99xxxx")}
    passed, total = score_feature(bad, SCHEMA)
    assert passed < total


# An all-good dataset should score exactly 1.0 (>= threshold).
def test_conformance_all_good():
    score, threshold = conformance_score([GOOD, GOOD, GOOD], SCHEMA)
    assert score == 1.0
    assert score >= threshold


# Identical index sets should report an exact match.
def test_validate_exact_match():
    a = [{"properties": {"index": "88a"}}, {"properties": {"index": "88b"}}]
    matches, missing, extra = validate_against_small(a, a)
    assert matches is True
    assert not missing and not extra


# A missing hexagon should be detected by validation.
def test_validate_missing():
    extracted = [{"properties": {"index": "88a"}}]
    truth = [{"properties": {"index": "88a"}}, {"properties": {"index": "88b"}}]
    matches, missing, extra = validate_against_small(extracted, truth)
    assert matches is False
    assert "88b" in missing
