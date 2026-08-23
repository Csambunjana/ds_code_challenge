# ============================================================================
# Unit tests for Task 2 (H3 assignment + join metrics + validation logic)
# Run with:  python3 -m pytest tests/ -v
# ============================================================================

import os
import sys
import pandas as pd

# make the src/ package importable when running pytest from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.task2_transformation import (                   # functions under test
    assign_h3_index,
    join_metrics,
    validate_against_truth,
)


# Rows WITH valid coordinates should get a real (non-"0") H3 index.
def test_assign_h3_valid_coords():
    df = pd.DataFrame({"latitude": [-33.87283933403916],
                       "longitude": [18.52248797221645]})
    out = assign_h3_index(df)
    assert out["h3_level8_index"].iloc[0] == "88ad360225fffff"   # known correct index


# Rows with MISSING coordinates should get index "0".
def test_assign_h3_missing_coords():
    df = pd.DataFrame({"latitude": [None], "longitude": [None]})
    out = assign_h3_index(df)
    assert out["h3_level8_index"].iloc[0] == "0"


# join_metrics: no-coord rows count as expected-missing, not true failures.
def test_join_metrics_expected_missing():
    df = pd.DataFrame({
        "latitude": [-33.87, None],                      # one has coords, one doesn't
        "longitude": [18.52, None],
    })
    df = assign_h3_index(df)
    m = join_metrics(df)
    assert m["expected_missing"] == 1                    # the None-coord row
    assert m["true_failures"] == 0                       # the valid row mapped fine


# validate_against_truth: identical indices should report a 100% match.
def test_validate_perfect_match():
    computed = pd.DataFrame({"notification_number": [1, 2],
                             "h3_level8_index": ["88a", "88b"]})
    truth = pd.DataFrame({"notification_number": [1, 2],
                          "h3_level8_index": ["88a", "88b"]})
    matches, total, rate = validate_against_truth(computed, truth)
    assert matches == 2 and total == 2 and rate == 1.0


# validate_against_truth: a mismatch should lower the match rate.
def test_validate_with_mismatch():
    computed = pd.DataFrame({"notification_number": [1, 2],
                             "h3_level8_index": ["88a", "WRONG"]})
    truth = pd.DataFrame({"notification_number": [1, 2],
                          "h3_level8_index": ["88a", "88b"]})
    matches, total, rate = validate_against_truth(computed, truth)
    assert matches == 1 and total == 2 and rate == 0.5
