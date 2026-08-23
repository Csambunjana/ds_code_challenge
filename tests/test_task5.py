# ============================================================================
# Unit tests for Task 5 (subsample + anonymisation logic)
# ============================================================================
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.task5_transformations import subsample_near_centroid, anonymise


# Requests near the centroid are kept; far ones are dropped.
def test_subsample_radius():
    df = pd.DataFrame({
        "latitude":  [-33.577, -33.9],     # near Atlantis, and far (central CT)
        "longitude": [18.487, 18.6],
    })
    out = subsample_near_centroid(df, -33.5772, 18.4869, 1.0/60.0)
    assert len(out) == 1                    # only the near one survives


# Anonymise: exact lat/lon removed, h3 res-9 + 6h bucket added.
def test_anonymise_removes_pii():
    df = pd.DataFrame({
        "notification_number": [1, 2, 3],
        "latitude":  [-33.577, -33.577, -33.900],
        "longitude": [18.487, 18.487, 18.600],
        "creation_timestamp": ["2020-01-01 03:00:00+02:00"] * 3,
        "directorate": ["A", "A", "B"],
    })
    anon, review = anonymise(df)
    combined = pd.concat([anon, review])
    assert "latitude" not in combined.columns          # exact coords removed
    assert "notification_number" not in combined.columns  # id removed
    assert "h3_level9_index" in combined.columns        # coarse location added
    assert "creation_6h_bucket" in combined.columns     # coarse time added
