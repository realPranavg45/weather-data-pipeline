"""
tests/test_cleaner.py
---------------------
pytest tests for cleaner.py

Run with:
    pytest tests/ -v

Each test function name starts with 'test_' so pytest discovers it automatically.
Each test covers one specific cleaning rule.
"""

import sys
import os
# Make sure Python can find the project modules (extractor, cleaner, etc.)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
import pytest
from cleaner import (
    to_dataframe,
    parse_timestamps,
    cast_value_column,
    remove_duplicates,
    add_quality_flag,
    clean
)


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_tag_map():
    """A minimal tag_map DataFrame used by multiple tests."""
    return pd.DataFrame([
        {"tag_id": "VAD_TEMP", "valid_min": -10.0, "valid_max": 55.0},
        {"tag_id": "VAD_HUM",  "valid_min":   0.0, "valid_max": 100.0},
        {"tag_id": "VAD_WIND", "valid_min":   0.0, "valid_max": 200.0},
    ])


@pytest.fixture
def sample_rows():
    """A minimal set of raw rows from the extractor."""
    return [
        {
            "city": "Vadodara", "tag_prefix": "VAD",
            "api_variable": "temperature_2m", "tag_suffix": "TEMP",
            "tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5
        },
        {
            "city": "Vadodara", "tag_prefix": "VAD",
            "api_variable": "relative_humidity_2m", "tag_suffix": "HUM",
            "tag_id": "VAD_HUM", "timestamp": "2024-06-01T10:00", "value": 70.0
        },
    ]


# ---------------------------------------------------------------------------
# Test: to_dataframe
# ---------------------------------------------------------------------------

def test_to_dataframe_creates_correct_shape(sample_rows):
    """
    Given 2 raw rows, to_dataframe should return a DataFrame
    with 2 rows and the expected columns.
    """
    df = to_dataframe(sample_rows)
    assert len(df) == 2, "Expected 2 rows"
    assert "tag_id"    in df.columns
    assert "timestamp" in df.columns
    assert "value"     in df.columns


def test_to_dataframe_empty_input():
    """An empty list should return an empty DataFrame (not crash)."""
    df = to_dataframe([])
    assert df.empty, "Expected empty DataFrame for empty input"


# ---------------------------------------------------------------------------
# Test: cast_value_column
# ---------------------------------------------------------------------------

def test_cast_value_column_converts_none_to_nan():
    """
    None values (from the API's JSON null) should become float NaN after casting.
    NaN is pandas' representation of a missing number.
    """
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": None},
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T11:00", "value": 30.0},
    ])
    df = cast_value_column(df)

    # First row should be NaN
    assert pd.isna(df.loc[0, "value"]), "None should become NaN"
    # Second row should stay as a float
    assert df.loc[1, "value"] == 30.0,  "Valid number should stay as-is"


def test_cast_value_column_dtype_is_float():
    """After casting, the value column dtype should be float64."""
    df = pd.DataFrame([{"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": "32.5"}])
    df = cast_value_column(df)
    assert df["value"].dtype in ["float64", "Float64"], "value should be float type"


# ---------------------------------------------------------------------------
# Test: remove_duplicates
# ---------------------------------------------------------------------------

def test_remove_duplicates_drops_exact_duplicate():
    """
    Two rows with the same (tag_id, timestamp) should result in only 1 row.
    """
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},  # exact copy
    ])
    df = remove_duplicates(df)
    assert len(df) == 1, "Duplicate rows should be removed"


def test_remove_duplicates_keeps_different_timestamps():
    """
    Two rows with the same tag_id but different timestamps are NOT duplicates.
    """
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T11:00", "value": 33.0},
    ])
    df = remove_duplicates(df)
    assert len(df) == 2, "Rows with different timestamps should be kept"


def test_remove_duplicates_keeps_different_tags():
    """
    Two rows with the same timestamp but different tag_ids are NOT duplicates.
    """
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},
        {"tag_id": "VAD_HUM",  "timestamp": "2024-06-01T10:00", "value": 70.0},
    ])
    df = remove_duplicates(df)
    assert len(df) == 2, "Rows with different tag_ids should be kept"


# ---------------------------------------------------------------------------
# Test: add_quality_flag
# ---------------------------------------------------------------------------

def test_quality_flag_good(sample_tag_map):
    """A value within the valid range should get flag='good'."""
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5}
    ])
    df = add_quality_flag(df, sample_tag_map)
    assert df.loc[0, "quality_flag"] == "good"


def test_quality_flag_missing(sample_tag_map):
    """A NaN value (missing) should get flag='missing'."""
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": float("nan")}
    ])
    df = add_quality_flag(df, sample_tag_map)
    assert df.loc[0, "quality_flag"] == "missing"


def test_quality_flag_out_of_range_high(sample_tag_map):
    """A value above valid_max should get flag='out_of_range'."""
    df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 999.9}  # max is 55
    ])
    df = add_quality_flag(df, sample_tag_map)
    assert df.loc[0, "quality_flag"] == "out_of_range"


def test_quality_flag_out_of_range_low(sample_tag_map):
    """A value below valid_min should get flag='out_of_range'."""
    df = pd.DataFrame([
        {"tag_id": "VAD_HUM", "timestamp": "2024-06-01T10:00", "value": -5.0}  # min is 0
    ])
    df = add_quality_flag(df, sample_tag_map)
    assert df.loc[0, "quality_flag"] == "out_of_range"


def test_quality_flag_boundary_values_are_good(sample_tag_map):
    """Values exactly at the boundary (valid_min and valid_max) should be 'good'."""
    df = pd.DataFrame([
        {"tag_id": "VAD_HUM", "timestamp": "2024-06-01T10:00", "value": 0.0},    # exactly valid_min
        {"tag_id": "VAD_HUM", "timestamp": "2024-06-01T11:00", "value": 100.0},  # exactly valid_max
    ])
    df = add_quality_flag(df, sample_tag_map)
    assert df.loc[0, "quality_flag"] == "good", "valid_min boundary should be 'good'"
    assert df.loc[1, "quality_flag"] == "good", "valid_max boundary should be 'good'"


# ---------------------------------------------------------------------------
# Test: full clean() pipeline
# ---------------------------------------------------------------------------

def test_clean_pipeline_end_to_end(sample_rows, sample_tag_map):
    """
    The full clean() function should return a non-empty DataFrame
    when given valid raw rows and a valid tag_map.
    """
    good_df, rejected_df = clean(sample_rows, sample_tag_map)
    assert not good_df.empty, "good_df should not be empty for valid input"
    assert "quality_flag" in good_df.columns
    assert "timestamp"    in good_df.columns
    assert "value"        in good_df.columns


def test_clean_pipeline_empty_input(sample_tag_map):
    """clean() should handle empty input gracefully (return empty DataFrames)."""
    good_df, rejected_df = clean([], sample_tag_map)
    assert good_df.empty
    assert rejected_df.empty
