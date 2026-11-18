"""
cleaner.py
----------
Phase 3: Cleans the raw rows produced by extractor.py using pandas.

Cleaning steps (in order):
  1. Convert raw list of dicts → pandas DataFrame
  2. Parse timestamps → timezone-aware datetime in IST (Asia/Kolkata = UTC+5:30)
  3. Cast 'value' column to float (handles None / null gracefully)
  4. Remove exact duplicate rows (same tag_id + timestamp)
  5. Add 'quality_flag' column:
       'missing'      → value is NaN / null
       'out_of_range' → value is outside the valid min/max for that tag
       'good'         → everything looks correct

Returns a cleaned DataFrame ready for the loader.
"""

import pandas as pd
import logging


# ---------------------------------------------------------------------------
# Logger — uses the same logger name so messages go to the same file
# ---------------------------------------------------------------------------
logger = logging.getLogger("cleaner")


# ---------------------------------------------------------------------------
# Step 1 + 2 + 3: Convert, parse timestamps, cast types
# ---------------------------------------------------------------------------
def to_dataframe(raw_rows: list[dict]) -> pd.DataFrame:
    """
    Convert the raw list-of-dicts from the extractor into a DataFrame,
    and do basic type conversions.

    Why pandas?
      - Handles large datasets efficiently in memory
      - Makes column-level operations (type casting, deduplication) one-liners
    """
    if not raw_rows:
        logger.warning("No raw rows received — returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(raw_rows)
    logger.info(f"DataFrame created: {len(df)} rows, columns: {list(df.columns)}")
    return df


def parse_timestamps(df: pd.DataFrame, timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    Convert the 'timestamp' column from a plain string (e.g. '2024-01-01T10:00')
    to a timezone-aware pandas Timestamp in IST.

    Why one timezone?
      If your pipeline runs across midnight or you later add international
      cities, mixing naive (no-tz) and aware (tz) datetimes causes bugs.
      Standardising on IST (UTC+5:30) keeps everything comparable.

    Steps:
      1. pd.to_datetime() parses the string
      2. dt.tz_localize() marks it as IST (since Open-Meteo returns local time)
      3. The result is stored back as ISO-8601 string with offset for SQLite
    """
    df = df.copy()  # never modify the input in-place

    # Open-Meteo returns timestamps without timezone info (e.g. '2024-01-01T10:00')
    # We tell pandas these are already in IST
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Localize to IST — 'nonexistent' and 'ambiguous' handle DST edge cases
    # (India doesn't observe DST, so this is mostly for safety)
    df["timestamp"] = df["timestamp"].dt.tz_localize(
        timezone,
        nonexistent="shift_forward",
        ambiguous="infer"
    )

    # Store as ISO string with offset: '2024-01-01T10:00:00+05:30'
    # SQLite doesn't have a native datetime type; TEXT is the standard choice.
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    logger.info("Timestamps parsed and localized to IST.")
    return df


def cast_value_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast 'value' to float.

    The API sometimes returns None (Python null) for missing hours.
    pd.to_numeric with errors='coerce' turns anything unparseable into NaN,
    which is pandas' way of representing missing numbers.
    """
    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    missing_count = df["value"].isna().sum()
    logger.info(f"Value column cast to float. Missing values found: {missing_count}")
    return df


# ---------------------------------------------------------------------------
# Step 4: Remove duplicates
# ---------------------------------------------------------------------------
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows where both 'tag_id' and 'timestamp' are identical.

    This can happen if the extractor is called twice by mistake, or if
    the API returns overlapping windows (e.g. two 'live' calls within an hour).

    keep='first' means the first occurrence is kept, the rest are dropped.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["tag_id", "timestamp"], keep="first")
    after = len(df)
    removed = before - after
    if removed > 0:
        logger.info(f"Removed {removed} duplicate rows (kept {after}).")
    else:
        logger.info("No duplicate rows found.")
    return df


# ---------------------------------------------------------------------------
# Step 5: Quality flag
# ---------------------------------------------------------------------------
def add_quality_flag(df: pd.DataFrame, tag_map_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'quality_flag' column with one of three values:
      'missing'      → value is NaN
      'out_of_range' → value exists but falls outside [valid_min, valid_max]
      'good'         → value exists and is within range

    We join with tag_map_df to get the valid_min / valid_max per tag.

    Args:
        df         : cleaned weather DataFrame
        tag_map_df : DataFrame loaded from the tag_map table (has valid_min, valid_max)
    """
    df = df.copy()

    # Merge weather data with tag metadata on tag_id
    # 'left' join keeps all weather rows; tags that don't match get NaN limits
    df = df.merge(
        tag_map_df[["tag_id", "valid_min", "valid_max"]],
        on="tag_id",
        how="left"
    )

    # Start everyone as 'good', then override
    df["quality_flag"] = "good"

    # Rule 1: missing value
    df.loc[df["value"].isna(), "quality_flag"] = "missing"

    # Rule 2: out of range (only applies where value is NOT missing)
    out_of_range_mask = (
        df["value"].notna() &
        (
            (df["value"] < df["valid_min"]) |
            (df["value"] > df["valid_max"])
        )
    )
    df.loc[out_of_range_mask, "quality_flag"] = "out_of_range"

    # Count for logging
    counts = df["quality_flag"].value_counts().to_dict()
    logger.info(f"Quality flags assigned: {counts}")

    # Drop the helper columns we merged in (they live in the DB, not this table)
    df = df.drop(columns=["valid_min", "valid_max"], errors="ignore")

    return df


# ---------------------------------------------------------------------------
# Main clean function (called by run_pipeline.py)
# ---------------------------------------------------------------------------
def clean(raw_rows: list[dict], tag_map_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full cleaning pipeline. Returns:
      - good_df    : rows with quality_flag in ('good', 'out_of_range') — loadable
      - rejected_df: rows with quality_flag == 'missing' that you may want to log

    Both DataFrames keep all columns so the loader knows what to insert.

    Why return out_of_range rows?
      In a real industrial system, out-of-range readings are still real data
      — a sensor can legitimately read a strange value. We store them with
      the flag so analysts can decide whether to include them in analysis.
    """
    df = to_dataframe(raw_rows)
    if df.empty:
        return df, df

    df = parse_timestamps(df)
    df = cast_value_column(df)
    df = remove_duplicates(df)
    df = add_quality_flag(df, tag_map_df)

    # Separate into loadable vs rejected
    # 'missing' rows are still inserted (with NULL value) — they represent
    # real gaps that the gap detector will later look for.
    # We track them in the run log as 'rows_rejected' for visibility.
    rejected_df = df[df["quality_flag"] == "missing"].copy()
    good_df     = df.copy()  # all rows go to DB, including 'missing'

    logger.info(f"Cleaning complete. {len(good_df)} rows ready to load, {len(rejected_df)} flagged missing.")
    return good_df, rejected_df


# ---------------------------------------------------------------------------
# Standalone test run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    # Quick sanity-check using fake data
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

    sample_rows = [
        {"city": "Vadodara", "tag_prefix": "VAD", "api_variable": "temperature_2m",
         "tag_suffix": "TEMP", "tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},
        {"city": "Vadodara", "tag_prefix": "VAD", "api_variable": "temperature_2m",
         "tag_suffix": "TEMP", "tag_id": "VAD_TEMP", "timestamp": "2024-06-01T10:00", "value": 32.5},  # duplicate
        {"city": "Vadodara", "tag_prefix": "VAD", "api_variable": "temperature_2m",
         "tag_suffix": "TEMP", "tag_id": "VAD_TEMP", "timestamp": "2024-06-01T11:00", "value": None},   # missing
        {"city": "Vadodara", "tag_prefix": "VAD", "api_variable": "temperature_2m",
         "tag_suffix": "TEMP", "tag_id": "VAD_TEMP", "timestamp": "2024-06-01T12:00", "value": 999.9}, # out of range
    ]

    tag_map_df = pd.DataFrame([
        {"tag_id": "VAD_TEMP", "valid_min": -10.0, "valid_max": 55.0},
        {"tag_id": "VAD_HUM",  "valid_min": 0.0,   "valid_max": 100.0},
        {"tag_id": "VAD_WIND", "valid_min": 0.0,   "valid_max": 200.0},
    ])

    good_df, rejected_df = clean(sample_rows, tag_map_df)
    print("\nCleaned DataFrame:")
    print(good_df[["tag_id", "timestamp", "value", "quality_flag"]].to_string(index=False))
    print(f"\nRows flagged missing: {len(rejected_df)}")
