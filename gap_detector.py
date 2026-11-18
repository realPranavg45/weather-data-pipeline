"""
gap_detector.py
---------------
Phase 5 (updated for PostgreSQL): Finds missing hourly timestamps and backfills.

PostgreSQL changes from the SQLite version:
  - Uses psycopg2 connection (from db.py) instead of sqlite3
  - Uses SQLAlchemy engine for pd.read_sql_query calls
  - %s placeholders instead of ?
  - All logic (find_gaps, backfill, print_gap_report) is identical

Usage:
  python gap_detector.py --start 2024-01-01 --end 2024-01-07
  python gap_detector.py --start 2024-01-01 --end 2024-01-07 --dry-run
"""

import logging
import argparse
import pandas as pd

from db        import get_connection, get_engine, load_config
from extractor import extract, setup_logging
from cleaner   import clean
from loader    import load_data, load_tag_map

logger = logging.getLogger("gap_detector")


# ---------------------------------------------------------------------------
# Step 1: Find gaps
# ---------------------------------------------------------------------------
def find_gaps(config: dict, start: str, end: str) -> pd.DataFrame:
    """
    Compare every expected (tag_id, timestamp) pair against what's in the DB.
    Returns a DataFrame of missing pairs.

    Args:
        config : loaded config dict (used to get engine + tag list)
        start  : 'YYYY-MM-DD' start date (inclusive)
        end    : 'YYYY-MM-DD' end date   (inclusive)

    Returns:
        DataFrame with columns ['tag_id', 'timestamp']
    """
    # --- Build the full expected index ---
    hourly_range = pd.date_range(
        start = start,
        end   = pd.Timestamp(end) + pd.Timedelta(hours=23),
        freq  = "h",
        tz    = "Asia/Kolkata"
    )
    hourly_strings = hourly_range.strftime("%Y-%m-%dT%H:%M:%S%z").tolist()

    # Get all tag IDs from tag_map
    engine = get_engine(config)
    with engine.connect() as pg_conn:
        tag_ids = pd.read_sql_query("SELECT tag_id FROM tag_map;", pg_conn)["tag_id"].tolist()

    # Cartesian product: every (tag_id, timestamp) that should exist
    expected_df = pd.DataFrame(
        pd.MultiIndex.from_product([tag_ids, hourly_strings], names=["tag_id", "timestamp"]).tolist(),
        columns=["tag_id", "timestamp"]
    )
    logger.info(f"Expected rows in {start} to {end}: {len(expected_df)}")

    # --- Load what's already in the DB ---
    with engine.connect() as pg_conn:
        existing_df = pd.read_sql_query("""
            SELECT tag_id, timestamp
            FROM   weather_data
            WHERE  timestamp >= %(start)s
              AND  timestamp <= %(end)s;
        """, pg_conn, params={
            "start": f"{start}T00:00:00+0530",
            "end":   f"{end}T23:00:00+0530"
        })

    logger.info(f"Rows already in DB: {len(existing_df)}")

    # --- Set difference → gaps ---
    merged   = expected_df.merge(existing_df, on=["tag_id", "timestamp"], how="left", indicator=True)
    gaps_df  = merged[merged["_merge"] == "left_only"][["tag_id", "timestamp"]].reset_index(drop=True)

    logger.info(f"Gaps found: {len(gaps_df)}")
    return gaps_df


# ---------------------------------------------------------------------------
# Step 2: Backfill
# ---------------------------------------------------------------------------
def backfill(config: dict, gaps_df: pd.DataFrame) -> int:
    """
    Fetch archive data for dates in gaps_df and load into the DB.
    Uses the existing extract → clean → load pipeline.
    Safe to run multiple times (upsert in load_data prevents duplicates).
    """
    if gaps_df.empty:
        logger.info("No gaps to backfill.")
        return 0

    gaps_df      = gaps_df.copy()
    gaps_df["date"] = gaps_df["timestamp"].str[:10]

    unique_dates = sorted(gaps_df["date"].unique())
    start_date   = unique_dates[0]
    end_date     = unique_dates[-1]

    logger.info(f"Backfilling {start_date} to {end_date} ({len(unique_dates)} unique dates)...")

    raw_rows   = extract(config, mode="historical", start=start_date, end=end_date, logger=logger)
    tag_map_df = load_tag_map(config)
    good_df, _ = clean(raw_rows, tag_map_df)

    conn        = get_connection(config)
    rows_loaded = load_data(conn, good_df)
    conn.close()

    logger.info(f"Backfill complete. {rows_loaded} rows inserted/updated.")
    return rows_loaded


# ---------------------------------------------------------------------------
# Gap report
# ---------------------------------------------------------------------------
def print_gap_report(gaps_df: pd.DataFrame) -> None:
    if gaps_df.empty:
        print("\n✅ No gaps found — all hourly data is present.")
        return

    print(f"\n{'=' * 55}")
    print(f"GAP REPORT — {len(gaps_df)} missing (tag, timestamp) pairs")
    print(f"{'=' * 55}")
    for tag_id, count in gaps_df.groupby("tag_id").size().items():
        print(f"  {tag_id:<12}  {count:>4} missing hours")
    print(f"{'=' * 55}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Gap detector and backfill script.")
    parser.add_argument("--start",    required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",      required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--dry-run",  action="store_true", help="Detect only, do not backfill")
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    config = load_config()
    logger = setup_logging(config["logging"]["file"], config["logging"]["level"])

    logger.info("GAP DETECTION STARTED")

    gaps_df = find_gaps(config, args.start, args.end)
    print_gap_report(gaps_df)

    if not args.dry_run and not gaps_df.empty:
        backfill(config, gaps_df)
    elif args.dry_run:
        logger.info("Dry run — skipping backfill.")

    logger.info("GAP DETECTION COMPLETE")
