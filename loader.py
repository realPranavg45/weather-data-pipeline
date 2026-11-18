"""
loader.py
---------
Phase 4 (updated for PostgreSQL): Loads cleaned data into the database.

Key PostgreSQL differences from SQLite:
  - INSERT OR REPLACE  →  INSERT ... ON CONFLICT (tag_id, timestamp) DO UPDATE SET ...
  - %s placeholders     instead of ?
  - Uses psycopg2 cursor for writes, SQLAlchemy engine for pandas reads

ON CONFLICT DO UPDATE (also called "upsert"):
  If a row with the same (tag_id, timestamp) already exists, its value and
  quality_flag are overwritten. This makes reruns safe — no duplicates, no errors.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta, timezone

from db import get_engine

logger = logging.getLogger("loader")


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------
def _now_ist() -> str:
    """Return current time as IST ISO-8601 string."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%dT%H:%M:%S%z")


# ---------------------------------------------------------------------------
# pipeline_runs helpers
# ---------------------------------------------------------------------------
def start_run(conn, mode: str, date_from: str = None, date_to: str = None) -> int:
    """
    Insert a 'running' row into pipeline_runs and return the run_id.

    PostgreSQL returns the new SERIAL id using RETURNING run_id.
    (SQLite used cursor.lastrowid; PostgreSQL's equivalent is RETURNING.)
    """
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_runs (start_time, mode, date_from, date_to, status)
        VALUES (%s, %s, %s, %s, 'running')
        RETURNING run_id;
    """, (_now_ist(), mode, date_from, date_to))

    run_id = cursor.fetchone()[0]   # RETURNING gives us the new run_id directly
    conn.commit()

    logger.info(f"Pipeline run started — run_id={run_id}, mode={mode}")
    return run_id


def finish_run(conn, run_id: int, rows_loaded: int, rows_rejected: int, status: str = "success") -> None:
    """Update the pipeline_runs row when the run ends."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE pipeline_runs
        SET end_time      = %s,
            rows_loaded   = %s,
            rows_rejected = %s,
            status        = %s
        WHERE run_id = %s;
    """, (_now_ist(), rows_loaded, rows_rejected, status, run_id))
    conn.commit()
    logger.info(f"Run finished — run_id={run_id}, status={status}, loaded={rows_loaded}, rejected={rows_rejected}")


# ---------------------------------------------------------------------------
# Main load function
# ---------------------------------------------------------------------------
def load_data(conn, df: pd.DataFrame) -> int:
    """
    Insert every row in `df` into weather_data using an upsert.

    PostgreSQL upsert syntax:
      INSERT INTO weather_data (tag_id, timestamp, value, quality_flag)
      VALUES (...)
      ON CONFLICT (tag_id, timestamp)        ← the UNIQUE constraint
      DO UPDATE SET                          ← overwrite with new values
          value        = EXCLUDED.value,
          quality_flag = EXCLUDED.quality_flag;

    EXCLUDED refers to the row that was being inserted (but conflicted).
    This is cleaner than SQLite's INSERT OR REPLACE, which deletes + re-inserts.

    Args:
        conn : open psycopg2 connection
        df   : cleaned DataFrame from cleaner.py

    Returns:
        Number of rows inserted or updated.
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing to load.")
        return 0

    cursor = conn.cursor()
    rows_loaded = 0
    required = ["tag_id", "timestamp", "value", "quality_flag"]

    for _, row in df[required].iterrows():
        try:
            # Convert pandas NaN to Python None so psycopg2 stores NULL
            value = None if pd.isna(row["value"]) else float(row["value"])

            cursor.execute("""
                INSERT INTO weather_data (tag_id, timestamp, value, quality_flag)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tag_id, timestamp) DO UPDATE SET
                    value        = EXCLUDED.value,
                    quality_flag = EXCLUDED.quality_flag;
            """, (row["tag_id"], row["timestamp"], value, row["quality_flag"]))

            rows_loaded += 1

        except Exception as e:
            logger.error(f"Failed to insert row {row.to_dict()}: {e}")
            conn.rollback()   # roll back failed row, continue with rest

    conn.commit()
    logger.info(f"Loaded {rows_loaded} rows into weather_data.")
    return rows_loaded


# ---------------------------------------------------------------------------
# Helper: load tag_map into a DataFrame (used by cleaner.add_quality_flag)
# ---------------------------------------------------------------------------
def load_tag_map(config: dict) -> pd.DataFrame:
    """
    Read tag_map from PostgreSQL into a pandas DataFrame.

    Uses the SQLAlchemy engine because pandas 2.x requires it for
    non-SQLite databases (raw psycopg2 connections are no longer supported
    by pd.read_sql_query in newer pandas versions).
    """
    engine = get_engine(config)
    with engine.connect() as pg_conn:
        df = pd.read_sql_query("SELECT * FROM tag_map;", pg_conn)
    return df


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json
    import logging
    from db import get_connection, load_config

    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

    config = load_config()
    conn   = get_connection(config)

    tag_map_df = load_tag_map(config)
    print("Tag map loaded from PostgreSQL:")
    print(tag_map_df[["tag_id", "valid_min", "valid_max"]].to_string(index=False))

    run_id = start_run(conn, mode="live")
    finish_run(conn, run_id, rows_loaded=0, rows_rejected=0, status="success")

    engine = get_engine(config)
    with engine.connect() as pg_conn:
        runs = pd.read_sql_query("SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT 5;", pg_conn)
    print("\nLatest pipeline_runs:")
    print(runs.to_string(index=False))

    conn.close()
