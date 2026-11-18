"""
db_setup.py
-----------
Phase 1 (updated): Creates all PostgreSQL tables and populates tag_map.

Key PostgreSQL differences from SQLite:
  - Uses psycopg2 driver instead of sqlite3
  - SERIAL replaces INTEGER AUTOINCREMENT
  - %s placeholders instead of ?
  - INSERT ... ON CONFLICT DO NOTHING  instead of INSERT OR IGNORE
  - No PRAGMA statements needed (PostgreSQL enforces FK by default per session)

Run once to initialise the database:
    python db_setup.py
"""

import json
import logging
import psycopg2

from db import get_connection, load_config

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------
def create_tables(conn) -> None:
    """
    Create all required tables if they don't already exist.
    Safe to run multiple times — IF NOT EXISTS prevents errors on re-runs.

    PostgreSQL-specific notes:
      SERIAL       → auto-incrementing integer (equivalent to SQLite's AUTOINCREMENT)
      TEXT         → variable-length string (no length limit in PostgreSQL)
      REAL         → 4-byte floating point (same as SQLite)
    """
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # TABLE 1: tag_map
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tag_map (
            tag_id       TEXT PRIMARY KEY,
            city         TEXT NOT NULL,
            tag_prefix   TEXT NOT NULL,
            measurement  TEXT NOT NULL,
            api_variable TEXT NOT NULL,
            unit         TEXT NOT NULL,
            valid_min    REAL,
            valid_max    REAL
        );
    """)
    logger.info("Table 'tag_map' ready.")

    # ------------------------------------------------------------------
    # TABLE 2: weather_data
    # UNIQUE constraint on (tag_id, timestamp) prevents duplicates.
    # INSERT ... ON CONFLICT will use this constraint in the loader.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id           SERIAL PRIMARY KEY,
            tag_id       TEXT   NOT NULL,
            timestamp    TEXT   NOT NULL,
            value        REAL,
            quality_flag TEXT   NOT NULL DEFAULT 'good',

            UNIQUE (tag_id, timestamp),
            FOREIGN KEY (tag_id) REFERENCES tag_map(tag_id)
        );
    """)
    logger.info("Table 'weather_data' ready.")

    # ------------------------------------------------------------------
    # TABLE 3: pipeline_runs
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id        SERIAL PRIMARY KEY,
            start_time    TEXT   NOT NULL,
            end_time      TEXT,
            mode          TEXT   NOT NULL,
            date_from     TEXT,
            date_to       TEXT,
            rows_loaded   INTEGER DEFAULT 0,
            rows_rejected INTEGER DEFAULT 0,
            status        TEXT    DEFAULT 'running'
        );
    """)
    logger.info("Table 'pipeline_runs' ready.")

    conn.commit()


# ---------------------------------------------------------------------------
# Populate tag_map
# ---------------------------------------------------------------------------
def populate_tag_map(conn, config: dict) -> None:
    """
    Insert one row per (city × variable) into tag_map.

    PostgreSQL equivalent of SQLite's INSERT OR IGNORE:
      INSERT INTO ... ON CONFLICT (tag_id) DO NOTHING

    This means: if the tag already exists, skip it silently.
    Re-running this function never raises an error.
    """
    cursor = conn.cursor()
    rows_inserted = 0

    for city in config["cities"]:
        for variable in config["variables"]:
            tag_id = f"{city['tag_prefix']}_{variable['tag_suffix']}"

            cursor.execute("""
                INSERT INTO tag_map
                    (tag_id, city, tag_prefix, measurement, api_variable, unit, valid_min, valid_max)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tag_id) DO NOTHING;
            """, (
                tag_id,
                city["name"],
                city["tag_prefix"],
                variable["measurement"],
                variable["api_name"],
                variable["unit"],
                variable["valid_min"],
                variable["valid_max"]
            ))

            # rowcount = 1 means a row was actually inserted (not skipped)
            if cursor.rowcount > 0:
                rows_inserted += 1
                logger.info(f"  Inserted: {tag_id} → {city['name']} | {variable['measurement']} ({variable['unit']})")

    conn.commit()
    logger.info(f"tag_map populated — {rows_inserted} new rows inserted (existing rows skipped).")


# ---------------------------------------------------------------------------
# Display tag_map
# ---------------------------------------------------------------------------
def show_tag_map(conn) -> None:
    """Print the tag_map table to the console for verification."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT tag_id, city, measurement, unit, valid_min, valid_max
        FROM tag_map
        ORDER BY tag_id;
    """)
    rows = cursor.fetchall()

    print("\n" + "=" * 70)
    print(f"{'TAG_ID':<12} {'CITY':<12} {'MEASUREMENT':<20} {'UNIT':<6} {'MIN':>6} {'MAX':>6}")
    print("=" * 70)
    for row in rows:
        print(f"{row[0]:<12} {row[1]:<12} {row[2]:<20} {row[3]:<6} {row[4]:>6} {row[5]:>6}")
    print("=" * 70)
    print(f"Total tags: {len(rows)}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    config = load_config()
    db     = config["database"]

    logger.info(f"Connecting to PostgreSQL → {db['host']}:{db['port']}/{db['name']}")

    try:
        conn = get_connection(config)
        logger.info("Connected successfully.")

        create_tables(conn)
        populate_tag_map(conn, config)
        show_tag_map(conn)

        conn.close()
        logger.info("Database setup complete. Ready for the pipeline.")

    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise
