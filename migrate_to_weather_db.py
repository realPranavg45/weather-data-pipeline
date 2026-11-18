"""
migrate_to_weather_db.py
------------------------
One-time migration script:
  1. Creates a new 'weather' database in PostgreSQL
  2. Creates all tables (tag_map, weather_data, pipeline_runs)
  3. Copies all data from 'Main' to 'weather'
  4. Verifies the migration

Run:  python migrate_to_weather_db.py
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import sys


def load_config(path="config.json"):
    with open(path) as f:
        return json.load(f)


def create_database(config):
    """
    Create the 'weather' database.
    CREATE DATABASE cannot run inside a transaction, so we set autocommit mode.
    """
    db = config["database"]

    # Connect to the default 'postgres' database to issue CREATE DATABASE
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname="postgres",
        user=db["user"],
        password=db["password"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Check if 'weather' database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'weather';")
    exists = cur.fetchone()

    if exists:
        print("[INFO] Database 'weather' already exists. Skipping creation.")
    else:
        cur.execute("CREATE DATABASE weather;")
        print("[OK] Database 'weather' created successfully.")

    cur.close()
    conn.close()


def create_tables_in_weather(config):
    """Create all required tables in the new 'weather' database."""
    db = config["database"]

    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname="weather",
        user=db["user"],
        password=db["password"]
    )
    cur = conn.cursor()

    cur.execute("""
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
    print("[OK] Table 'tag_map' created in 'weather'.")

    cur.execute("""
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
    print("[OK] Table 'weather_data' created in 'weather'.")

    cur.execute("""
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
    print("[OK] Table 'pipeline_runs' created in 'weather'.")

    conn.commit()
    cur.close()
    conn.close()


def migrate_data(config):
    """Copy all data from 'Main' to 'weather'."""
    db = config["database"]

    # Source connection (Main)
    src = psycopg2.connect(
        host=db["host"], port=db["port"],
        dbname=db["name"], user=db["user"], password=db["password"]
    )
    # Destination connection (weather)
    dst = psycopg2.connect(
        host=db["host"], port=db["port"],
        dbname="weather", user=db["user"], password=db["password"]
    )

    src_cur = src.cursor()
    dst_cur = dst.cursor()

    # --- Migrate tag_map ---
    src_cur.execute("SELECT tag_id, city, tag_prefix, measurement, api_variable, unit, valid_min, valid_max FROM tag_map;")
    tag_rows = src_cur.fetchall()
    for row in tag_rows:
        dst_cur.execute("""
            INSERT INTO tag_map (tag_id, city, tag_prefix, measurement, api_variable, unit, valid_min, valid_max)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tag_id) DO NOTHING;
        """, row)
    dst.commit()
    print(f"[OK] Migrated {len(tag_rows)} rows to tag_map.")

    # --- Migrate weather_data ---
    src_cur.execute("SELECT tag_id, timestamp, value, quality_flag FROM weather_data;")
    weather_rows = src_cur.fetchall()
    for row in weather_rows:
        dst_cur.execute("""
            INSERT INTO weather_data (tag_id, timestamp, value, quality_flag)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tag_id, timestamp) DO NOTHING;
        """, row)
    dst.commit()
    print(f"[OK] Migrated {len(weather_rows)} rows to weather_data.")

    # --- Migrate pipeline_runs ---
    src_cur.execute("SELECT start_time, end_time, mode, date_from, date_to, rows_loaded, rows_rejected, status FROM pipeline_runs;")
    run_rows = src_cur.fetchall()
    for row in run_rows:
        dst_cur.execute("""
            INSERT INTO pipeline_runs (start_time, end_time, mode, date_from, date_to, rows_loaded, rows_rejected, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, row)
    dst.commit()
    print(f"[OK] Migrated {len(run_rows)} rows to pipeline_runs.")

    src_cur.close()
    dst_cur.close()
    src.close()
    dst.close()


def verify_migration(config):
    """Compare row counts between Main and weather databases."""
    db = config["database"]

    src = psycopg2.connect(
        host=db["host"], port=db["port"],
        dbname=db["name"], user=db["user"], password=db["password"]
    )
    dst = psycopg2.connect(
        host=db["host"], port=db["port"],
        dbname="weather", user=db["user"], password=db["password"]
    )

    tables = ["tag_map", "weather_data", "pipeline_runs"]
    print("\n" + "=" * 50)
    print("MIGRATION VERIFICATION")
    print("=" * 50)
    print(f"{'Table':<20} {'Main DB':<10} {'Weather DB':<10} {'Match'}")
    print("-" * 50)

    all_match = True
    for table in tables:
        src_cur = src.cursor()
        dst_cur = dst.cursor()

        src_cur.execute(f"SELECT COUNT(*) FROM {table};")
        src_count = src_cur.fetchone()[0]

        dst_cur.execute(f"SELECT COUNT(*) FROM {table};")
        dst_count = dst_cur.fetchone()[0]

        match = "OK" if src_count == dst_count else "FAIL"
        if src_count != dst_count:
            all_match = False

        print(f"{table:<20} {src_count:<10} {dst_count:<10} {match}")

        src_cur.close()
        dst_cur.close()

    print("=" * 50)
    if all_match:
        print("[OK] All data migrated successfully!")
    else:
        print("[FAIL] Some data was not migrated. Check above.")
    print()

    src.close()
    dst.close()
    return all_match


if __name__ == "__main__":
    config = load_config()

    print("=" * 50)
    print("MIGRATING FROM 'Main' -> 'weather' DATABASE")
    print("=" * 50)
    print()

    # Step 1: Create the database
    create_database(config)

    # Step 2: Create tables
    create_tables_in_weather(config)

    # Step 3: Copy data
    migrate_data(config)

    # Step 4: Verify
    success = verify_migration(config)

    if success:
        print("Migration complete!")
        print("   Next step: Update config.json to set database.name = 'weather'")
    else:
        print("Migration had issues. Please check the output above.")
        sys.exit(1)
