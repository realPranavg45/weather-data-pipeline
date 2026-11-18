"""
tests/test_gap_detector.py
--------------------------
pytest tests for gap_detector.py — PostgreSQL version.

Isolation strategy:
  Each test uses a unique date range that no production run would ever touch
  (year 2020 dates). After the test, any rows inserted are explicitly deleted
  in the teardown step.

  This avoids SAVEPOINTs entirely (which differ between drivers) and keeps
  the test logic simple and readable.

Run with:
    pytest tests/ -v
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import pandas as pd

from db        import get_connection, load_config
from db_setup  import create_tables, populate_tag_map
from gap_detector import find_gaps


# ---------------------------------------------------------------------------
# Session-scoped fixtures (run once for all tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def config():
    """Load config once and share it across all tests."""
    return load_config()


@pytest.fixture(scope="session", autouse=True)
def ensure_schema(config):
    """
    Before any tests run:
      1. Make sure the tables exist in PostgreSQL.
      2. Make sure VAD_TEMP is in tag_map (needed by find_gaps).

    autouse=True means this runs automatically without tests asking for it.
    """
    conn = get_connection(config)
    create_tables(conn)

    mini_config = {
        "cities": [{
            "name": "Vadodara", "tag_prefix": "VAD",
            "latitude": 22.3072, "longitude": 73.1812, "timezone": "Asia/Kolkata"
        }],
        "variables": [{
            "api_name": "temperature_2m", "tag_suffix": "TEMP",
            "measurement": "Temperature", "unit": "C",
            "valid_min": -10, "valid_max": 55
        }]
    }
    populate_tag_map(conn, mini_config)
    conn.close()


# ---------------------------------------------------------------------------
# Per-test fixture: a fresh connection with cleanup
# ---------------------------------------------------------------------------

@pytest.fixture
def pg_conn(config):
    """
    Open a psycopg2 connection for one test.
    After the test finishes, delete all weather_data rows for year 2020
    so tests don't interfere with each other or with real data.
    """
    conn = get_connection(config)

    yield conn   # test runs here

    # Teardown: clean up test rows
    cur = conn.cursor()
    cur.execute("DELETE FROM weather_data WHERE timestamp LIKE '2020%';")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Helper: insert a weather_data row using a cursor
# ---------------------------------------------------------------------------

def insert_row(conn, tag_id: str, timestamp: str, value: float = 25.0):
    """
    Insert a single row into weather_data.
    Uses ON CONFLICT DO NOTHING so it's safe to call multiple times.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_data (tag_id, timestamp, value, quality_flag)
        VALUES (%s, %s, %s, 'good')
        ON CONFLICT (tag_id, timestamp) DO NOTHING;
    """, (tag_id, timestamp, value))
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_find_gaps_returns_all_missing_when_db_is_empty(config, pg_conn):
    """
    With no data for 2020-01-01, all 24 hours for VAD_TEMP should be gaps.
    """
    # Make sure the date is clean
    cur = pg_conn.cursor()
    cur.execute("DELETE FROM weather_data WHERE tag_id='VAD_TEMP' AND timestamp LIKE '2020-01-01%';")
    pg_conn.commit()

    gaps     = find_gaps(config, start="2020-01-01", end="2020-01-01")
    vad_gaps = gaps[gaps["tag_id"] == "VAD_TEMP"]
    assert len(vad_gaps) == 24, f"Expected 24 gaps, got {len(vad_gaps)}"


def test_find_gaps_returns_empty_when_all_data_present(config, pg_conn):
    """
    If every hourly timestamp for VAD_TEMP on 2020-02-01 is present,
    find_gaps should return zero gaps for that tag and date.
    """
    for hour in range(24):
        ts = f"2020-02-01T{hour:02d}:00:00+0530"
        insert_row(pg_conn, "VAD_TEMP", ts)

    gaps     = find_gaps(config, start="2020-02-01", end="2020-02-01")
    vad_gaps = gaps[gaps["tag_id"] == "VAD_TEMP"]
    assert vad_gaps.empty, "Should find no gaps when all 24 hours are present"


def test_find_gaps_detects_specific_missing_hours(config, pg_conn):
    """
    Insert all hours except 05:00 and 10:00.
    find_gaps must report exactly 2 missing hours.
    """
    missing_hours = {5, 10}
    for hour in range(24):
        if hour not in missing_hours:
            ts = f"2020-03-01T{hour:02d}:00:00+0530"
            insert_row(pg_conn, "VAD_TEMP", ts)

    gaps     = find_gaps(config, start="2020-03-01", end="2020-03-01")
    vad_gaps = gaps[gaps["tag_id"] == "VAD_TEMP"]
    assert len(vad_gaps) == 2, f"Expected 2 gaps, got {len(vad_gaps)}"


def test_find_gaps_spans_multiple_days(config, pg_conn):
    """
    Over 2 empty days, find_gaps should return 48 gaps for VAD_TEMP
    (24 hours x 2 days).
    """
    cur = pg_conn.cursor()
    cur.execute("DELETE FROM weather_data WHERE tag_id='VAD_TEMP' AND timestamp LIKE '2020-04%';")
    pg_conn.commit()

    gaps     = find_gaps(config, start="2020-04-01", end="2020-04-02")
    vad_gaps = gaps[gaps["tag_id"] == "VAD_TEMP"]
    assert len(vad_gaps) == 48, f"Expected 48 gaps for 2 empty days, got {len(vad_gaps)}"


def test_find_gaps_is_idempotent(config, pg_conn):
    """
    Running find_gaps twice must produce the same result.
    It should never modify any data.
    """
    gaps1 = find_gaps(config, start="2020-05-01", end="2020-05-01")
    gaps2 = find_gaps(config, start="2020-05-01", end="2020-05-01")
    assert len(gaps1) == len(gaps2), "find_gaps must be idempotent"


def test_find_gaps_result_has_correct_columns(config, pg_conn):
    """find_gaps must always return a DataFrame with [tag_id, timestamp]."""
    gaps = find_gaps(config, start="2020-06-01", end="2020-06-01")
    assert "tag_id"    in gaps.columns
    assert "timestamp" in gaps.columns
