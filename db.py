"""
db.py
-----
Central database connection helper for the Weather Data Pipeline.

Why a separate module?
  Every script (db_setup, loader, gap_detector, run_pipeline) needs a DB
  connection. Putting the connection logic here means you only write it once.
  If the credentials change, you update config.json — nothing else.

Two connection types are provided:
  get_connection()   → a raw psycopg2 connection (for INSERT / UPDATE / DDL)
  get_engine()       → a SQLAlchemy engine  (required by pandas.read_sql_query)

psycopg2  : the standard Python driver for PostgreSQL.
SQLAlchemy: an ORM/toolkit; here we only use it as a thin wrapper so pandas
            can read query results directly into a DataFrame.
"""

import json
import psycopg2
from sqlalchemy import create_engine, text


def load_config(path: str = "config.json") -> dict:
    """Read and return the JSON configuration file."""
    with open(path, "r") as f:
        return json.load(f)


def get_connection(config: dict):
    """
    Open and return a raw psycopg2 connection to the PostgreSQL database.

    Use this for:
      - CREATE TABLE / DDL statements
      - INSERT, UPDATE, DELETE
      - Any query where you iterate rows with a cursor

    The caller is responsible for closing the connection (or use a context manager).

    Args:
        config : the loaded config dict (must have a 'database' section)

    Returns:
        psycopg2 connection object
    """
    db = config["database"]

    conn = psycopg2.connect(
        host     = db["host"],
        port     = db["port"],
        dbname   = db["name"],
        user     = db["user"],
        password = db["password"]
    )

    # autocommit=False (the default) means we control transactions manually.
    # We call conn.commit() after each batch of writes.
    return conn


def get_engine(config: dict):
    """
    Build and return a SQLAlchemy engine for PostgreSQL.

    Use this ONLY for pandas operations:
      pd.read_sql_query(sql, engine)

    pandas 2.x dropped support for raw DBAPI2 connections for SQL reads;
    it now requires a SQLAlchemy engine or connection.

    Args:
        config : the loaded config dict

    Returns:
        SQLAlchemy Engine object
    """
    db = config["database"]

    # Build the connection URL:
    # postgresql+psycopg2://user:password@host:port/dbname
    url = (
        f"postgresql+psycopg2://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )

    # pool_pre_ping=True: before using a pooled connection, test it is alive.
    # This prevents errors after the DB server restarts.
    engine = create_engine(url, pool_pre_ping=True)
    return engine


def test_connection(config: dict) -> bool:
    """
    Try to connect and run a trivial query.
    Returns True on success, False on failure.
    Useful as a quick health-check.
    """
    try:
        conn = get_connection(config)
        cur  = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Quick test when run directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    config = load_config()
    db = config["database"]
    print(f"Testing connection -> {db['host']}:{db['port']}/{db['name']} as {db['user']}")

    if test_connection(config):
        print("[OK] Connection successful!")
    else:
        print("[FAIL] Connection failed - check config.json and ensure PostgreSQL is running.")
