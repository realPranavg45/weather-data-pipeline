"""
verify.py — read-only verification script (PostgreSQL version)
Run: python verify.py
"""
import pandas as pd
from db import get_engine, load_config

config = load_config()
engine = get_engine(config)

with engine.connect() as conn:
    print("=== pipeline_runs ===")
    runs = pd.read_sql_query(
        "SELECT run_id, mode, rows_loaded, rows_rejected, status, start_time FROM pipeline_runs ORDER BY run_id;",
        conn
    )
    print(runs.to_string(index=False))

    print("\n=== Daily Summary (first 12 rows) ===")
    with open("sql/daily_summary.sql") as f:
        sql = f.read()
    df = pd.read_sql_query(sql, conn)
    print(df.head(12).to_string(index=False))

    print("\n=== 7-day Moving Average (VAD_TEMP, first 7 rows) ===")
    with open("sql/moving_average.sql") as f:
        sql2 = f.read()
    df2 = pd.read_sql_query(sql2, conn)
    print(df2[df2["tag_id"] == "VAD_TEMP"].head(7).to_string(index=False))

    print("\n=== Total rows in weather_data by quality_flag ===")
    qf = pd.read_sql_query(
        "SELECT quality_flag, COUNT(*) as count FROM weather_data GROUP BY quality_flag;",
        conn
    )
    print(qf.to_string(index=False))
