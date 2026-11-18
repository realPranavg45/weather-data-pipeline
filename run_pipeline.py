"""
run_pipeline.py
---------------
Main entry-point (updated for PostgreSQL).

Usage:
  python run_pipeline.py live
  python run_pipeline.py historical --start 2024-01-01 --end 2024-01-07
"""

import logging
import argparse
import sys

from db        import get_connection, load_config
from extractor import extract, setup_logging
from cleaner   import clean
from loader    import load_data, load_tag_map, start_run, finish_run


def parse_args():
    parser = argparse.ArgumentParser(description="Weather Data Pipeline")
    parser.add_argument("mode", choices=["live", "historical"])
    parser.add_argument("--start", type=str, default=None, help="YYYY-MM-DD")
    parser.add_argument("--end",   type=str, default=None, help="YYYY-MM-DD")
    return parser.parse_args()


def main():
    args   = parse_args()
    config = load_config()

    if args.mode == "historical" and (not args.start or not args.end):
        print("ERROR: historical mode requires --start and --end.")
        sys.exit(1)

    logger = setup_logging(config["logging"]["file"], config["logging"]["level"])
    logger.info("=" * 60)
    logger.info("PIPELINE RUN STARTING")
    logger.info("=" * 60)

    # Open PostgreSQL connection
    conn = get_connection(config)

    run_id        = start_run(conn, mode=args.mode, date_from=args.start, date_to=args.end)
    rows_loaded   = 0
    rows_rejected = 0
    status        = "success"

    try:
        # EXTRACT
        raw_rows = extract(config, args.mode, args.start, args.end, logger)

        # CLEAN
        tag_map_df          = load_tag_map(config)   # reads from PostgreSQL via SQLAlchemy
        good_df, rejected_df = clean(raw_rows, tag_map_df)
        rows_rejected        = len(rejected_df)

        # LOAD
        rows_loaded = load_data(conn, good_df)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        status = "failed"

    finally:
        finish_run(conn, run_id, rows_loaded, rows_rejected, status)
        conn.close()
        logger.info("PIPELINE RUN COMPLETE")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
