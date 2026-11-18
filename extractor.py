"""
extractor.py
------------
Phase 2: Fetches weather data from the Open-Meteo API.

Two modes (chosen via command-line argument):
  live        → fetches the last 24 hours of data for all cities
  historical  → fetches data for a given date range (--start, --end)

Usage examples:
  python extractor.py live
  python extractor.py historical --start 2024-01-01 --end 2024-01-07

Features:
  - Timeout on every HTTP request (configurable in config.json)
  - Automatic retries with a delay (configurable in config.json)
  - All activity logged to pipeline.log AND printed to the console
  - Returns a list of raw dicts ready for the cleaner (Phase 3)
"""

import json
import time
import logging
import argparse
import requests
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Logging setup — writes to both console and a log file simultaneously
# ---------------------------------------------------------------------------
def setup_logging(log_file: str, level: str = "INFO") -> logging.Logger:
    """
    Configure a logger that writes to two places at once:
      1. The console (so you see output while the script runs)
      2. A log file (so you have a permanent record)
    """
    logger = logging.getLogger("extractor")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [extractor]  %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler — appends to the log file, doesn't overwrite
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Avoid adding duplicate handlers if this module is imported multiple times
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------
def load_config(path: str = "config.json") -> dict:
    """Read and return the JSON configuration file."""
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Core HTTP fetch with retry logic
# ---------------------------------------------------------------------------
def fetch_with_retry(
    url: str,
    params: dict,
    timeout: int,
    max_retries: int,
    retry_delay: int,
    logger: logging.Logger
) -> dict | None:
    """
    Make an HTTP GET request to `url` with `params`.
    If the request fails (network error, bad status code), wait and retry.

    Returns the parsed JSON dict on success, or None if all retries fail.

    Why retries?
      APIs can fail temporarily (network blip, server busy). Retrying
      automatically makes the pipeline more resilient without human help.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"  Attempt {attempt}/{max_retries} → GET {url}")

            # timeout= prevents the script from hanging forever if the
            # server is slow or unreachable
            response = requests.get(url, params=params, timeout=timeout)

            # Raise an exception if the HTTP status is 4xx or 5xx
            response.raise_for_status()

            logger.info(f"  ✓ HTTP {response.status_code} received.")
            return response.json()

        except requests.exceptions.Timeout:
            logger.warning(f"  Timeout on attempt {attempt}. Waiting {retry_delay}s before retry...")

        except requests.exceptions.ConnectionError:
            logger.warning(f"  Connection error on attempt {attempt}. Waiting {retry_delay}s before retry...")

        except requests.exceptions.HTTPError as e:
            logger.error(f"  HTTP error: {e}. Will not retry this error.")
            return None  # HTTP errors (like 404) are not worth retrying

        # Wait before next attempt (only if there is a next attempt)
        if attempt < max_retries:
            time.sleep(retry_delay)

    logger.error(f"  All {max_retries} attempts failed for URL: {url}")
    return None


# ---------------------------------------------------------------------------
# Build query parameters for the Open-Meteo API
# ---------------------------------------------------------------------------
def build_params(city: dict, variables: list, mode: str, start: str = None, end: str = None) -> tuple[str, dict]:
    """
    Build the URL and query parameters for the Open-Meteo API call.

    Open-Meteo has two endpoints:
      - forecast API    → for recent / live data
      - archive API     → for historical data (past dates)

    Parameters returned as a dict are automatically URL-encoded by `requests`.
    """
    # The API expects variable names as a comma-separated list
    variable_names = [v["api_name"] for v in variables]

    # Common parameters for both modes
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(variable_names),
        "timezone": city["timezone"],
    }

    if mode == "live":
        # 'forecast_days=2' gives us today + yesterday = ~48 hours of recent data
        url = "https://api.open-meteo.com/v1/forecast"
        params["forecast_days"] = 2

    else:  # historical
        url = "https://archive-api.open-meteo.com/v1/archive"
        params["start_date"] = start
        params["end_date"] = end

    return url, params


# ---------------------------------------------------------------------------
# Parse a single city's API response into a list of flat dicts
# ---------------------------------------------------------------------------
def parse_response(api_data: dict, city: dict, variables: list) -> list[dict]:
    """
    The Open-Meteo API returns data like this:
      {
        "hourly": {
          "time": ["2024-01-01T00:00", "2024-01-01T01:00", ...],
          "temperature_2m": [22.1, 21.8, ...],
          "relative_humidity_2m": [65, 67, ...],
          "wind_speed_10m": [12.3, 11.9, ...]
        }
      }

    This function "unpivots" that into a list of flat dicts, one per
    (timestamp, variable) combination:
      [
        {"city": "Vadodara", "tag_prefix": "VAD", "api_variable": "temperature_2m",
         "timestamp": "2024-01-01T00:00", "value": 22.1},
        ...
      ]

    Why flat dicts? They are easy to pass to pandas in the cleaner.
    """
    rows = []
    hourly = api_data.get("hourly", {})
    timestamps = hourly.get("time", [])

    for variable in variables:
        api_name = variable["api_name"]
        values = hourly.get(api_name, [])

        for ts, val in zip(timestamps, values):
            rows.append({
                "city":         city["name"],
                "tag_prefix":   city["tag_prefix"],
                "api_variable": api_name,
                "tag_suffix":   variable["tag_suffix"],
                # tag_id = e.g. "VAD_TEMP"
                "tag_id":       f"{city['tag_prefix']}_{variable['tag_suffix']}",
                "timestamp":    ts,       # e.g. "2024-01-01T00:00"
                "value":        val,      # numeric or None if API returned null
            })

    return rows


# ---------------------------------------------------------------------------
# Main extractor function (called by run_pipeline.py and tests)
# ---------------------------------------------------------------------------
def extract(config: dict, mode: str, start: str = None, end: str = None, logger: logging.Logger = None) -> list[dict]:
    """
    Extract weather data for all cities.

    Args:
        config  : loaded config dict
        mode    : 'live' or 'historical'
        start   : start date string 'YYYY-MM-DD' (historical only)
        end     : end date string 'YYYY-MM-DD'   (historical only)
        logger  : logger instance

    Returns:
        List of raw row dicts (before cleaning)
    """
    if logger is None:
        logger = logging.getLogger("extractor")

    all_rows = []
    cities    = config["cities"]
    variables = config["variables"]
    api_cfg   = config["api"]

    logger.info(f"Starting extraction — mode={mode}" +
                (f", range={start} to {end}" if mode == "historical" else ""))

    for city in cities:
        logger.info(f"Fetching data for {city['name']} ({city['latitude']}, {city['longitude']})...")

        url, params = build_params(city, variables, mode, start, end)

        api_data = fetch_with_retry(
            url          = url,
            params       = params,
            timeout      = api_cfg["timeout_seconds"],
            max_retries  = api_cfg["max_retries"],
            retry_delay  = api_cfg["retry_delay_seconds"],
            logger       = logger
        )

        if api_data is None:
            logger.error(f"Skipping {city['name']} — no data received after all retries.")
            continue

        rows = parse_response(api_data, city, variables)
        logger.info(f"  Parsed {len(rows)} raw rows for {city['name']}.")
        all_rows.extend(rows)

    logger.info(f"Extraction complete. Total raw rows: {len(all_rows)}")
    return all_rows


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------
def parse_args():
    """
    Define the command-line interface for running the extractor directly.

    argparse is Python's built-in argument parser — no extra libraries needed.
    """
    parser = argparse.ArgumentParser(
        description="Weather Data Extractor — fetches data from Open-Meteo API."
    )
    parser.add_argument(
        "mode",
        choices=["live", "historical"],
        help="'live' fetches recent data; 'historical' requires --start and --end"
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date for historical mode, format: YYYY-MM-DD"
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date for historical mode, format: YYYY-MM-DD"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    config = load_config("config.json")

    # Validate that historical mode has dates
    if args.mode == "historical" and (args.start is None or args.end is None):
        print("ERROR: historical mode requires --start and --end arguments.")
        print("Example: python extractor.py historical --start 2024-01-01 --end 2024-01-07")
        exit(1)

    logger = setup_logging(
        log_file = config["logging"]["file"],
        level    = config["logging"]["level"]
    )

    rows = extract(config, args.mode, args.start, args.end, logger)

    # Print first 3 rows as a quick sanity check
    print(f"\nSample output (first 3 rows of {len(rows)} total):")
    for row in rows[:3]:
        print(f"  {row}")
