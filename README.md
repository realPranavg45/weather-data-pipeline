# Weather Data Pipeline

A production-grade **ETL (Extract, Transform, Load)** data pipeline built in Python that collects hourly weather data from the [Open-Meteo API](https://open-meteo.com/) for multiple Indian cities, cleans and validates it using pandas, stores it in a PostgreSQL database, and provides SQL-based analytics with gap detection and automatic backfill capabilities.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Database Design](#database-design)
- [Pipeline Phases](#pipeline-phases)
  - [Phase 1 — Database Setup](#phase-1--database-setup-db_setuppy)
  - [Phase 2 — Extraction](#phase-2--extraction-extractorpy)
  - [Phase 3 — Cleaning & Validation](#phase-3--cleaning--validation-cleanerpy)
  - [Phase 4 — Loading](#phase-4--loading-loaderpy)
  - [Phase 5 — Gap Detection & Backfill](#phase-5--gap-detection--backfill-gap_detectorpy)
  - [Phase 6 — SQL Analytics](#phase-6--sql-analytics)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Testing](#testing)
- [Internal Processing Flow](#internal-processing-flow)
- [Features](#features)
- [Error Handling & Resilience](#error-handling--resilience)
- [Data Quality Framework](#data-quality-framework)
- [Logging](#logging)

---

## Project Overview

This project implements a complete data engineering pipeline that:

1. **Extracts** hourly weather data (temperature, humidity, wind speed) from the Open-Meteo API for 4 Indian cities — Vadodara, Ahmedabad, Mumbai, and Delhi.
2. **Transforms** the raw API response by parsing timestamps to IST, casting data types, deduplicating records, and assigning quality flags based on validation rules.
3. **Loads** cleaned data into a PostgreSQL database using upsert logic (INSERT ... ON CONFLICT) to prevent duplicates across reruns.
4. **Detects gaps** in the hourly time series and automatically backfills missing data from the archive API.
5. **Analyzes** stored data using SQL window functions to compute daily summaries and 7-day rolling averages.

The pipeline supports two operational modes:
- **Live mode** — fetches the most recent 48 hours of forecast data.
- **Historical mode** — fetches archived data for any past date range.

---

## Architecture

```
                    ┌──────────────────────┐
                    │   Open-Meteo API     │
                    │  (Forecast/Archive)  │
                    └─────────┬────────────┘
                              │ HTTP GET (with retry)
                              ▼
              ┌───────────────────────────────┐
              │     EXTRACTOR (extractor.py)   │
              │  • Builds API query params     │
              │  • Fetches data per city       │
              │  • Retries on failure          │
              │  • Unpivots nested JSON        │
              │  • Returns list[dict]          │
              └───────────────┬───────────────┘
                              │ Raw rows (list of dicts)
                              ▼
              ┌───────────────────────────────┐
              │      CLEANER (cleaner.py)      │
              │  • Converts to pandas DF       │
              │  • Parses timestamps → IST     │
              │  • Casts values to float       │
              │  • Removes duplicates          │
              │  • Assigns quality flags       │
              │  • Splits good / rejected      │
              └───────────────┬───────────────┘
                              │ Cleaned DataFrame
                              ▼
              ┌───────────────────────────────┐
              │       LOADER (loader.py)       │
              │  • Upserts rows into PG        │
              │  • ON CONFLICT DO UPDATE       │
              │  • Handles NaN → NULL          │
              │  • Logs pipeline_runs          │
              └───────────────┬───────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │     PostgreSQL (weather DB)     │
              │  ┌───────────┐ ┌────────────┐ │
              │  │  tag_map  │ │weather_data│ │
              │  └───────────┘ └────────────┘ │
              │  ┌──────────────┐             │
              │  │pipeline_runs │             │
              │  └──────────────┘             │
              └───────────────┬───────────────┘
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
         ┌──────────────────┐  ┌─────────────────┐
         │  GAP DETECTOR    │  │  SQL ANALYTICS   │
         │(gap_detector.py) │  │ daily_summary    │
         │  • Finds missing │  │ moving_average   │
         │    hourly data   │  │                  │
         │  • Auto-backfill │  │                  │
         └──────────────────┘  └─────────────────┘
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.13 | Core pipeline logic |
| **HTTP Client** | `requests` | API calls to Open-Meteo with timeout and retry |
| **Data Processing** | `pandas` | DataFrame operations, type casting, deduplication, quality flagging |
| **Database** | PostgreSQL 15+ | Persistent storage with ACID guarantees |
| **DB Driver** | `psycopg2-binary` | Low-level PostgreSQL driver for INSERT/UPDATE/DDL operations |
| **ORM Layer** | `SQLAlchemy` | Engine wrapper required by `pandas.read_sql_query()` in pandas 2.x |
| **Testing** | `pytest` | Unit and integration testing with fixtures |
| **API** | [Open-Meteo](https://open-meteo.com/) | Free, open-source weather API (no API key required) |
| **Logging** | Python `logging` | Dual-output logging to console and file |
| **Configuration** | JSON (`config.json`) | Centralized, human-readable configuration |

### Why These Tools?

- **psycopg2 + SQLAlchemy together**: psycopg2 is used for write operations (INSERT, UPDATE, DDL) where you need raw cursor control. SQLAlchemy is used only as a thin wrapper because pandas 2.x dropped support for raw DBAPI2 connections in `pd.read_sql_query()` — it now requires a SQLAlchemy engine.
- **Open-Meteo**: Unlike most weather APIs, Open-Meteo is completely free, requires no API key, and provides both real-time and historical archive endpoints — perfect for a data pipeline project.
- **PostgreSQL over SQLite**: PostgreSQL provides true concurrent access, `INSERT ... ON CONFLICT` upserts, `SERIAL` auto-incrementing keys, and production-grade reliability. The pipeline was originally built on SQLite and later migrated to PostgreSQL.

---

## Database Design

The pipeline uses a dedicated PostgreSQL database called **`weather`** with three tables:

### 1. `tag_map` — Sensor/Tag Metadata

Each unique combination of city + weather variable gets a **tag ID** (e.g., `VAD_TEMP` = Vadodara Temperature). This follows the industrial IoT tagging convention where every data point has a unique identifier.

```sql
CREATE TABLE tag_map (
    tag_id       TEXT PRIMARY KEY,    -- e.g., 'VAD_TEMP', 'MUM_HUM'
    city         TEXT NOT NULL,       -- e.g., 'Vadodara'
    tag_prefix   TEXT NOT NULL,       -- e.g., 'VAD'
    measurement  TEXT NOT NULL,       -- e.g., 'Temperature'
    api_variable TEXT NOT NULL,       -- e.g., 'temperature_2m'
    unit         TEXT NOT NULL,       -- e.g., '°C'
    valid_min    REAL,                -- Lower bound for quality check
    valid_max    REAL                 -- Upper bound for quality check
);
```

**Current tags (4 cities x 3 variables = 12 tags):**

| Tag ID | City | Measurement | Unit | Valid Range |
|--------|------|-------------|------|-------------|
| VAD_TEMP | Vadodara | Temperature | °C | -10 to 55 |
| VAD_HUM | Vadodara | Relative Humidity | % | 0 to 100 |
| VAD_WIND | Vadodara | Wind Speed | km/h | 0 to 200 |
| AMD_TEMP | Ahmedabad | Temperature | °C | -10 to 55 |
| AMD_HUM | Ahmedabad | Relative Humidity | % | 0 to 100 |
| AMD_WIND | Ahmedabad | Wind Speed | km/h | 0 to 200 |
| MUM_TEMP | Mumbai | Temperature | °C | -10 to 55 |
| MUM_HUM | Mumbai | Relative Humidity | % | 0 to 100 |
| MUM_WIND | Mumbai | Wind Speed | km/h | 0 to 200 |
| DEL_TEMP | Delhi | Temperature | °C | -10 to 55 |
| DEL_HUM | Delhi | Relative Humidity | % | 0 to 100 |
| DEL_WIND | Delhi | Wind Speed | km/h | 0 to 200 |

### 2. `weather_data` — Time-Series Readings

Stores every hourly weather reading with its quality assessment.

```sql
CREATE TABLE weather_data (
    id           SERIAL PRIMARY KEY,
    tag_id       TEXT   NOT NULL,                     -- FK to tag_map
    timestamp    TEXT   NOT NULL,                     -- ISO-8601 with timezone
    value        REAL,                                -- Can be NULL (missing)
    quality_flag TEXT   NOT NULL DEFAULT 'good',      -- 'good', 'missing', 'out_of_range'

    UNIQUE (tag_id, timestamp),                       -- Prevents duplicate entries
    FOREIGN KEY (tag_id) REFERENCES tag_map(tag_id)   -- Enforces referential integrity
);
```

**Key design decisions:**
- `UNIQUE (tag_id, timestamp)` enables the upsert pattern — reruns overwrite existing data instead of creating duplicates.
- `timestamp` is stored as ISO-8601 TEXT (e.g., `2024-06-01T10:00:00+0530`) because it preserves timezone info explicitly.
- `value` is nullable — missing readings are stored as NULL with `quality_flag = 'missing'` rather than being discarded, so the gap detector can find them.

### 3. `pipeline_runs` — Execution Audit Log

Tracks every pipeline execution for monitoring and debugging.

```sql
CREATE TABLE pipeline_runs (
    run_id        SERIAL PRIMARY KEY,
    start_time    TEXT   NOT NULL,
    end_time      TEXT,
    mode          TEXT   NOT NULL,         -- 'live' or 'historical'
    date_from     TEXT,                    -- Start date (historical only)
    date_to       TEXT,                    -- End date (historical only)
    rows_loaded   INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    status        TEXT    DEFAULT 'running'  -- 'running', 'success', 'failed'
);
```

### Entity Relationship

```
tag_map (1) ──────< (many) weather_data
   │                        │
   │  tag_id (PK/FK)        │  tag_id + timestamp (UNIQUE)
   │                        │
   └── Defines valid        └── Stores hourly readings
       ranges per tag           with quality flags

pipeline_runs (independent)
   └── One row per pipeline execution
       Tracks rows_loaded, rows_rejected, status
```

---

## Pipeline Phases

### Phase 1 — Database Setup (`db_setup.py`)

**What it does:** Creates all three database tables and populates `tag_map` with the 12 tag definitions from `config.json`.

**How it works internally:**
1. Reads `config.json` to get database credentials and city/variable definitions.
2. Connects to PostgreSQL using `psycopg2` via the centralized `db.py` module.
3. Executes `CREATE TABLE IF NOT EXISTS` for each table — safe to rerun without errors.
4. Iterates over every `(city, variable)` combination in the config to build tag IDs.
5. Inserts each tag using `INSERT ... ON CONFLICT (tag_id) DO NOTHING` — existing tags are silently skipped on reruns.
6. Displays the final tag_map table for visual verification.

```bash
python db_setup.py
```

---

### Phase 2 — Extraction (`extractor.py`)

**What it does:** Fetches raw weather data from the Open-Meteo API for all configured cities.

**How it works internally:**

1. **Parameter Building** (`build_params()`):
   - Constructs the API URL and query parameters based on the mode.
   - **Live mode** → hits `api.open-meteo.com/v1/forecast` with `forecast_days=2` (returns ~48 hours of recent data).
   - **Historical mode** → hits `archive-api.open-meteo.com/v1/archive` with explicit `start_date` and `end_date`.
   - Requests all three variables (`temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`) in a single call per city.

2. **HTTP Fetching with Retry** (`fetch_with_retry()`):
   - Makes an HTTP GET request using the `requests` library.
   - Applies a **configurable timeout** (default 30 seconds) to prevent hanging.
   - On network errors or timeouts, **retries up to 3 times** with a 5-second delay between attempts.
   - On HTTP 4xx/5xx errors, does NOT retry (these are permanent failures).
   - Returns the parsed JSON response or `None` if all retries fail.

3. **Response Parsing** (`parse_response()`):
   - The API returns nested JSON where timestamps are in one array and values in another:
     ```json
     {"hourly": {"time": ["2024-01-01T00:00", ...], "temperature_2m": [22.1, ...], ...}}
     ```
   - This function **unpivots** the nested structure into flat dictionaries — one per `(timestamp, variable)` pair:
     ```python
     {"city": "Vadodara", "tag_id": "VAD_TEMP", "timestamp": "2024-01-01T00:00", "value": 22.1}
     ```
   - Each city + 3 variables + 24 hours = 72 rows per city per day.

4. **Aggregation**: All rows from all cities are collected into a single list and returned to the pipeline.

**Output:** A list of raw dictionaries (e.g., 576 rows for a live run across 4 cities with 2 days of data).

---

### Phase 3 — Cleaning & Validation (`cleaner.py`)

**What it does:** Transforms raw data into a validated, analysis-ready pandas DataFrame.

**How it works internally (5-step sequential pipeline):**

1. **`to_dataframe()`** — Converts the list of raw dicts into a pandas DataFrame. This enables vectorized operations that are orders of magnitude faster than Python loops for large datasets.

2. **`parse_timestamps()`** — Converts timestamp strings into timezone-aware IST datetimes:
   - `pd.to_datetime()` parses strings like `"2024-01-01T10:00"`.
   - `dt.tz_localize("Asia/Kolkata")` marks them as IST (UTC+5:30). Open-Meteo returns timestamps in the requested timezone without offset info, so we explicitly localize them.
   - Finally, formats back to ISO-8601 strings with offset (`"2024-01-01T10:00:00+0530"`) for storage.
   - Handles DST edge cases with `nonexistent="shift_forward"` (though India doesn't observe DST, this is a safety net).

3. **`cast_value_column()`** — Casts the `value` column to `float64`:
   - Uses `pd.to_numeric(errors='coerce')` which turns any unparseable values (including Python `None` from JSON null) into `NaN`.
   - Logs how many missing values were found.

4. **`remove_duplicates()`** — Drops rows where both `tag_id` and `timestamp` are identical:
   - Uses `df.drop_duplicates(subset=["tag_id", "timestamp"], keep="first")`.
   - This handles cases where the extractor might be called twice or the API returns overlapping time windows.

5. **`add_quality_flag()`** — Assigns a quality assessment to every row:
   - Joins the weather data with `tag_map` metadata (which has `valid_min` and `valid_max` per tag).
   - Applies three rules in order:
     - `'good'` — value exists and falls within `[valid_min, valid_max]`.
     - `'missing'` — value is `NaN` / `NULL`.
     - `'out_of_range'` — value exists but is outside the valid range.
   - Out-of-range values are **not discarded** — they represent real readings that might be unusual but legitimate (e.g., a heatwave causing 50°C).

**Output:** Two DataFrames — `good_df` (all rows, ready to load) and `rejected_df` (only rows flagged as `missing`, for logging purposes).

---

### Phase 4 — Loading (`loader.py`)

**What it does:** Inserts cleaned data into the `weather_data` table using PostgreSQL upserts.

**How it works internally:**

1. **Pipeline Run Tracking** (`start_run()` / `finish_run()`):
   - Before loading, inserts a row into `pipeline_runs` with `status='running'` and returns the new `run_id` using PostgreSQL's `RETURNING` clause.
   - After loading (success or failure), updates the same row with `end_time`, `rows_loaded`, `rows_rejected`, and final `status`.

2. **Upsert Logic** (`load_data()`):
   - Iterates over every row in the cleaned DataFrame.
   - Executes an upsert for each row:
     ```sql
     INSERT INTO weather_data (tag_id, timestamp, value, quality_flag)
     VALUES (%s, %s, %s, %s)
     ON CONFLICT (tag_id, timestamp) DO UPDATE SET
         value        = EXCLUDED.value,
         quality_flag = EXCLUDED.quality_flag;
     ```
   - `ON CONFLICT DO UPDATE` means: if a row with the same `(tag_id, timestamp)` already exists, overwrite its `value` and `quality_flag` with the new data. This makes the pipeline **idempotent** — running it twice with the same data is safe.
   - `EXCLUDED` refers to the row that was being inserted but conflicted — this is PostgreSQL-specific syntax.
   - Converts pandas `NaN` to Python `None` before insertion so psycopg2 stores it as SQL `NULL`.

3. **Error Isolation**: If a single row fails to insert (e.g., FK violation), the transaction for that row is rolled back but the loop continues with the remaining rows.

4. **Tag Map Loading** (`load_tag_map()`): Reads the full `tag_map` table into a pandas DataFrame using SQLAlchemy. This is used by the cleaner to look up `valid_min` and `valid_max` for quality flagging.

---

### Phase 5 — Gap Detection & Backfill (`gap_detector.py`)

**What it does:** Identifies missing hourly data points and automatically re-fetches them from the archive API.

**How it works internally:**

1. **Gap Detection** (`find_gaps()`):
   - **Builds an expected index**: generates every `(tag_id, timestamp)` pair that should exist in the given date range. For example, 1 day x 12 tags x 24 hours = 288 expected rows.
   - Uses `pd.date_range()` with hourly frequency and IST timezone to generate all expected timestamps.
   - Loads all `tag_id` values from `tag_map` to build the cartesian product.
   - **Loads the actual data** from `weather_data` for the same date range.
   - **Performs a set difference**: merges expected vs. actual on `(tag_id, timestamp)` with a LEFT join and `indicator=True`. Rows that exist only in the expected set (marked `left_only`) are gaps.

2. **Backfill** (`backfill()`):
   - Takes the gaps DataFrame and extracts the unique dates that need data.
   - Calls the **existing Extract → Clean → Load pipeline** to fetch and load the missing data from the archive API.
   - This reuses all existing code — no duplication of logic.
   - Safe to run multiple times because the loader uses upserts.

3. **Gap Report** (`print_gap_report()`): Prints a human-readable summary showing how many missing hours each tag has.

```bash
# Detect only (dry run)
python gap_detector.py --start 2024-06-01 --end 2024-06-07 --dry-run

# Detect and backfill
python gap_detector.py --start 2024-06-01 --end 2024-06-07
```

---

### Phase 6 — SQL Analytics

Two analytical SQL queries are provided in the `sql/` directory:

#### `daily_summary.sql` — Daily Aggregates

Computes the daily **min, max, and average** value for each tag, along with counts of missing and out-of-range hours.

```sql
SELECT
    tag_id,
    SUBSTR(timestamp, 1, 10)             AS date,
    ROUND(MIN(value)::NUMERIC, 2)        AS daily_min,
    ROUND(MAX(value)::NUMERIC, 2)        AS daily_max,
    ROUND(AVG(value)::NUMERIC, 2)        AS daily_avg,
    COUNT(*)                             AS total_hours,
    SUM(CASE WHEN quality_flag = 'missing'      THEN 1 ELSE 0 END) AS missing_hours,
    SUM(CASE WHEN quality_flag = 'out_of_range' THEN 1 ELSE 0 END) AS out_of_range_hours
FROM weather_data
WHERE quality_flag != 'missing'
GROUP BY tag_id, SUBSTR(timestamp, 1, 10)
ORDER BY tag_id, date;
```

> **Note:** PostgreSQL requires explicit `::NUMERIC` casts for the `ROUND(x, n)` function since it does not accept `REAL` type directly — unlike SQLite which is more permissive.

#### `moving_average.sql` — 7-Day Rolling Average

Uses a SQL **window function** to compute a 7-day rolling average of daily averages per tag.

```sql
AVG(daily_avg) OVER (
    PARTITION BY tag_id
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS avg_7day
```

**How the window function works:**
- `PARTITION BY tag_id` → resets the window for each tag independently (VAD_TEMP doesn't mix with MUM_HUM).
- `ORDER BY date` → slides the window chronologically.
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` → includes the current day plus the 6 prior days = 7-day window.
- Unlike `GROUP BY` (which collapses rows), window functions keep every individual row visible while computing the aggregate alongside it.

---

## Configuration

All settings are centralized in **`config.json`**:

```json
{
  "cities": [
    {"name": "Vadodara",  "tag_prefix": "VAD", "latitude": 22.3072, "longitude": 73.1812, "timezone": "Asia/Kolkata"},
    {"name": "Ahmedabad", "tag_prefix": "AMD", "latitude": 23.0225, "longitude": 72.5714, "timezone": "Asia/Kolkata"},
    {"name": "Mumbai",    "tag_prefix": "MUM", "latitude": 19.0760, "longitude": 72.8777, "timezone": "Asia/Kolkata"},
    {"name": "Delhi",     "tag_prefix": "DEL", "latitude": 28.6139, "longitude": 77.2090, "timezone": "Asia/Kolkata"}
  ],
  "variables": [
    {"api_name": "temperature_2m",      "tag_suffix": "TEMP", "measurement": "Temperature",       "unit": "°C",   "valid_min": -10, "valid_max": 55},
    {"api_name": "relative_humidity_2m", "tag_suffix": "HUM",  "measurement": "Relative Humidity", "unit": "%",    "valid_min": 0,   "valid_max": 100},
    {"api_name": "wind_speed_10m",      "tag_suffix": "WIND", "measurement": "Wind Speed",        "unit": "km/h", "valid_min": 0,   "valid_max": 200}
  ],
  "api": {
    "base_url": "https://api.open-meteo.com/v1/forecast",
    "historical_url": "https://archive-api.open-meteo.com/v1/archive",
    "timeout_seconds": 30,
    "max_retries": 3,
    "retry_delay_seconds": 5
  },
  "database": {
    "host": "localhost",
    "port": 5522,
    "name": "weather",
    "user": "postgres",
    "password": "Pranav2004"
  },
  "logging": {
    "file": "pipeline.log",
    "level": "INFO"
  }
}
```

**Adding a new city** is as simple as adding an entry to the `cities` array — no code changes needed. Same for adding a new weather variable to `variables`.

---

## Project Structure

```
WAETHER/
├── config.json              # Centralized configuration (cities, variables, DB, API)
├── conftest.py              # pytest root configuration for test discovery
├── requirements.txt         # Python dependencies
├── README.md                # This file
│
├── db.py                    # Database connection helpers (psycopg2 + SQLAlchemy)
├── db_setup.py              # Phase 1: Creates tables, populates tag_map
├── extractor.py             # Phase 2: Fetches data from Open-Meteo API
├── cleaner.py               # Phase 3: Cleans, validates, quality-flags data
├── loader.py                # Phase 4: Upserts data into PostgreSQL
├── gap_detector.py          # Phase 5: Detects missing data and backfills
├── run_pipeline.py          # Main entry point — orchestrates Phases 2-4
├── verify.py                # Read-only script to inspect DB contents & analytics
├── migrate_to_weather_db.py # One-time migration script (Main → weather DB)
│
├── sql/                     # Phase 6: SQL analytics queries
│   ├── daily_summary.sql    # Daily min/max/avg per tag
│   ├── moving_average.sql   # 7-day rolling average using window functions
│   └── placeholder.sql      # Placeholder file
│
├── tests/                   # pytest test suite
│   ├── __init__.py          # Package marker
│   ├── test_cleaner.py      # 14 unit tests for the cleaner module
│   ├── test_gap_detector.py # 6 integration tests for gap detection
│   └── placeholder_test.py  # Placeholder file
│
├── pipeline.log             # Pipeline execution log (auto-generated)
└── weather.db               # Legacy SQLite database (from earlier version)
```

---

## How to Run

### Prerequisites

1. **Python 3.10+** installed.
2. **PostgreSQL** running on `localhost:5522` (or update `config.json`).
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Step-by-Step

```bash
# 1. Set up the database (creates tables + tag_map)
python db_setup.py

# 2. Run a live pipeline (fetches recent 48 hours)
python run_pipeline.py live

# 3. Run a historical pipeline (fetches a specific date range)
python run_pipeline.py historical --start 2024-06-01 --end 2024-06-07

# 4. Check for gaps and backfill (dry run first)
python gap_detector.py --start 2024-06-01 --end 2024-06-07 --dry-run

# 5. Actually backfill any gaps found
python gap_detector.py --start 2024-06-01 --end 2024-06-07

# 6. Verify data in the database
python verify.py

# 7. Run the test suite
pytest tests/ -v
```

### Individual Module Testing

Each module can be run standalone for testing:

```bash
python db.py              # Test database connection
python extractor.py live  # Test extraction only
python cleaner.py         # Run cleaner with sample data
python loader.py          # Test tag_map loading and pipeline_runs
```

---

## Testing

The project includes **20 automated tests** using pytest:

### Cleaner Tests (`tests/test_cleaner.py`) — 14 tests

| Test | What it validates |
|------|-------------------|
| `test_to_dataframe_creates_correct_shape` | DataFrame has correct rows and columns |
| `test_to_dataframe_empty_input` | Empty input returns empty DataFrame (no crash) |
| `test_cast_value_column_converts_none_to_nan` | JSON `null` → pandas `NaN` |
| `test_cast_value_column_dtype_is_float` | Value column dtype is `float64` |
| `test_remove_duplicates_drops_exact_duplicate` | Same (tag_id, timestamp) → keeps only first |
| `test_remove_duplicates_keeps_different_timestamps` | Same tag, different times → both kept |
| `test_remove_duplicates_keeps_different_tags` | Same time, different tags → both kept |
| `test_quality_flag_good` | Valid value → flag is `'good'` |
| `test_quality_flag_missing` | NaN value → flag is `'missing'` |
| `test_quality_flag_out_of_range_high` | Value > valid_max → `'out_of_range'` |
| `test_quality_flag_out_of_range_low` | Value < valid_min → `'out_of_range'` |
| `test_quality_flag_boundary_values_are_good` | Exactly at min/max → `'good'` (not out_of_range) |
| `test_clean_pipeline_end_to_end` | Full clean() with valid input → non-empty result |
| `test_clean_pipeline_empty_input` | Full clean() with empty input → empty DataFrames |

### Gap Detector Tests (`tests/test_gap_detector.py`) — 6 integration tests

These tests run against the **live PostgreSQL database** using isolated year-2020 date ranges that never conflict with production data. Each test cleans up its own rows in the teardown phase.

| Test | What it validates |
|------|-------------------|
| `test_find_gaps_returns_all_missing_when_db_is_empty` | Empty DB → 24 gaps for one day |
| `test_find_gaps_returns_empty_when_all_data_present` | All 24 hours present → 0 gaps |
| `test_find_gaps_detects_specific_missing_hours` | 2 missing hours inserted → exactly 2 gaps |
| `test_find_gaps_spans_multiple_days` | 2 empty days → 48 gaps |
| `test_find_gaps_is_idempotent` | Running twice gives same result |
| `test_find_gaps_result_has_correct_columns` | Output DataFrame has `tag_id` and `timestamp` |

```bash
# Run all tests
pytest tests/ -v

# Run only cleaner tests
pytest tests/test_cleaner.py -v

# Run only gap detector tests
pytest tests/test_gap_detector.py -v
```

---

## Internal Processing Flow

Here's exactly what happens when you run `python run_pipeline.py live`:

```
1. parse_args()
   └─ Parses CLI: mode="live", start=None, end=None

2. load_config()
   └─ Reads config.json into a Python dict

3. setup_logging()
   └─ Creates logger with console + file handlers
   └─ Writes to pipeline.log

4. get_connection()
   └─ Opens psycopg2 connection to PostgreSQL (weather DB)

5. start_run()
   └─ INSERT INTO pipeline_runs → returns run_id
   └─ Status set to 'running'

6. EXTRACT — extract(config, mode="live")
   ├─ For each of 4 cities:
   │   ├─ build_params() → URL + query dict
   │   ├─ fetch_with_retry() → HTTP GET with 30s timeout
   │   │   └─ On failure: retry up to 3 times, 5s delay
   │   ├─ parse_response() → unpivot nested JSON
   │   └─ Append 144 rows (48 hours × 3 variables)
   └─ Returns 576 raw row dicts

7. CLEAN — clean(raw_rows, tag_map_df)
   ├─ to_dataframe() → list[dict] → pandas DataFrame
   ├─ parse_timestamps() → localize to IST, format ISO-8601
   ├─ cast_value_column() → pd.to_numeric(errors='coerce')
   ├─ remove_duplicates() → drop_duplicates on (tag_id, timestamp)
   ├─ add_quality_flag() → merge with tag_map, apply rules
   └─ Returns (good_df, rejected_df)

8. LOAD — load_data(conn, good_df)
   ├─ For each row in good_df:
   │   ├─ Convert NaN → None
   │   └─ INSERT ... ON CONFLICT DO UPDATE
   ├─ conn.commit()
   └─ Returns rows_loaded count

9. finish_run()
   └─ UPDATE pipeline_runs SET status='success', rows_loaded=576, ...

10. conn.close()
    └─ Pipeline complete, logged to pipeline.log
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Dual-mode operation** | Live (recent 48h) and historical (any past date range) |
| **Automatic retries** | Network errors and timeouts trigger up to 3 retries with configurable delay |
| **Idempotent loads** | Upsert logic means rerunning with the same data is safe — no duplicates |
| **Data quality flags** | Every row is classified as `good`, `missing`, or `out_of_range` |
| **Gap detection** | Automatically identifies missing hourly data points |
| **Auto-backfill** | Fetches and loads missing data from the archive API |
| **Pipeline audit log** | Every execution is recorded with status, row counts, and timestamps |
| **Config-driven** | Adding cities or variables requires zero code changes |
| **Comprehensive testing** | 20 automated tests (unit + integration) |
| **Dual logging** | Simultaneous console and file output |
| **SQL analytics** | Pre-built daily summaries and 7-day moving averages |
| **Standalone modules** | Every module can be run independently for debugging |

---

## Error Handling & Resilience

| Scenario | Handling |
|----------|----------|
| API timeout | Retries up to 3 times with 5-second delay |
| Network error (DNS, connection refused) | Retries up to 3 times with 5-second delay |
| HTTP 4xx/5xx | Logs the error, skips that city, continues with others |
| All retries exhausted | Logs error, skips the city, pipeline continues |
| Duplicate data (re-run) | `ON CONFLICT DO UPDATE` overwrites existing rows silently |
| Missing API values (null) | Stored as SQL NULL with `quality_flag='missing'` |
| Out-of-range readings | Stored with `quality_flag='out_of_range'` (not discarded) |
| Single row insert failure | Row is rolled back, remaining rows continue loading |
| Pipeline crash mid-run | `pipeline_runs` row stays as `status='running'` for detection |
| Empty API response | Logged as warning, returns empty DataFrame |

---

## Data Quality Framework

The pipeline implements a three-tier quality classification:

```
                   ┌─────────────────────┐
                   │   Incoming Value     │
                   └──────────┬──────────┘
                              │
                   ┌──────────▼──────────┐
                   │   Is value NULL?     │
                   └──────────┬──────────┘
                        Yes   │   No
                   ┌──────────┤──────────┐
                   ▼          │          ▼
            ┌──────────┐     │   ┌──────────────┐
            │ 'missing' │     │   │ Within range? │
            └──────────┘     │   └──────┬───────┘
                              │    Yes   │   No
                              │   ┌──────┤───────┐
                              │   ▼      │       ▼
                              │ ┌──────┐ │ ┌──────────────┐
                              │ │'good'│ │ │'out_of_range'│
                              │ └──────┘ │ └──────────────┘
                              │          │
                              └──────────┘
```

**Why keep out-of-range data?** In meteorology and industrial monitoring, unusual readings can be real events (heatwaves, sensor spikes). Discarding them loses information. Instead, we flag them and let downstream analysts decide whether to include them in their analysis.

---

## Logging

All pipeline activity is logged to `pipeline.log` with the format:

```
2026-11-18 12:32:40,101  INFO      [extractor]  Starting extraction — mode=live
2026-11-18 12:32:40,103  INFO      [extractor]  Fetching data for Vadodara (22.3072, 73.1812)...
2026-11-18 12:32:40,103  INFO      [extractor]    Attempt 1/3 → GET https://api.open-meteo.com/v1/forecast
2026-11-18 12:32:41,273  INFO      [extractor]    ✓ HTTP 200 received.
2026-11-18 12:32:41,278  INFO      [extractor]    Parsed 144 raw rows for Vadodara.
```

The logger writes to **two destinations simultaneously**:
1. **Console** (stdout) — for real-time monitoring during execution.
2. **Log file** (`pipeline.log`) — for post-run analysis and debugging. The file uses append mode, so logs from all runs accumulate.

---

## License

This project is for educational and portfolio purposes. The Open-Meteo API is free and open-source under the [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/).
