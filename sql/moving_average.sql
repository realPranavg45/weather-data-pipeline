-- moving_average.sql
-- -------------------
-- Compute a 7-day rolling average of daily averages per tag.
-- Uses a SQL window function (no Spark, no pandas needed here!).
--
-- What is a window function?
--   Unlike GROUP BY (which collapses rows), a window function computes
--   an aggregate OVER a sliding "window" of rows, keeping each row visible.
--   OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...) defines the window.
--
-- How to read the output:
--   Each row = one tag on one day.
--   'avg_7day' is the average of that day's daily_avg plus the 6 preceding days.
--   The first 6 days of data for each tag will show averages over fewer days
--   because there isn't enough history yet.
--
-- How to run (Python):
--   with open("sql/moving_average.sql") as f:
--       result = pd.read_sql_query(f.read(), conn)

WITH

-- Step 1: Compute daily averages (same logic as daily_summary.sql)
-- PostgreSQL requires NUMERIC type for ROUND(x, n); REAL must be cast.
daily_avgs AS (
    SELECT
        tag_id,
        SUBSTR(timestamp, 1, 10) AS date,
        ROUND(AVG(value)::NUMERIC, 4)     AS daily_avg

    FROM
        weather_data

    WHERE
        quality_flag != 'missing'

    GROUP BY
        tag_id,
        SUBSTR(timestamp, 1, 10)
)

-- Step 2: Apply the 7-day rolling window on top of the daily averages
SELECT
    tag_id,
    date,
    ROUND(daily_avg, 2) AS daily_avg,

    -- Window function:
    --   PARTITION BY tag_id  → reset the window for each tag independently
    --   ORDER BY date        → slide the window chronologically
    --   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW → look back 6 rows + current = 7 days
    ROUND(
        AVG(daily_avg) OVER (
            PARTITION BY tag_id
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS avg_7day

FROM
    daily_avgs

ORDER BY
    tag_id,
    date;
