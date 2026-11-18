-- daily_summary.sql
-- ------------------
-- For each tag, compute the daily minimum, maximum, and average value.
-- Run from Python via pandas or any PostgreSQL client.
--
-- Why is this useful?
--   Raw hourly data is noisy. Daily aggregates reveal trends:
--   Was Mumbai hotter than Delhi on average last week?
--   Which day had the highest wind speed in Vadodara?
--
-- How to run (Python):
--   with open("sql/daily_summary.sql") as f:
--       result = pd.read_sql_query(f.read(), conn)

SELECT
    tag_id,

    -- Extract only the date portion from the ISO-8601 timestamp
    -- e.g. '2024-01-01T10:00:00+05:30' → '2024-01-01'
    SUBSTR(timestamp, 1, 10)      AS date,

    -- Only include 'good' and 'out_of_range' in aggregates
    -- (excluding 'missing' which have NULL values anyway)
    -- PostgreSQL requires NUMERIC type for ROUND(x, n); REAL must be cast.
    ROUND(MIN(value)::NUMERIC, 2)          AS daily_min,
    ROUND(MAX(value)::NUMERIC, 2)          AS daily_max,
    ROUND(AVG(value)::NUMERIC, 2)          AS daily_avg,

    COUNT(*)                      AS total_hours,
    SUM(CASE WHEN quality_flag = 'missing'      THEN 1 ELSE 0 END) AS missing_hours,
    SUM(CASE WHEN quality_flag = 'out_of_range' THEN 1 ELSE 0 END) AS out_of_range_hours

FROM
    weather_data

WHERE
    -- Exclude rows where value is NULL from min/max/avg
    -- (PostgreSQL's MIN/AVG ignore NULLs by default, but being explicit is clearer)
    quality_flag != 'missing'

GROUP BY
    tag_id,
    SUBSTR(timestamp, 1, 10)   -- group by date string

ORDER BY
    tag_id,
    date;
