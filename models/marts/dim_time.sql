WITH time_series AS (
  SELECT generate_series(
    '2023-01-01 00:00:00'::timestamp,
    '2023-01-01 23:59:00'::timestamp,
    interval '1 minute'
  ) AS ts
)

SELECT
  ROW_NUMBER() OVER () AS time_id,
  EXTRACT(hour FROM ts) AS hour,
  EXTRACT(minute FROM ts) AS minute,
  CAST(ts AS time) AS time_val
FROM time_series
