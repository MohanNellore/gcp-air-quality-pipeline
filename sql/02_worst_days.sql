-- The 10 most polluted city-days in the whole dataset
SELECT
  city,
  reading_date,
  pm25,
  aqi,
  aqi_bucket
FROM {table}
WHERE pm25 IS NOT NULL
ORDER BY pm25 DESC
LIMIT 10
