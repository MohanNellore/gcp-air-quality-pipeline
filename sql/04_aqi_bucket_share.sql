-- Share of days in each AQI category per city (window function over an aggregate)
SELECT
  city,
  aqi_bucket,
  COUNT(*) AS days,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY city), 1) AS pct_of_days
FROM {table}
WHERE aqi_bucket IS NOT NULL AND aqi_bucket <> ''
GROUP BY city, aqi_bucket
ORDER BY city, days DESC
