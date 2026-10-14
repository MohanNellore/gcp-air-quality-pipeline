-- Monthly average PM2.5 per city (shows pollution trend over time)
SELECT
  city,
  reading_year,
  reading_month,
  ROUND(AVG(pm25), 2) AS avg_pm25,
  COUNT(*) AS readings
FROM {table}
WHERE pm25 IS NOT NULL
GROUP BY city, reading_year, reading_month
ORDER BY city, reading_year, reading_month
