-- Average pollution by city and season (which season is worst where?)
SELECT
  city,
  season,
  ROUND(AVG(pm25), 2) AS avg_pm25,
  ROUND(AVG(pm10), 2) AS avg_pm10,
  COUNT(*) AS readings
FROM {table}
GROUP BY city, season
ORDER BY city, avg_pm25 DESC
