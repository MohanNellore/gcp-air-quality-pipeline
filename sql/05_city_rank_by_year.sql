-- Rank cities by yearly average PM2.5 (1 = most polluted) using a CTE and RANK()
WITH yearly AS (
  SELECT
    city,
    reading_year,
    AVG(pm25) AS avg_pm25
  FROM {table}
  WHERE pm25 IS NOT NULL
  GROUP BY city, reading_year
)
SELECT
  city,
  reading_year,
  ROUND(avg_pm25, 2) AS avg_pm25,
  RANK() OVER (PARTITION BY reading_year ORDER BY avg_pm25 DESC) AS pollution_rank
FROM yearly
ORDER BY reading_year, pollution_rank
