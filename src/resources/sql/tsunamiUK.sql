#standardSQL
SELECT
  year
FROM `bigquery-public-data.noaa_tsunami.historical_source_event`
WHERE country = 'UK'
ORDER BY country desc
LIMIT 10