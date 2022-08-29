#standardSQL
SELECT
  year, country, (maximum_water_height) AS tsunami_height
FROM `bigquery-public-data.noaa_tsunami.historical_source_event`
WHERE year < 1000
ORDER BY tsunami_height ASC
LIMIT 10