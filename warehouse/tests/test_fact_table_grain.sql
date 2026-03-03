SELECT park_code, date_key, COUNT(*)
FROM analytics.fact_ndvi
GROUP BY park_code, date_key
HAVING COUNT(*) > 1;