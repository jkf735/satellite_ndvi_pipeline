SELECT COUNT(*) AS null_keys
FROM analytics.fact_ndvi
WHERE park_code IS NULL
   OR date_key IS NULL;