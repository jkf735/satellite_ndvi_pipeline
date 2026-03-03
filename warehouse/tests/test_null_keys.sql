SELECT COUNT(*) AS null_keys
FROM fact_ndvi
WHERE park_code IS NULL
   OR date_key IS NULL;