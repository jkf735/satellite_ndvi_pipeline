SELECT COUNT(*) AS orphan_rows
FROM fact_ndvi f
LEFT JOIN dim_park d
    ON f.park_code = d.park_code
WHERE d.park_code IS NULL;