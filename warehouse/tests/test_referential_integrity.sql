SELECT COUNT(*) AS orphan_rows
FROM analytics.fact_ndvi f
LEFT JOIN analytics.dim_park d
    ON f.park_code = d.park_code
WHERE d.park_code IS NULL;