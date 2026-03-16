CREATE OR REPLACE VIEW marts.mart_ndvi_anomalies AS
SELECT
    park_code,
    f.date_key,
    mean_ndvi,
    (mean_ndvi - AVG(mean_ndvi) OVER (PARTITION BY park_code, d.month))
        / NULLIF(STDDEV(mean_ndvi) OVER (PARTITION BY park_code, d.month), 0)
        AS z_score
FROM analytics.fact_ndvi f
JOIN analytics.dim_date d ON f.date_key = d.date_key;