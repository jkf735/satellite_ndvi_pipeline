CREATE OR REPLACE VIEW marts.mart_ndvi_anomalies AS
SELECT
    park_code,
    date_key,
    mean_ndvi,
    (mean_ndvi - AVG(mean_ndvi) OVER (PARTITION BY park_code))
        / STDDEV(mean_ndvi) OVER (PARTITION BY park_code)
        AS z_score
FROM analytics.fact_ndvi;