CREATE OR REPLACE VIEW marts.mart_ndvi_monthly AS
SELECT
    f.park_code,
    d.year,
    d.month,
    AVG(f.mean_ndvi) AS avg_ndvi
FROM analytics.fact_ndvi f
JOIN analytics.dim_date d
    ON f.date_key = d.date_key
GROUP BY
    f.park_code,
    d.year,
    d.month;