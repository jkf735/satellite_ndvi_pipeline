CREATE OR REPLACE VIEW marts.mart_ndvi_trend AS
SELECT
    park_code,
    date_key,
    mean_ndvi,
    AVG(mean_ndvi) OVER (
        PARTITION BY park_code
        ORDER BY date_key
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_6mo_avg
FROM analytics.fact_ndvi;