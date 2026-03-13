CREATE OR REPLACE VIEW marts.mart_ndvi_seasonality AS
SELECT
    park_code,
    month,
    AVG(mean_ndvi) AS seasonal_avg_ndvi
FROM analytics.fact_ndvi f
JOIN analytics.dim_date d
  ON f.date_key = d.date_key
GROUP BY park_code, month;