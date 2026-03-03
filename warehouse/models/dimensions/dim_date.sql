CREATE OR REPLACE TABLE analytics.dim_date AS
SELECT
    date::DATE AS date_key,
    EXTRACT(year FROM date) AS year,
    EXTRACT(month FROM date) AS month
FROM (
    SELECT DISTINCT date FROM raw.stg_ndvi
);