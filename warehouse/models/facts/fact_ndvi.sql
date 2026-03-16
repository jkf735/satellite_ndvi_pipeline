CREATE OR REPLACE TABLE analytics.fact_ndvi AS
SELECT
    park_code,
    date AS date_key,
    mean_ndvi,
    std_ndvi,
    source_raster
FROM raw.stg_ndvi;