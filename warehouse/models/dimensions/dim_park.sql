CREATE OR REPLACE TABLE analytics.dim_park AS
SELECT
    park_code,
    unit_name
FROM raw.stg_parks;