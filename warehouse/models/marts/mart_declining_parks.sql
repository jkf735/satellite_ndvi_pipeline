CREATE OR REPLACE VIEW marts.mart_declining_parks AS
WITH ordered AS (
    SELECT
        f.park_code,
        f.mean_ndvi,
        ROW_NUMBER() OVER (
            PARTITION BY f.park_code
            ORDER BY d.date_key
        ) AS month_index
    FROM analytics.fact_ndvi f
    JOIN analytics.dim_date d
      ON f.date_key = d.date_key
)
SELECT
    park_code,
    REGR_SLOPE(mean_ndvi, month_index) AS ndvi_slope,
    CASE
        WHEN ndvi_slope < -0.001 THEN 'declining'
        WHEN ndvi_slope >  0.001 THEN 'improving'
    ELSE 'stable'
END AS trend_label
FROM ordered
GROUP BY park_code;