CREATE TABLE IF NOT EXISTS park_ndvi_stats (
                park_name TEXT,
                park_code TEXT,
                date DATE,
                year INT,
                month INT,
                mean_ndvi DOUBLE PRECISION,
                std_ndvi DOUBLE PRECISION,
                min_ndvi DOUBLE PRECISION,
                max_ndvi DOUBLE PRECISION,
                valid_pixels BIGINT,
                source_raster TEXT UNIQUE
            );