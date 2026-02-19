CREATE TABLE IF NOT EXISTS parks_raw (
    id SERIAL PRIMARY KEY,
    name TEXT,
    source TEXT,
    geom GEOMETRY(MultiPolygon, 4326),
    ingest_ts TIMESTAMP DEFAULT now()
);