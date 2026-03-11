-- =========================================
-- RESET VALIDATION TABLES
-- =========================================

DROP TABLE IF EXISTS parks_validated CASCADE;
DROP TABLE IF EXISTS parks_qa_failures CASCADE;
DROP TABLE IF EXISTS parks_repaired CASCADE;

CREATE TABLE parks_repaired (
    object_id INTEGER,
    park_code TEXT,
    park_name TEXT,
    park_type TEXT,
    original_geom GEOMETRY(MultiPolygon, 4326),
    was_valid BOOLEAN,
    repaired_geom GEOMETRY(MultiPolygon, 4326),
    repair_success BOOLEAN
);

CREATE TABLE parks_qa_failures (
    object_id INTEGER,
    park_code TEXT,
    park_name TEXT,
    geom GEOMETRY(MULTIPOLYGON, 4326),
    reason TEXT
);

CREATE TABLE parks_validated (
    id SERIAL PRIMARY KEY,
    park_code TEXT,
    park_name TEXT,
    park_type TEXT,
    geom GEOMETRY(MultiPolygon, 4326)
);

-- =========================================
-- ATTEMPT GEOMETRY REPAIR
-- =========================================
INSERT INTO parks_repaired (
    object_id,
    park_code,
    park_name,
    park_type,
    original_geom,
    was_valid,
    repaired_geom,
    repair_success
)
WITH repair_attempt AS (
    SELECT
        objectid AS object_id,
        unit_code AS park_code,
        unit_name AS park_name,
        unit_type AS park_type,
        wkb_geometry AS original_geom,
        ST_IsValid(wkb_geometry) AS was_valid,
        ST_CollectionExtract(ST_MakeValid(wkb_geometry), 3)::geometry(MultiPolygon, 4326) AS repaired_geom
    FROM parks_raw
)
SELECT
    object_id,
    park_code,
    park_name,
    park_type,
    original_geom,
    was_valid,
    repaired_geom,
    ST_IsValid(repaired_geom) AS repair_success
FROM repair_attempt;

-- =========================================
-- QA CHECKS
-- =========================================

-- 1. Invalid geometries
INSERT INTO parks_qa_failures (object_id, park_code, park_name, geom, reason)
SELECT
    r.object_id,
    r.park_code,
    r.park_name,
    r.original_geom,
    'INVALID GEOMETRY' AS reason
FROM parks_repaired r
WHERE r.repair_success = FALSE
  AND r.object_id NOT IN (
      SELECT object_id FROM parks_qa_failures
  );

-- 2. Null geometries and CRS missmatch
INSERT INTO parks_qa_failures (object_id, park_code, park_name, geom, reason)
SELECT
    r.object_id,
    r.park_code,
    r.park_name,
    r.repaired_geom,
    CASE
        WHEN r.repaired_geom IS NULL THEN 'NULL_GEOMETRY'
        WHEN ST_SRID(r.repaired_geom) != 4326 THEN 'CRS_MISMATCH'
        ELSE NULL
    END AS reason
FROM parks_repaired r
WHERE r.repair_success = TRUE
  AND (
        r.repaired_geom IS NULL
     OR ST_SRID(r.repaired_geom) != 4326
  )
  AND r.object_id NOT IN (
      SELECT object_id FROM parks_qa_failures
  );

-- 3. Duplicate geometries
INSERT INTO parks_qa_failures (object_id, park_code, park_name, geom, reason)
SELECT DISTINCT r1.object_id, r1.park_code, r1.park_name, r1.repaired_geom, 'DUPLICATE_GEOMETRY'
FROM parks_repaired r1
JOIN parks_repaired r2
  ON r1.object_id < r2.object_id
 AND ST_Equals(r1.repaired_geom, r2.repaired_geom);


-- =========================================
-- INSERT VALID RECORDS
-- =========================================

INSERT INTO parks_validated (park_code, park_name, park_type, geom)
SELECT park_code, park_name, park_type, repaired_geom
FROM parks_repaired
WHERE object_id NOT IN (
    SELECT object_id FROM parks_qa_failures
);

-- =========================================
-- ADD SPATIAL INDEX
-- =========================================

CREATE INDEX parks_geom_idx
ON parks_validated
USING GIST (geom);

-- =========================================
-- QA Metrics Summary
-- =========================================

-- Total rows in raw
SELECT 'TOTAL_RAW', COUNT(*) AS count FROM parks_raw;

-- Total rows repaired
SELECT 'REPAIRED', COUNT(*) AS count FROM parks_repaired WHERE was_valid = FALSE;

-- Total rows successfully validated
SELECT 'VALIDATED', COUNT(*) AS count FROM parks_validated;

-- Total rows failed QA
SELECT 'QA_FAILURES', COUNT(*) AS count FROM parks_qa_failures;