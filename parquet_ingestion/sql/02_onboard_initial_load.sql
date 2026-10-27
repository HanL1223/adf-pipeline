-- README.md sections 4 (step 3) and 5. Read-only check of a table's EXTRACT_META rows. Changes nothing.
-- The setup DML runs in the pipeline as the ADF role (it holds CONTROL_DEV_RW):
--   test prime  -> 'Prepare row' creates the <table>_PRIME_TEST row automatically;
--   live prime  -> 'Prepare row' fixes or creates the live INITIAL_LOAD row only with p_Prepare_Live_Row = true.
-- CONTROL text columns carry a collation: COLLATE(col, '') strips it for TRIM with characters.
USE ROLE OPS_DEV_RO;
USE DATABASE EDP_DEV;

SET stage_table = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY';   -- the live STAGE_TABLE_NAME

-- ===================================================================================================== A. The table's rows
-- Expect exactly 1 TABLE row (the delta twin, e.g. 156), 0 or 1 FILE row (the live INITIAL_LOAD row, e.g. 768) and,
-- after a test prime, 1 FILE row named <stage_table>_PRIME_TEST. Save this output before a test as the baseline.
SELECT EXTRACT_META_ID, SOURCE_NAME, SOURCE_TYPE, STAGE_TABLE_NAME, OBJECT_NAME, TARGET_SCHEMA, ACTIVE_FLAG,
       RUN_GROUP, PRIMARY_KEYS, SQL_STATEMENT, SOURCE_PATH, STAGE_FILE_TYPE, PARALLEL_STAGE_LOAD_FLAG,
       SCHEMA_DRIFT_CODE, SQL_SNIPPET
FROM CONTROL.EXTRACT_META
WHERE UPPER(STAGE_TABLE_NAME) IN (UPPER($stage_table), UPPER($stage_table) || '_PRIME_TEST')
ORDER BY STAGE_TABLE_NAME, SOURCE_TYPE, EXTRACT_META_ID;

-- The delta twin must be a SQL Server TABLE row whose SOURCE_NAME is a SourceConfig key (ADF global parameter).
-- Its SQL_SNIPPET should be the BRONZE-MAX watermark pattern (Altida_Skill.md recipe A2). Note the watermark
-- column: use it as window_column when you prime.

-- ===================================================================================================== B. What 'Prepare row' would do (live)
-- LIVE_ROW_READY         : nothing to change.
-- LIVE_ROW_NEEDS_PREPARE : the live prime stops unless p_Prepare_Live_Row = true; then exactly this row changes
--                          (SQL_STATEMENT -> NULL, RUN_GROUP -> DATA_PRIME).
-- JSON build (PL_ATILDA_DATA_PRIME_JSON) only:
-- LIVE_ROW_NEEDS_CONVERT : a Parquet row. With p_Prepare_Live_Row = true the JSON prime converts it in place
--                          (OBJECT_NAME <t>_part_%.json, STAGE_FILE_TYPE/STAGE_FILE_SPEC NULL, SCHEMA_DRIFT_CODE from
--                          the delta twin, plus the two fixes above). Save this output first: it is the backup.
SELECT EXTRACT_META_ID, OBJECT_NAME, STAGE_FILE_TYPE, STAGE_FILE_SPEC, SCHEMA_DRIFT_CODE, SQL_STATEMENT,
       PARALLEL_STAGE_LOAD_FLAG, RUN_GROUP, ACTIVE_FLAG, SOURCE_PATH,
       CASE WHEN UPPER(STAGE_FILE_TYPE) = 'PARQUET' THEN 'LIVE_ROW_NEEDS_CONVERT (JSON build)'
            WHEN SQL_STATEMENT IS NULL AND UPPER(COALESCE(COLLATE(RUN_GROUP, ''), '')) = 'DATA_PRIME' THEN 'LIVE_ROW_READY'
            ELSE 'LIVE_ROW_NEEDS_PREPARE' END AS PREPARE
FROM CONTROL.EXTRACT_META
WHERE UPPER(STAGE_TABLE_NAME) = UPPER($stage_table) AND SOURCE_TYPE = 'FILE'
  AND ENDSWITH(UPPER(SOURCE_NAME), 'INITIAL_LOAD');

-- ===================================================================================================== C. SOURCE_PATH must be unique
SELECT LOWER(TRIM(COLLATE(SOURCE_PATH, ''), '/ ')) AS SOURCE_PATH, COUNT(*) AS ROWS_USING_IT
FROM CONTROL.EXTRACT_META
WHERE UPPER(STAGE_TABLE_NAME) IN (UPPER($stage_table), UPPER($stage_table) || '_PRIME_TEST') AND SOURCE_TYPE = 'FILE'
GROUP BY 1;   -- expect 1 per path (compared case-insensitively, as Check pair does)
