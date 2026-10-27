-- README steps 7-10. Monitoring for data priming. Read-only.
-- The run's own record is the ADF run (Monitor): 'Get source columns' output has WINDOW_FILTER, 'Data in' returns
-- batch_rows, 'End prime' returns the reconcile row.
USE ROLE OPS_DEV_RO;   -- for a test prime, the test BRONZE table is owned by the ingestion role: use a role that can
                        -- read it (for example DEV_ACCOUNT_ROLE_DATA_INGESTION) for section B
USE DATABASE EDP_DEV;

SET stage_table = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY';   -- for a test prime: the *_PRIME_TEST name
SET run_id      = '<RunId>';   -- the run's Trigger time in ADF Monitor, in UTC, as yyyyMMddHHmmss (e.g. 20261026031500)
SET bronze_tbl  = 'EDP_DEV.BRONZE.' || $stage_table;
SET wm_col      = 'REFRESHDATETIME';   -- delta watermark column, UPPERCASE as DLA stores it

-- ===================================================================================================== A. State of the pair
-- After a passed prime: initial row ACTIVE = FALSE, delta twin ACTIVE = TRUE (unless p_Enable_Delta = false).
-- Initial row TRUE = a prime is running, or one was cancelled (re-run with p_Force = true).
-- Delta twin FALSE after a failed LIVE prime = intended; it stays off until a prime passes.
-- Test prime: with the *_PRIME_TEST name this shows only the test row; run it again with the live name to confirm
-- the delta twin and the live initial row did not change. The handover query in B is for live primes only.
SELECT EXTRACT_META_ID, SOURCE_NAME, SOURCE_TYPE, ACTIVE_FLAG, RUN_GROUP
FROM CONTROL.EXTRACT_META WHERE UPPER(STAGE_TABLE_NAME) = UPPER($stage_table) ORDER BY SOURCE_TYPE;

-- ===================================================================================================== B. One run in detail
-- ATILDA plans of this run (one per file); all COMPLETED after a pass.
SELECT s.*
FROM CONTROL.EXTRACT_RUN_PLAN_STATUS_VW s
JOIN CONTROL.EXTRACT_META i ON i.EXTRACT_META_ID = s.EXTRACT_META_ID
WHERE UPPER(i.STAGE_TABLE_NAME) = UPPER($stage_table) AND i.SOURCE_TYPE = 'FILE'
  AND CONTAINS(s.FILE_NAME, '_part_' || $run_id || '_')
ORDER BY s.FILE_NAME;

-- BRONZE rows per extract batch (compare with 'Data in' batch_rows in ADF Monitor).
SELECT REGEXP_SUBSTR(COLLATE(ETL_FILE_NAME, ''), '_part_' || $run_id || '_b([0-9]+)_', 1, 1, 'e', 1) AS BATCH_NO, COUNT(*) AS ROWS_LOADED
FROM IDENTIFIER($bronze_tbl)
WHERE CONTAINS(ETL_FILE_NAME, '_part_' || $run_id || '_')
GROUP BY 1 ORDER BY 1;

-- Handover: the delta continues from this value; it must be just below the end bound in WINDOW_FILTER.
SELECT MAX(TRY_TO_TIMESTAMP(IDENTIFIER($wm_col)::VARCHAR)) AS MAX_WATERMARK, COUNT(*) AS ROWS_IN_BRONZE
FROM IDENTIFIER($bronze_tbl);

-- ===================================================================================================== C. Recovery
-- There is no manual DML: people do not write CONTROL. Recover by re-running the prime:
--   stale lock after a cancel      -> re-run that table alone with p_Force = true;
--   delta twin left OFF            -> fix the cause, re-run the live prime (it turns the delta ON on a pass).
--   delta twin OFF after a PASSED prime (p_Enable_Delta false, or watermark unusable): to turn it on without a
--   re-prime, a holder of EDP_DEV.CONTROL_DEV_RW (e.g. DEV_ROLE_DATA_ENGINEER) sets ACTIVE_FLAG = TRUE, with sign-off.
