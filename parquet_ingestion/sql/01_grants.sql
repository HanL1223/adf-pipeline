-- README.md section 3.3. Grants the ADF Snowflake role needs for data priming. No objects are created: the pipeline's
-- control SQL runs inline (tools/sf_*.sql) as the ADF service user's role.
-- Run the SHOW lines first (read-only). Then an admin runs only the GRANTs that are missing.
-- DEV_ACCOUNT_ROLE_DATA_INGESTION = role of linked service cw_au_edp_snowflake_wh (dataset DS_Snowflake_Wh), which the
-- pipelines use. DS_Snowflake (cw_au_edp_snowflake) runs as OPS_DEV and cannot write CONTROL.

-- ===================================================================================================== A. Check
-- In EDP_DEV, CONTROL writes go through database role EDP_DEV.CONTROL_DEV_RW (INSERT, UPDATE, DELETE, SELECT on
-- EXTRACT_META), granted to database role EDP_DEV.DEV_ROLE_DATA_INGESTION. The ADF role must hold that database role:
SHOW GRANTS OF DATABASE ROLE EDP_DEV.DEV_ROLE_DATA_INGESTION;   -- expect ROLE DEV_ACCOUNT_ROLE_DATA_INGESTION
SHOW GRANTS OF DATABASE ROLE EDP_DEV.CONTROL_DEV_RW;            -- expect DATABASE_ROLE DEV_ROLE_DATA_INGESTION
SHOW GRANTS TO DATABASE ROLE EDP_DEV.CONTROL_DEV_RW;            -- what it covers: EXTRACT_META, LOAD_META, views, procs
--   The ADF role needs, through those database roles:
--     SELECT, INSERT, UPDATE on CONTROL.EXTRACT_META          Prepare row (test row / live row fix), lock flags
--     DELETE on CONTROL.EXTRACT_META                          Prepare row dedupe (concurrent first runs) and the cleanup
--     DELETE on CONTROL.LOAD_META                             only PL_ATILDA_DATA_PRIME_CLEANUP_TEST with p_Delete_Test_Row
--     SELECT on EXTRACT_RUN_PLAN_STATUS_VW, EXTRACT_RUN_PLAN_VW
--     USAGE on PR_LOG_EXTRACT_RUN_PLAN, PR_CREATE_EXTRACT_RUN_PLAN_FILE, PR_CREATE_EXTRACT_ARTIFACTS (house pipelines use them)
--     SELECT, TRUNCATE on the live BRONZE tables you prime; the test table is created (and owned) by the role itself.
SHOW GRANTS TO ROLE DEV_ACCOUNT_ROLE_DATA_INGESTION;
SHOW FUTURE GRANTS IN SCHEMA EDP_DEV.BRONZE;
SHOW FUTURE GRANTS IN DATABASE EDP_DEV;

-- ===================================================================================================== B. If something is missing (admin, sign-off)
-- Prefer the existing database-role model: grant the missing privilege to the database role, e.g.
-- GRANT DATABASE ROLE EDP_DEV.DEV_ROLE_DATA_INGESTION TO ROLE DEV_ACCOUNT_ROLE_DATA_INGESTION;
-- GRANT SELECT, TRUNCATE ON TABLE EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY TO DATABASE ROLE EDP_DEV.<bronze rw db role>;
-- If the first test shows BRONZE.<table>_PRIME_TEST owned by another role (the DLA procs run with owner's rights),
-- the ADF role cannot re-test or clean up. Then, e.g.:
-- GRANT SELECT, TRUNCATE ON TABLE EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST TO ROLE DEV_ACCOUNT_ROLE_DATA_INGESTION;
--   and for the cleanup's DROP the owner drops it, or transfers OWNERSHIP to the ADF role (admin decision).
-- Do not grant CONTROL writes to OPS_DEV or ACCOUNTADMIN: people never write CONTROL by hand; the pipeline does.
-- Future grants: a schema-level future grant makes Snowflake IGNORE database-level future grants for that schema.
-- Check SHOW FUTURE GRANTS IN DATABASE EDP_DEV first; DLA-created BRONZE tables are owned by the ingestion role anyway.
