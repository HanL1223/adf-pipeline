EXECUTE IMMEDIATE $$
-- The initial row stages JSON (STAGE_FILE_TYPE and STAGE_FILE_SPEC NULL, as RDBMS rows do), OBJECT_NAME is
-- <table>_part_%.json, and SCHEMA_DRIFT_CODE is copied from the delta twin (2): JSON has no schema, so columns absent from
-- the first file (all NULL there) are added later instead of being dropped. Live mode can also convert a Parquet
-- INITIAL_LOAD row left by the retired Parquet build (opt-in, below).
-- SOURCE_PATH is always the lowercase table name + '/': the row's own folder in the prime's landing container
-- (p_Landing_Container, default json-priming). Only ADF reads it; Snowflake reads the staged copy in snowflake-data.
-- 'Prepare row' Lookup: the setup DML, run as the ADF role (which holds CONTROL_DEV_RW), before 'Check pair'.
--   Test  (source_from set, table = <source_from>_PRIME_TEST): create the test INITIAL_LOAD row from the live delta
--         twin if it does not exist yet, or move an existing inactive test row to the SOURCE_PATH convention.
--   Live  (no source_from): the live INITIAL_LOAD row must have SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME and the
--         SOURCE_PATH convention. With p_Prepare_Live_Row = true, fix that one row (or create it if the table has
--         none); the old values are returned in OLD_VALUES (the ADF run output is the backup). Otherwise stop and
--         say exactly what would change.
--         If the table has no JSON initial row but exactly one Parquet INITIAL_LOAD row (an earlier Parquet onboarding),
--         p_Prepare_Live_Row = true converts that row in place to JSON (OBJECT_NAME <t>_part_%.json, STAGE_FILE_TYPE and
--         STAGE_FILE_SPEC NULL, SCHEMA_DRIFT_CODE from the delta twin) plus the fixes above. Its id and SOURCE_NAME
--         stay. OLD_VALUES holds every changed column, so the row can be put back by hand.
--   Anything else: no change; 'Check pair' reports the problem.
-- p_Force (a row left ACTIVE by a cancelled run) lets Prepare change that row anyway, as it lets Begin take its lock.
-- CONTROL text columns carry a collation: COLLATE(col, '') before TRIM with characters and SPLIT_PART.
-- Tokens {{...}} are replaced by tools/build_pipelines.py.
DECLARE
    v_status  VARCHAR DEFAULT 'OK';
    msg       VARCHAR DEFAULT '';
    v_action  VARCHAR DEFAULT 'NONE';
    v_table   VARCHAR DEFAULT UPPER(TRIM('{{TABLE}}'));
    v_src     VARCHAR DEFAULT UPPER(TRIM('{{SOURCE_FROM}}'));
    v_path    VARCHAR DEFAULT LOWER(TRIM('{{TABLE}}')) || '/';   -- SOURCE_PATH convention
    n_delta   NUMBER DEFAULT 0;
    n_init    NUMBER DEFAULT 0;
    n_name    NUMBER DEFAULT 0;
    n_path    NUMBER DEFAULT 0;
    v_id      NUMBER;
    v_active  BOOLEAN;
    v_needs   BOOLEAN;
    v_old     VARCHAR;
    n_dup     NUMBER DEFAULT 0;
    n_off     NUMBER DEFAULT 0;
    n_pq      NUMBER DEFAULT 0;
    res       RESULTSET;
BEGIN
    -- Cheap input checks first, so a bad parameter never leaves setup DML behind.
    IF (TRY_TO_BOOLEAN('{{ENABLE_DELTA}}') IS NULL) THEN
        v_status := 'ERROR';
        msg := 'enable_delta / p_Enable_Delta must be true or false, got {{ENABLE_DELTA}}. Nothing changed.';
    ELSEIF ({{ROWS_PER_FILE}} < 1000) THEN
        v_status := 'ERROR';
        msg := 'p_Rows_Per_File must be at least 1000, got {{ROWS_PER_FILE}}. Nothing changed.';
    ELSEIF (v_src <> '' AND v_table = v_src || '_PRIME_TEST') THEN
        -- ---------------------------------------------------------------- test target
        SELECT COUNT(*) INTO :n_name FROM EDP_DEV.CONTROL.EXTRACT_META WHERE UPPER(STAGE_TABLE_NAME) = :v_table;
        IF (n_name > 0) THEN
            -- Test rows belong to the prime: keep an inactive one on the SOURCE_PATH convention.
            UPDATE EDP_DEV.CONTROL.EXTRACT_META SET SOURCE_PATH = :v_path
            WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND (NOT COALESCE(ACTIVE_FLAG, FALSE) OR {{FORCE}})
              AND ENDSWITH(UPPER(SOURCE_NAME), '_PRIME_TEST_INITIAL_LOAD')
              AND LOWER(TRIM(COALESCE(COLLATE(SOURCE_PATH, ''), ''), '/ ')) <> LOWER(TRIM(:v_path, '/ '))
              AND NOT EXISTS (SELECT 1 FROM EDP_DEV.CONTROL.EXTRACT_META x
                              WHERE LOWER(TRIM(COLLATE(x.SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(:v_path, '/ ')));
            n_path := SQLROWCOUNT;
            v_action := 'TEST_ROW_EXISTS';
            msg := 'Test row for ' || v_table || ' already exists.'
                || IFF(n_path > 0, ' SOURCE_PATH moved to ' || v_path || '.', '');
            -- Not moved and still off the convention: say why instead of letting Check pair fail with a loop.
            SELECT COUNT(*) INTO :n_off FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE'
              AND LOWER(TRIM(COALESCE(COLLATE(SOURCE_PATH, ''), ''), '/ ')) <> LOWER(TRIM(:v_path, '/ '));
            IF (n_path = 0 AND n_off > 0) THEN
                v_status := 'ERROR';
                msg := 'Test row for ' || v_table || ' could not be moved to SOURCE_PATH ' || v_path || ': it is ACTIVE '
                    || '(check ADF Monitor, then re-run with p_Force = true) or the path is used by another row '
                    || '(sql/02 section C). Nothing changed.';
            END IF;
        ELSE
            SELECT COUNT(*) INTO :n_delta FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = :v_src AND SOURCE_TYPE = 'TABLE';
            SELECT COUNT(*) INTO :n_path FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE LOWER(TRIM(COLLATE(SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(:v_path, '/ '));
            IF (n_delta <> 1) THEN
                v_status := 'ERROR';
                msg := 'Cannot create the test row: expected 1 delta twin (SOURCE_TYPE TABLE) named ' || v_src
                    || ', found ' || n_delta || '.';
            ELSEIF (n_path > 0) THEN
                v_status := 'ERROR';
                msg := 'Cannot create the test row: SOURCE_PATH ' || v_path || ' is already used.';
            ELSE
                INSERT INTO EDP_DEV.CONTROL.EXTRACT_META
                    (SOURCE_NAME, SOURCE_TYPE, OBJECT_NAME, LOAD_TYPE, PRIMARY_KEYS, SQL_SNIPPET, RUN_GROUP,
                     LVL2_FOLDER_NAME, LANDING_SCHEMA, TARGET_SCHEMA, STAGE_TABLE_NAME, ACTIVE_FLAG, SQL_STATEMENT,
                     SOURCE_PATH, REMOVE_SOURCE_FILE_FLAG, RELOAD_SAME_FILE_FLAG, STAGE_FILE_TYPE, STAGE_FILE_SPEC,
                     SCHEMA_DRIFT_CODE, PARALLEL_STAGE_LOAD_FLAG)
                SELECT d.SOURCE_NAME || '_' || UPPER(SPLIT_PART(COLLATE(d.OBJECT_NAME, ''), '.', -1)) || '_PRIME_TEST_INITIAL_LOAD',
                       'FILE', LOWER(SPLIT_PART(COLLATE(d.OBJECT_NAME, ''), '.', -1)) || '_part_%.json', 'APPEND',
                       d.PRIMARY_KEYS, NULL, 'DATA_PRIME', d.LVL2_FOLDER_NAME || '_PRIME_TEST', d.LANDING_SCHEMA,
                       d.TARGET_SCHEMA, :v_table, FALSE, NULL, :v_path, FALSE, FALSE, NULL, NULL,
                       COALESCE(d.SCHEMA_DRIFT_CODE, 2), FALSE
                FROM EDP_DEV.CONTROL.EXTRACT_META d
                WHERE UPPER(d.STAGE_TABLE_NAME) = :v_src AND d.SOURCE_TYPE = 'TABLE'
                  AND NOT EXISTS (SELECT 1 FROM EDP_DEV.CONTROL.EXTRACT_META x WHERE UPPER(x.STAGE_TABLE_NAME) = :v_table);
                IF (SQLROWCOUNT = 0) THEN
                    v_action := 'TEST_ROW_EXISTS';
                    msg := 'Test row for ' || v_table || ' was created by another run.';
                ELSE
                    v_action := 'CREATED_TEST_ROW';
                    msg := 'Created test row ' || v_table || ' (inactive, DATA_PRIME, SOURCE_PATH ' || v_path || ').';
                END IF;
                -- Two first-time runs at once can both insert (no unique constraints): keep the lowest id.
                DELETE FROM EDP_DEV.CONTROL.EXTRACT_META
                WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND NOT COALESCE(ACTIVE_FLAG, FALSE)
                  AND ENDSWITH(UPPER(SOURCE_NAME), '_PRIME_TEST_INITIAL_LOAD')
                  AND EXTRACT_META_ID > (SELECT MIN(EXTRACT_META_ID) FROM EDP_DEV.CONTROL.EXTRACT_META
                                         WHERE UPPER(STAGE_TABLE_NAME) = :v_table);
                n_dup := SQLROWCOUNT;
                IF (n_dup > 0) THEN
                    msg := msg || ' Removed ' || n_dup || ' duplicate(s) from a concurrent run.';
                END IF;
            END IF;
        END IF;
    ELSEIF (v_src = '') THEN
        -- ---------------------------------------------------------------- live table
        SELECT COUNT(*), MAX(EXTRACT_META_ID) INTO :n_init, :v_id FROM EDP_DEV.CONTROL.EXTRACT_META
        WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND STAGE_FILE_TYPE IS NULL
          AND ENDSWITH(UPPER(SOURCE_NAME), 'INITIAL_LOAD');
        IF (n_init = 1) THEN
            SELECT TO_VARCHAR(OBJECT_CONSTRUCT_KEEP_NULL('EXTRACT_META_ID', EXTRACT_META_ID, 'SQL_STATEMENT', SQL_STATEMENT,
                                              'RUN_GROUP', RUN_GROUP, 'SOURCE_PATH', SOURCE_PATH, 'ACTIVE_FLAG', ACTIVE_FLAG)),
                   COALESCE(ACTIVE_FLAG, FALSE),
                   (SQL_STATEMENT IS NOT NULL OR UPPER(COALESCE(COLLATE(RUN_GROUP, ''), '')) <> 'DATA_PRIME'
                    OR LOWER(TRIM(COALESCE(COLLATE(SOURCE_PATH, ''), ''), '/ ')) <> LOWER(TRIM(:v_path, '/ ')))
            INTO :v_old, :v_active, :v_needs
            FROM EDP_DEV.CONTROL.EXTRACT_META WHERE EXTRACT_META_ID = :v_id;
            SELECT COUNT(*) INTO :n_path FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE EXTRACT_META_ID <> :v_id AND LOWER(TRIM(COLLATE(SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(:v_path, '/ '));
            IF (NOT v_needs) THEN
                v_action := 'LIVE_ROW_READY';
                msg := 'Initial row ' || v_id || ' is ready.';
            ELSEIF (NOT {{PREPARE_LIVE}}) THEN
                v_status := 'ERROR';
                v_action := 'LIVE_ROW_NEEDS_PREPARE';
                msg := 'Initial row ' || v_id || ' needs SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME and SOURCE_PATH '
                    || v_path || ' (now ' || v_old || '). Nothing changed. Re-run with p_Prepare_Live_Row = true '
                    || 'to apply; the old values are recorded in this output.';
            ELSEIF (v_active AND NOT {{FORCE}}) THEN
                v_status := 'ERROR';
                msg := 'Initial row ' || v_id || ' is ACTIVE (a prime is running, or one was cancelled); not changed. '
                    || 'After a cancelled run, re-run with p_Force = true.';
            ELSEIF (n_path > 0) THEN
                v_status := 'ERROR';
                msg := 'Cannot prepare initial row ' || v_id || ': SOURCE_PATH ' || v_path || ' is used by another row.';
            ELSE
                UPDATE EDP_DEV.CONTROL.EXTRACT_META
                SET SQL_STATEMENT = NULL, RUN_GROUP = 'DATA_PRIME', SOURCE_PATH = :v_path
                WHERE EXTRACT_META_ID = :v_id AND (COALESCE(ACTIVE_FLAG, FALSE) = FALSE OR {{FORCE}});
                IF (SQLROWCOUNT = 0) THEN
                    v_status := 'ERROR';
                    msg := 'Initial row ' || v_id || ' became ACTIVE before it could be prepared; not changed.';
                ELSE
                    v_action := 'LIVE_ROW_PREPARED';
                    msg := 'Prepared initial row ' || v_id || '. Old values: ' || v_old || '.';
                END IF;
            END IF;
        ELSEIF (n_init = 0) THEN
            SELECT COUNT(*) INTO :n_delta FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'TABLE';
            SELECT COUNT(*), MAX(EXTRACT_META_ID) INTO :n_pq, :v_id FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND UPPER(STAGE_FILE_TYPE) = 'PARQUET'
              AND ENDSWITH(UPPER(SOURCE_NAME), 'INITIAL_LOAD');
            -- Rows other than the Parquet row being converted (v_id NULL when there is none) already on the path.
            SELECT COUNT(*) INTO :n_path FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE EXTRACT_META_ID <> COALESCE(:v_id, -1)
              AND LOWER(TRIM(COLLATE(SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(:v_path, '/ '));
            IF (n_pq = 1) THEN
                -- Parquet INITIAL_LOAD row from an earlier onboarding: convert it in place (opt-in).
                SELECT TO_VARCHAR(OBJECT_CONSTRUCT_KEEP_NULL('EXTRACT_META_ID', EXTRACT_META_ID, 'OBJECT_NAME', OBJECT_NAME,
                                                  'STAGE_FILE_TYPE', STAGE_FILE_TYPE, 'STAGE_FILE_SPEC', STAGE_FILE_SPEC,
                                                  'SCHEMA_DRIFT_CODE', SCHEMA_DRIFT_CODE, 'SQL_STATEMENT', SQL_STATEMENT,
                                                  'RUN_GROUP', RUN_GROUP, 'SOURCE_PATH', SOURCE_PATH,
                                                  'ACTIVE_FLAG', ACTIVE_FLAG)),
                       COALESCE(ACTIVE_FLAG, FALSE)
                INTO :v_old, :v_active
                FROM EDP_DEV.CONTROL.EXTRACT_META WHERE EXTRACT_META_ID = :v_id;
                IF (NOT {{PREPARE_LIVE}}) THEN
                    v_status := 'ERROR';
                    v_action := 'LIVE_ROW_NEEDS_CONVERT';
                    msg := 'Initial row ' || v_id || ' is a Parquet row (now ' || v_old || '). The JSON prime needs it '
                        || 'converted to JSON (OBJECT_NAME <t>_part_%.json, STAGE_FILE_TYPE/STAGE_FILE_SPEC NULL, '
                        || 'SCHEMA_DRIFT_CODE from the delta twin, SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME, SOURCE_PATH '
                        || v_path || '). Nothing changed. Re-run with p_Prepare_Live_Row = true to apply; '
                        || 'the old values are recorded in this output.';
                ELSEIF (n_delta <> 1) THEN
                    v_status := 'ERROR';
                    msg := 'Cannot convert initial row ' || v_id || ': expected 1 delta twin (SOURCE_TYPE TABLE) for '
                        || v_table || ', found ' || n_delta || '.';
                ELSEIF (v_active AND NOT {{FORCE}}) THEN
                    v_status := 'ERROR';
                    msg := 'Initial row ' || v_id || ' is ACTIVE (a prime is running, or one was cancelled); not changed. '
                        || 'After a cancelled run, re-run with p_Force = true.';
                ELSEIF (n_path > 0) THEN
                    v_status := 'ERROR';
                    msg := 'Cannot convert initial row ' || v_id || ': SOURCE_PATH ' || v_path || ' is used by another row.';
                ELSE
                    UPDATE EDP_DEV.CONTROL.EXTRACT_META m
                    SET OBJECT_NAME = d.OBJ, STAGE_FILE_TYPE = NULL, STAGE_FILE_SPEC = NULL, SCHEMA_DRIFT_CODE = d.SDC,
                        SQL_STATEMENT = NULL, RUN_GROUP = 'DATA_PRIME', SOURCE_PATH = :v_path
                    FROM (SELECT LOWER(SPLIT_PART(COLLATE(OBJECT_NAME, ''), '.', -1)) || '_part_%.json' AS OBJ,
                                 COALESCE(SCHEMA_DRIFT_CODE, 2) AS SDC
                          FROM EDP_DEV.CONTROL.EXTRACT_META
                          WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'TABLE') d
                    WHERE m.EXTRACT_META_ID = :v_id AND (COALESCE(m.ACTIVE_FLAG, FALSE) = FALSE OR {{FORCE}})
                      AND UPPER(m.STAGE_FILE_TYPE) = 'PARQUET';
                    IF (SQLROWCOUNT = 0) THEN
                        v_status := 'ERROR';
                        msg := 'Initial row ' || v_id || ' changed before it could be converted; not changed.';
                    ELSE
                        v_action := 'LIVE_ROW_CONVERTED';
                        msg := 'Converted initial row ' || v_id || ' from Parquet to JSON. Old values: ' || v_old || '.';
                    END IF;
                END IF;
            ELSEIF (NOT {{PREPARE_LIVE}}) THEN
                v_status := 'ERROR';
                v_action := 'LIVE_ROW_MISSING';
                msg := v_table || ' has no JSON INITIAL_LOAD row. Nothing changed. Re-run with p_Prepare_Live_Row = true to '
                    || 'create it.';
            ELSEIF (n_delta <> 1) THEN
                v_status := 'ERROR';
                msg := 'Cannot create the initial row: expected 1 delta twin (SOURCE_TYPE TABLE) for ' || v_table
                    || ', found ' || n_delta || '.';
            ELSEIF (n_path > 0) THEN
                v_status := 'ERROR';
                msg := 'Cannot create the initial row: SOURCE_PATH ' || v_path || ' is already used.';
            ELSE
                INSERT INTO EDP_DEV.CONTROL.EXTRACT_META
                    (SOURCE_NAME, SOURCE_TYPE, OBJECT_NAME, LOAD_TYPE, PRIMARY_KEYS, SQL_SNIPPET, RUN_GROUP,
                     LVL2_FOLDER_NAME, LANDING_SCHEMA, TARGET_SCHEMA, STAGE_TABLE_NAME, ACTIVE_FLAG, SQL_STATEMENT,
                     SOURCE_PATH, REMOVE_SOURCE_FILE_FLAG, RELOAD_SAME_FILE_FLAG, STAGE_FILE_TYPE, STAGE_FILE_SPEC,
                     SCHEMA_DRIFT_CODE, PARALLEL_STAGE_LOAD_FLAG)
                SELECT d.SOURCE_NAME || '_' || UPPER(SPLIT_PART(COLLATE(d.OBJECT_NAME, ''), '.', -1)) || '_INITIAL_LOAD',
                       'FILE', LOWER(SPLIT_PART(COLLATE(d.OBJECT_NAME, ''), '.', -1)) || '_part_%.json', 'APPEND',
                       d.PRIMARY_KEYS, NULL, 'DATA_PRIME', d.LVL2_FOLDER_NAME, d.LANDING_SCHEMA, d.TARGET_SCHEMA,
                       d.STAGE_TABLE_NAME, FALSE, NULL, :v_path, FALSE, FALSE, NULL, NULL,
                       COALESCE(d.SCHEMA_DRIFT_CODE, 2), FALSE
                FROM EDP_DEV.CONTROL.EXTRACT_META d
                WHERE UPPER(d.STAGE_TABLE_NAME) = :v_table AND d.SOURCE_TYPE = 'TABLE'
                  AND NOT EXISTS (SELECT 1 FROM EDP_DEV.CONTROL.EXTRACT_META x
                                  WHERE UPPER(x.STAGE_TABLE_NAME) = :v_table AND x.SOURCE_TYPE = 'FILE');
                IF (SQLROWCOUNT = 0) THEN
                    v_status := 'ERROR';
                    msg := v_table || ' already has a FILE row that is not a JSON INITIAL_LOAD row (for example the Parquet '
                        || 'INITIAL_LOAD row); not created. Ask the ATILDA owners.';
                ELSE
                    v_action := 'CREATED_LIVE_ROW';
                    msg := 'Created initial row for ' || v_table || ' (inactive, DATA_PRIME, SOURCE_PATH ' || v_path || ').';
                    -- Two runs at once can both insert: keep the lowest id of the rows created here.
                    DELETE FROM EDP_DEV.CONTROL.EXTRACT_META
                    WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND NOT COALESCE(ACTIVE_FLAG, FALSE)
                      AND UPPER(COALESCE(COLLATE(RUN_GROUP, ''), '')) = 'DATA_PRIME'
                      AND LOWER(TRIM(COLLATE(SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(:v_path, '/ '))
                      AND EXTRACT_META_ID > (SELECT MIN(EXTRACT_META_ID) FROM EDP_DEV.CONTROL.EXTRACT_META
                                             WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE');
                END IF;
            END IF;
        END IF;
    END IF;
    res := (SELECT :v_status AS STATUS, :msg AS MESSAGE, :v_action AS ACTION, :v_old AS OLD_VALUES);
    RETURN TABLE(res);
END;
$$
