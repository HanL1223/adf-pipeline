EXECUTE IMMEDIATE $$
-- 'Cleanup test target' Lookup (PL_ATILDA_DATA_PRIME_CLEANUP_TEST), run as the ADF role.
-- Acts only on a *_PRIME_TEST name whose EXTRACT_META rows (one, or duplicates from concurrent first runs) are all
-- inactive FILE rows with SOURCE_NAME ending in _PRIME_TEST_INITIAL_LOAD. Otherwise it changes nothing and returns
-- ERROR. No row at all = nothing to clean (OK).
-- Drops the test BRONZE table. With p_Delete_Test_Row = true it also deletes the test EXTRACT_META row and its
-- auto-generated LOAD_META row (JOB_GROUP_SEQ 0); ATILDA run plans and logs are kept as history.
-- Tokens {{...}} are replaced by tools/build_pipelines.py.
DECLARE
    v_status  VARCHAR DEFAULT 'ERROR';
    msg       VARCHAR DEFAULT '';
    v_table   VARCHAR DEFAULT UPPER(TRIM('{{TABLE}}'));
    n_rows    NUMBER DEFAULT 0;
    n_ok      NUMBER DEFAULT 0;
    v_id      NUMBER;
    v_schema  VARCHAR;
    v_path    VARCHAR;
    v_tbl     VARCHAR;
    n_lm      NUMBER DEFAULT 0;
    res       RESULTSET;
BEGIN
    SELECT COUNT(*),
           COUNT_IF(SOURCE_TYPE = 'FILE' AND ENDSWITH(UPPER(SOURCE_NAME), '_PRIME_TEST_INITIAL_LOAD')
                    AND NOT COALESCE(ACTIVE_FLAG, FALSE)),
           MAX(EXTRACT_META_ID), MAX(UPPER(COLLATE(TARGET_SCHEMA, ''))), MAX(COLLATE(SOURCE_PATH, ''))
    INTO :n_rows, :n_ok, :v_id, :v_schema, :v_path
    FROM EDP_DEV.CONTROL.EXTRACT_META WHERE UPPER(STAGE_TABLE_NAME) = :v_table;

    IF (NOT ENDSWITH(v_table, '_PRIME_TEST')) THEN
        msg := v_table || ' is not a test target (*_PRIME_TEST). Nothing changed.';
    ELSEIF (n_rows = 0) THEN
        v_status := 'OK';
        msg := v_table || ': no test row; nothing to clean.';
        v_path := NULL;
    ELSEIF (n_ok <> n_rows) THEN
        msg := v_table || ': all ' || n_rows || ' row(s) must be inactive test rows (FILE, *_PRIME_TEST_INITIAL_LOAD); '
            || n_ok || ' are. Nothing changed.';
    ELSE
        v_tbl := 'EDP_DEV."' || REPLACE(v_schema, '"', '""') || '"."' || REPLACE(v_table, '"', '""') || '"';
        DROP TABLE IF EXISTS IDENTIFIER(:v_tbl);
        msg := 'Dropped ' || v_tbl || '.';
        IF ({{DELETE_ROW}}) THEN
            DELETE FROM EDP_DEV.CONTROL.LOAD_META
            WHERE UPPER(TARGET_TABLE_NAME) = :v_table AND JOB_GROUP_SEQ = 0
              AND ENDSWITH(UPPER(COLLATE(JOB_GROUP, '')), '_' || :v_table);
            n_lm := SQLROWCOUNT;
            DELETE FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = :v_table AND SOURCE_TYPE = 'FILE' AND NOT COALESCE(ACTIVE_FLAG, FALSE)
              AND ENDSWITH(UPPER(SOURCE_NAME), '_PRIME_TEST_INITIAL_LOAD');
            msg := msg || ' Deleted ' || SQLROWCOUNT || ' test row(s) and ' || n_lm || ' auto LOAD_META row(s).';
        ELSE
            msg := msg || ' Test row ' || v_id || ' kept for the next test.';
        END IF;
        v_status := 'OK';
    END IF;
    res := (SELECT :v_status AS STATUS, :msg AS MESSAGE, :v_path AS SOURCE_PATH);
    RETURN TABLE(res);
END;
$$
