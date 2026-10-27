EXECUTE IMMEDIATE $$
-- 'End prime' Lookup: reconcile this run, check the delta twin's watermark, release the lock, and turn the delta
-- twin ON only when the reconcile passed, p_Enable_Delta is true, BRONZE is not empty and the watermark is usable.
-- Tokens {{...}} come from 'Check pair' and from the batch rows returned by PL_ATILDA_DATA_PRIME_IN.
-- Test mode ({{TEST_MODE}} = True): reconcile only; the delta twin is never touched.
DECLARE
    v_status    VARCHAR DEFAULT 'ERROR';
    msg         VARCHAR DEFAULT '';
    v_tag       VARCHAR DEFAULT '_part_{{RUN}}_';
    v_rx        VARCHAR DEFAULT '_part_{{RUN}}_b([0-9]+)_';
    v_tbl       VARCHAR DEFAULT '{{BRONZE}}';
    v_wmq       VARCHAR DEFAULT '{{WM_QUERY}}';
    v_dupq      VARCHAR DEFAULT '{{DUP_QUERY}}';
    rows_copied NUMBER DEFAULT 0;
    bronze_rows NUMBER DEFAULT 0;
    other_rows  NUMBER DEFAULT 0;
    open_plans  NUMBER DEFAULT 0;
    mismatch    VARCHAR DEFAULT '';
    dup_keys    NUMBER DEFAULT 0;
    tbl_exists  NUMBER DEFAULT 0;
    late_delta  NUMBER DEFAULT 0;
    wm_ok       BOOLEAN DEFAULT FALSE;
    wm_msg      VARCHAR DEFAULT '';
    outv        VARCHAR;
    tok         VARCHAR;
    restored    BOOLEAN DEFAULT FALSE;
    res         RESULTSET;
BEGIN
    BEGIN
        -- Rows extracted, per batch, as returned by PL_ATILDA_DATA_PRIME_IN: [{"b": "<batch>", "rows": <n>}, ...]
        SELECT COALESCE(SUM(f.value:"rows"::NUMBER), 0) INTO :rows_copied
        FROM TABLE(FLATTEN(INPUT => PARSE_JSON('{{BATCH_JSON}}'))) f;

        SELECT COUNT(*) INTO :open_plans FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW
        WHERE EXTRACT_META_ID = {{INIT}} AND CONTAINS(FILE_NAME, :v_tag) AND EXTRACT_RUN_PLAN_STATUS <> 'COMPLETED';

        -- Live mode: a delta plan created after 'Check pair' (a J4 run that read the flags before begin turned the
        -- delta OFF) could load into BRONZE after this check. Refuse to pass; the delta stays OFF.
        IF (NOT {{TEST_MODE}}) THEN
            SELECT COUNT(*) INTO :late_delta FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW
            WHERE EXTRACT_META_ID = {{DELTA}} AND EXTRACT_RUN_PLAN_ID > {{DELTA_MAX_PLAN}};
        END IF;

        SELECT COUNT(*) INTO :tbl_exists FROM EDP_DEV.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{{TS}}' AND TABLE_NAME = '{{ST}}';

        IF (tbl_exists = 1) THEN
            -- other_rows: BRONZE was truncated at begin, so any row not from this run is contamination
            -- (for example a load still running in Snowflake from a cancelled run).
            SELECT COUNT_IF(CONTAINS(ETL_FILE_NAME, :v_tag)), COUNT_IF(NOT COALESCE(CONTAINS(ETL_FILE_NAME, :v_tag), FALSE))
            INTO :bronze_rows, :other_rows FROM IDENTIFIER(:v_tbl);

            -- Per batch: rows loaded vs rows extracted. A batch of 0 rows wrote no file.
            SELECT COALESCE(LISTAGG('b' || COALESCE(b.k, t.k) || ' extracted ' || COALESCE(b.n, 0)
                                    || ' loaded ' || COALESCE(t.n, 0), '; '), '')
            INTO :mismatch
            FROM (SELECT f.value:"b"::VARCHAR AS k, f.value:"rows"::NUMBER AS n
                  FROM TABLE(FLATTEN(INPUT => PARSE_JSON('{{BATCH_JSON}}'))) f) b
            FULL OUTER JOIN
                 (SELECT REGEXP_SUBSTR(COLLATE(ETL_FILE_NAME, ''), :v_rx, 1, 1, 'e', 1) AS k, COUNT(*) AS n
                  FROM IDENTIFIER(:v_tbl) WHERE CONTAINS(ETL_FILE_NAME, :v_tag) GROUP BY 1) t
              ON b.k = t.k
            WHERE COALESCE(b.n, 0) <> COALESCE(t.n, 0);

            -- Duplicate keys (PRIMARY_KEYS, NULL-safe GROUP BY) over the whole table.
            IF (v_dupq <> '') THEN
                LET drs RESULTSET := (EXECUTE IMMEDIATE :v_dupq);
                LET dc CURSOR FOR drs;
                OPEN dc;
                FETCH dc INTO dup_keys;
                CLOSE dc;
            END IF;
        END IF;

        -- Delta twin watermark: run its own SQL_SNIPPET as ATILDA does. Empty BRONZE or an unparseable watermark
        -- yields 1900-01-01, which would make the delta pull the whole table.
        IF (v_wmq = '') THEN
            wm_ok := TRUE;
            wm_msg := 'no SQL_SNIPPET (full pull feed)';
        ELSE
            BEGIN
                LET wrs RESULTSET := (EXECUTE IMMEDIATE :v_wmq);
                LET wc CURSOR FOR wrs;
                OPEN wc;
                FETCH wc INTO outv;
                CLOSE wc;
                tok := REGEXP_SUBSTR(COLLATE(outv, ''), '[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2}([.][0-9]+)?)?)?');
                wm_ok := (tok IS NULL OR COALESCE(TRY_TO_TIMESTAMP_NTZ(REPLACE(tok, 'T', ' ')) >= '1901-01-01'::TIMESTAMP_NTZ, FALSE));
                wm_msg := 'watermark ' || COALESCE(tok, 'none in snippet output');
            EXCEPTION
                WHEN OTHER THEN
                    wm_ok := FALSE;
                    wm_msg := 'snippet failed: ' || SQLERRM;
            END;
        END IF;

        UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE WHERE EXTRACT_META_ID = {{INIT}};
        IF (open_plans = 0 AND bronze_rows = rows_copied AND other_rows = 0 AND mismatch = '' AND dup_keys = 0
            AND late_delta = 0) THEN
            v_status := 'OK';
            IF ({{TEST_MODE}}) THEN
                msg := 'Test prime: ' || bronze_rows || ' rows in ' || v_tbl || '. Delta twin {{DELTA}} untouched.';
            ELSEIF ({{ENABLE_DELTA}} AND bronze_rows > 0 AND wm_ok) THEN
                UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = TRUE WHERE EXTRACT_META_ID = {{DELTA}};
                restored := TRUE;
                msg := 'Primed ' || bronze_rows || ' rows. Delta twin {{DELTA}} is ON (' || wm_msg || ').';
            ELSE
                msg := 'Primed ' || bronze_rows || ' rows. Delta twin {{DELTA}} left OFF: '
                    || CASE WHEN NOT {{ENABLE_DELTA}} THEN 'p_Enable_Delta is false.'
                            WHEN bronze_rows = 0 THEN 'BRONZE is empty, the delta would pull the whole table.'
                            ELSE 'its watermark is not usable (' || wm_msg || ').' END;
            END IF;
        ELSE
            msg := 'Reconcile failed: open_plans=' || open_plans || ', rows_copied=' || rows_copied
                || ', bronze_rows=' || bronze_rows || ', other_rows=' || other_rows || ', dup_keys=' || dup_keys
                || IFF(late_delta = 0, '', ', delta twin {{DELTA}} planned ' || late_delta || ' run(s) during the prime')
                || IFF(mismatch = '', '', ', batches: ' || mismatch)
                || '. Lock released; delta twin left OFF. Fix the cause and re-run.';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            v_status := 'ERROR';
            msg := 'End failed (' || SQLERRM || '). Lock released; delta twin left OFF.';
            UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE WHERE EXTRACT_META_ID = {{INIT}};
    END;
    res := (SELECT :v_status AS STATUS, :msg AS MESSAGE, :rows_copied AS ROWS_COPIED, :bronze_rows AS BRONZE_ROWS,
                   :other_rows AS OTHER_ROWS, :open_plans AS OPEN_PLANS, :mismatch AS BATCH_MISMATCH,
                   :dup_keys AS DUP_KEYS, :restored AS DELTA_RESTORED);
    RETURN TABLE(res);
END;
$$
