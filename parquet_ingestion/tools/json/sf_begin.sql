EXECUTE IMMEDIATE $$
-- JSON variant of tools/sf_begin.sql (target old_json): old plans are closed with extract_run_plan_status, which
-- EXTRACT_RUN_PLAN_VW reads (open = latest extract_run_plan_status 'PLANNED'); etlplan_status alone does not close them.
-- 'Begin prime' Lookup: lock, abandon the initial row's old plans, delta OFF, truncate BRONZE.
-- Runs only after 'Check pair' and 'Get source columns' passed. Tokens {{...}} come from 'Check pair'.
-- Test mode ({{TEST_MODE}} = True): the delta twin is never touched; only the test table is truncated.
DECLARE
    v_status VARCHAR DEFAULT 'ERROR';
    msg      VARCHAR DEFAULT '';
    cnt      NUMBER DEFAULT 0;
    v_maxplan NUMBER DEFAULT 0;
    v_pid    NUMBER;
    v_pmsg   VARCHAR DEFAULT 'Abandoned by data prime run {{RUN}}';
    v_tbl    VARCHAR DEFAULT '{{BRONZE}}';
    res      RESULTSET;
BEGIN
    -- Lock (atomic): the initial row's ACTIVE_FLAG. p_Force takes over a lock left by a cancelled run.
    UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = TRUE
    WHERE EXTRACT_META_ID = {{INIT}} AND (COALESCE(ACTIVE_FLAG, FALSE) = FALSE OR {{FORCE}});
    IF (SQLROWCOUNT = 0) THEN
        msg := 'Initial row {{INIT}} is ACTIVE: another prime is running, or one was cancelled. '
            || 'Check ADF Monitor, then re-run with p_Force = true.';
    ELSE
        BEGIN
            -- Old open plans of the initial row belong to earlier primes. Close them, or ATILDA keeps re-processing them
            -- (PR_CREATE_EXTRACT_ARTIFACTS touches every open plan of the row).
            LET plans RESULTSET := (SELECT EXTRACT_RUN_PLAN_ID FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW
                                    WHERE EXTRACT_META_ID = {{INIT}} AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED');
            LET c_plans CURSOR FOR plans;
            FOR rec IN c_plans DO
                v_pid := rec.EXTRACT_RUN_PLAN_ID;
                CALL EDP_DEV.CONTROL.PR_LOG_EXTRACT_RUN_PLAN(:v_pid, 'etlplan_status', 'ABANDONED', :v_pmsg);
                CALL EDP_DEV.CONTROL.PR_LOG_EXTRACT_RUN_PLAN(:v_pid, 'extract_run_plan_status', 'ABANDONED', :v_pmsg);
            END FOR;

            -- Delta OFF, then re-check: a delta run may have planned since 'Check pair'. Skipped in test mode.
            IF (NOT {{TEST_MODE}}) THEN
                UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE WHERE EXTRACT_META_ID = {{DELTA}};
                SELECT COUNT(*), COALESCE(MAX(EXTRACT_RUN_PLAN_ID), 0) INTO :cnt, :v_maxplan
                FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = {{DELTA}};
                SELECT COUNT(*) INTO :cnt FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW
                WHERE EXTRACT_META_ID = {{DELTA}} AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED';
                -- v_maxplan: the delta's highest plan id once it is OFF; End fails on any later plan (late_delta).
            END IF;
            IF (cnt > 0) THEN
                UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = {{DELTA_ACTIVE}} WHERE EXTRACT_META_ID = {{DELTA}};
                UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE WHERE EXTRACT_META_ID = {{INIT}};
                msg := 'Delta twin {{DELTA}} planned ' || cnt || ' run(s) while the prime was starting. '
                    || 'Nothing was truncated. Let it finish, then re-run.';
            ELSE
                TRUNCATE TABLE IF EXISTS IDENTIFIER(:v_tbl);
                v_status := 'OK';
                msg := 'Locked and cleared ' || v_tbl
                    || IFF({{TEST_MODE}}, ' (test mode: delta twin {{DELTA}} untouched).', '; delta twin {{DELTA}} is OFF.');
            END IF;
        EXCEPTION
            -- TRUNCATE is the last statement that can fail, so on any error BRONZE is untouched: restore both flags.
            WHEN OTHER THEN
                v_status := 'ERROR';
                msg := 'Begin failed (' || SQLERRM || '). Nothing was truncated; lock released, delta twin restored.';
                IF (NOT {{TEST_MODE}}) THEN
                    UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = {{DELTA_ACTIVE}} WHERE EXTRACT_META_ID = {{DELTA}};
                END IF;
                UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE WHERE EXTRACT_META_ID = {{INIT}};
        END;
    END IF;
    res := (SELECT :v_status AS STATUS, :msg AS MESSAGE, :v_maxplan AS DELTA_MAX_PLAN);
    RETURN TABLE(res);
END;
$$
