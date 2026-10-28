-- The initial row must stage JSON (STAGE_FILE_TYPE NULL) with OBJECT_NAME <prefix>_part_%.json and SOURCE_PATH
-- <lowercase table>/ (its folder in the landing container).
-- 'Check pair' Lookup (read-only). Finds the INITIAL_LOAD / delta pair of one STAGE_TABLE_NAME, validates it and
-- derives everything later steps need. Tokens {{...}} are replaced by tools/build_pipelines.py.
-- Test mode: when source_from names another (live) table, the delta twin is looked up under that name and used
-- only as the source definition; the live table and its delta twin are never changed.
-- CONTROL text columns carry a collation; COLLATE(col, '') strips it where TRIM/RTRIM with characters would fail
-- ("Function TRIM with collations requires whitespace-only constant as the trim characters").
WITH i AS (
    SELECT * FROM EDP_DEV.CONTROL.EXTRACT_META
    WHERE UPPER(STAGE_TABLE_NAME) = UPPER('{{TABLE}}') AND SOURCE_TYPE = 'FILE'
      AND STAGE_FILE_TYPE IS NULL AND ENDSWITH(UPPER(SOURCE_NAME), 'INITIAL_LOAD')),
t AS (
    -- Test mode needs both: source_from set AND a target named *_PRIME_TEST. Anything in between is an error.
    SELECT COALESCE(NULLIF(UPPER(TRIM('{{SOURCE_FROM}}')), ''), UPPER('{{TABLE}}')) AS source_table,
           NULLIF(TRIM('{{SOURCE_FROM}}'), '') IS NOT NULL AS src_set,
           ENDSWITH(UPPER(TRIM('{{TABLE}}')), '_PRIME_TEST') AS ends_test,
           src_set AND ends_test AND UPPER(TRIM('{{TABLE}}')) = UPPER(TRIM('{{SOURCE_FROM}}')) || '_PRIME_TEST' AS test_mode,
           -- Any other row (whatever SOURCE_TYPE) under the test name means it is a live table.
           (SELECT COUNT(*) FROM EDP_DEV.CONTROL.EXTRACT_META
            WHERE UPPER(STAGE_TABLE_NAME) = UPPER('{{TABLE}}')) - (SELECT COUNT(*) FROM i) AS live_rows),
d AS (
    SELECT * FROM EDP_DEV.CONTROL.EXTRACT_META
    WHERE UPPER(STAGE_TABLE_NAME) = (SELECT source_table FROM t) AND SOURCE_TYPE = 'TABLE'
      AND TARGET_SCHEMA IN (SELECT TARGET_SCHEMA FROM i)),
n AS (SELECT (SELECT COUNT(*) FROM i) AS ni, (SELECT COUNT(*) FROM d) AS nd),
p AS (SELECT COUNT(*) AS path_users FROM EDP_DEV.CONTROL.EXTRACT_META m, i
      WHERE m.EXTRACT_META_ID <> i.EXTRACT_META_ID
        AND LOWER(TRIM(COLLATE(m.SOURCE_PATH, ''), '/ ')) = LOWER(TRIM(COLLATE(i.SOURCE_PATH, ''), '/ '))),
o AS (SELECT COUNT(*) AS d_open FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW s, d
      WHERE s.EXTRACT_META_ID = d.EXTRACT_META_ID AND s.EXTRACT_RUN_PLAN_STATUS = 'PLANNED'),
k AS (SELECT LISTAGG('"' || REPLACE(UPPER(TRIM(t.value)), '"', '""') || '"', ', ') AS pk_cols
      FROM i, d, TABLE(SPLIT_TO_TABLE(COALESCE(NULLIF(TRIM(COLLATE(i.PRIMARY_KEYS, '')), ''), NULLIF(TRIM(COLLATE(d.PRIMARY_KEYS, '')), ''), ''), ',')) t
      WHERE TRIM(t.value) <> ''),
x AS (
    SELECT n.ni, n.nd, p.path_users, o.d_open, k.pk_cols, t.test_mode, t.live_rows, t.source_table,
           t.src_set, t.ends_test, COLLATE(i.RUN_GROUP, '') AS i_group,
           i.EXTRACT_META_ID AS i_id, COLLATE(i.SOURCE_NAME, '') AS i_source, COLLATE(i.SOURCE_PATH, '') AS i_path,
           COLLATE(i.OBJECT_NAME, '') AS i_object,
           COLLATE(i.SQL_STATEMENT, '') AS i_sqlstmt,
           COALESCE(i.ACTIVE_FLAG, FALSE) AS i_active, COLLATE(i.TARGET_SCHEMA, '') AS tschema, COLLATE(i.STAGE_TABLE_NAME, '') AS stage,
           d.EXTRACT_META_ID AS d_id, COLLATE(d.SOURCE_NAME, '') AS d_source, COLLATE(d.OBJECT_NAME, '') AS d_object,
           COALESCE(d.ACTIVE_FLAG, FALSE) AS d_active, NULLIF(TRIM(COLLATE(d.SQL_SNIPPET, '')), '') AS d_snippet
    FROM n CROSS JOIN p CROSS JOIN o CROSS JOIN k CROSS JOIN t LEFT JOIN i ON TRUE LEFT JOIN d ON TRUE
    LIMIT 1),
y AS (SELECT x.*, 'EDP_DEV."' || REPLACE(UPPER(tschema), '"', '""') || '"."' || REPLACE(UPPER(stage), '"', '""') || '"' AS bronze
      FROM x)
SELECT
    CASE
        WHEN ni <> 1 THEN 'Expected 1 INITIAL_LOAD JSON row (SOURCE_TYPE FILE, STAGE_FILE_TYPE NULL) for {{TABLE}}, found ' || ni
                          || '. Test target: if an old (Parquet) test row exists, run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST '
                          || 'with p_Delete_Test_Row = true first. Live table: re-run with p_Prepare_Live_Row = true.'
        WHEN src_set AND NOT ends_test THEN 'source_from is set, so {{TABLE}} must be a test target named *_PRIME_TEST '
                          || '(created automatically). Remove source_from to prime the live table.'
        WHEN ends_test AND NOT src_set THEN '{{TABLE}} is a test target; add "source_from": "<live table>".'
        WHEN src_set AND NOT test_mode THEN 'source_from must be the live table whose name + _PRIME_TEST is {{TABLE}}.'
        WHEN test_mode AND live_rows > 0 THEN 'Test target {{TABLE}} has ' || live_rows || ' other EXTRACT_META row(s); '
                          || 'it is not a pure test table.'
        WHEN nd <> 1 THEN 'Expected 1 delta twin (SOURCE_TYPE TABLE) named ' || source_table
                          || ' with the same TARGET_SCHEMA, found ' || nd || '.'
        WHEN i_sqlstmt IS NOT NULL THEN 'Initial row ' || i_id || ' has SQL_STATEMENT ''' || i_sqlstmt
                          || '''; it must be NULL (1=1 deletes BRONZE before every file). Re-run with p_Prepare_Live_Row = true.'
        WHEN UPPER(COALESCE(i_group, '')) <> 'DATA_PRIME' THEN 'Initial row ' || i_id || ' RUN_GROUP is '
                          || COALESCE(i_group, 'NULL') || '; it must be DATA_PRIME so no trigger runs it. Re-run with p_Prepare_Live_Row = true.'
        WHEN LOWER(TRIM(COALESCE(i_path, ''), '/ ')) <> LOWER(TRIM('{{TABLE}}'))
             THEN 'Initial row ' || i_id || ' SOURCE_PATH must be ' || LOWER(TRIM('{{TABLE}}')) || '/ (its folder in the '
                  || 'landing container), got ' || COALESCE(i_path, 'NULL') || '. Re-run: Prepare moves a test row itself; '
                  || 'a live row needs p_Prepare_Live_Row = true.'
        WHEN i_object IS NULL OR NOT ENDSWITH(LOWER(i_object), '_part_%.json') OR LENGTH(i_object) <= 12
             OR CONTAINS(LEFT(i_object, LENGTH(i_object) - 12), '%')
             THEN 'Initial row ' || i_id || ' OBJECT_NAME must be <prefix>_part_%.json, got ' || COALESCE(i_object, 'NULL') || '.'
        WHEN NULLIF(TRIM(d_object), '') IS NULL THEN 'Delta twin ' || d_id || ' has no OBJECT_NAME (schema.table).'
        WHEN path_users > 0 THEN 'SOURCE_PATH ' || i_path || ' is also used by ' || path_users
                          || ' other EXTRACT_META row(s); the prime clears that folder, so it must be unique.'
        WHEN NOT test_mode AND d_open > 0 THEN 'Delta twin ' || d_id || ' has ' || d_open || ' open (PLANNED) plan(s). Let the delta run finish, '
                          || 'then re-run.'
        WHEN CONTAINS(COALESCE(d_snippet, ''), '$$') THEN 'Delta twin ' || d_id || ' SQL_SNIPPET contains $$; not supported.'
        WHEN i_active AND NOT {{FORCE}} THEN 'Initial row ' || i_id || ' is ACTIVE: another prime is running, or one was '
                          || 'cancelled. Check ADF Monitor, then re-run with p_Force = true.'
        ELSE ''
    END AS ERROR_MESSAGE,
    i_id AS INITIAL_META_ID,
    d_id AS DELTA_META_ID,
    d_active AS DELTA_ACTIVE,
    test_mode AS TEST_MODE,
    COALESCE(TRY_TO_BOOLEAN('{{ENABLE_DELTA}}'), FALSE) AS ENABLE_DELTA,
    d_source AS SOURCE_NAME,
    d_object AS SOURCE_OBJECT,
    i_path AS SOURCE_PATH,
    LEFT(i_object, LENGTH(i_object) - 12) AS FILE_PREFIX,
    i_source AS INITIAL_SOURCE_NAME,
    UPPER(tschema) AS TARGET_SCHEMA,
    UPPER(stage) AS STAGE_TABLE,
    bronze AS BRONZE_TABLE,
    -- Queries for the End block, already escaped for a '...' literal there.
    IFF(d_snippet IS NULL OR test_mode, '',
        REPLACE('SELECT TO_VARCHAR($1) AS S FROM (' || RTRIM(d_snippet, '; ') || ') LIMIT 1', '''', '''''')) AS WM_QUERY,
    IFF(COALESCE(pk_cols, '') = '', '',
        REPLACE('SELECT COUNT(*) AS N FROM (SELECT 1 FROM ' || bronze || ' GROUP BY ' || pk_cols
                || ' HAVING COUNT(*) > 1)', '''', '''''')) AS DUP_QUERY
FROM y
