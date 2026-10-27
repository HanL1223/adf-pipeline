# Live prime of DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY with the JSON build

Status 2026-10-27. This file covers three things: the bug audit, what changed in this round, and the step-by-step live prime.
It supersedes the open TODO lists in handover.md section 11 and the "Live prime" section of adf_old_json/README.md.

## 1. Whole pipeline or only "Data out"?

Run the **whole** entry pipeline, PL_ATILDA_DATA_PRIME_JSON. Running Data out on its own does not work for a live prime:

- **The files belong to the test row.** The test run wrote its files to `data_prime_test/db_stockdb_productnetworkcostshistory_prime_test/` for test row 790. The live prime uses the live initial row (768), with its own SOURCE_PATH and EXTRACT_META_ID. Data out only plans files under that row's path, and only files from the current RunId.
- **The live window is different.** For handover, the live prime needs the full history (or the window you choose) up to the source server's clock. The test used a fixed past window.
- **The safety steps live in the parent pipeline.** Prepare (converts row 768 to JSON), Check pair, Begin and End all run in PL_ATILDA_DATA_PRIME_JSON:
  - Begin takes the lock, turns delta 156 OFF and truncates the live BRONZE table.
  - End reconciles the rows and turns delta 156 back ON only after a pass.
  - Data out alone would append into a BRONZE table that still holds the delta's data, with the delta still running.

## 2. Bug audit: every finding and its status

| # | Finding (where) | Before this change | Now |
|---|---|---|---|
| 1 | A blank `p_Batch_ID` reaches the child as null, and SET BATCH ID fails on `replace(null)` (JSON test run 1) | Fixed in JSON only | `@empty(p_Batch_ID)` in **all** targets |
| 2 | Copy to BLOB read `SOURCE_PATH/FILE_NAME` and got a 404. OBJECT_NAME is the landing file; FILE_NAME is ATILDA's target path (JSON run 3, DESIGN 10.4 disproved) | Fixed in JSON only | All targets read `SOURCE_PATH/OBJECT_NAME` and write `LVL1_FOLDER_NAME/FILE_NAME`. JSON copies JSON to JSON. Parquet copies Parquet to Parquet on DS_Storage_Landing_Parquet, which needs Java, as the Parquet extract already does |
| 3 | Old plans stayed open: the view checks `extract_run_plan_status`, not `etlplan_status` (JSON run 4) | Fixed in JSON only, in a separate tools/json/sf_begin.sql | Fixed in the shared tools/sf_begin.sql; the JSON copy is deleted |
| 4 | A JSON live prime is impossible while live row 768 is a Parquet row. Prepare refused to touch it | Open (needed your decision) | **Opt-in conversion**: with `p_Prepare_Live_Row = true`, JSON Prepare converts the single Parquet INITIAL_LOAD row in place and returns every old value in OLD_VALUES (section 4 step 5) |
| 5 | No JSON build for cwr-ae-dev-data-adf | Missing | New target `new_json` creates adf_new_json/pipeline/. It matches adf_old_json except for the container (the StagingContainer global) |
| 6 | DS_Snowflake runs as OPS_DEV; no StagingContainer global in the old factory; `File ""` fails validation; collated CONTROL columns | Fixed earlier | Still fixed in all targets (checked) |
| 7 | Docs out of date: tools/json "three files", "handover section 9", README 1.3 "FILE_NAME is the landing name" | Stale | Corrected here. The .docx files were not edited |

**Still open.** Code cannot fix these yet:

- **A. Load time grows with the file count (suspected).** PR_CREATE_EXTRACT_ARTIFACTS(meta, plan) also touches every other open plan of the row, and Data out creates all plans up front. Total work may then grow with files², which could explain the 5-hour test run. Confirm it from that run, using the EXTRACT_RUN_LOG entries per plan. Until then, keep the file count low (section 4 step 3).
- **B. New factory, Snowflake side.** CONTROL.STORAGE_DETAILS and EDP_AZURE_SI_DEV must allow `azure://cwraedevdata.blob.core.windows.net/staging/` (section 3).
- **C. Parquet builds.** They need 64-bit Java on the SHIR, in either factory.
- **D. Live-mode ATILDA behaviour on an existing BRONZE table.** This is the first live run. Proven so far: first load into a new table, in the test. Not yet proven: loading into an existing, truncated table. End's reconcile catches any row mismatch, and the delta stays OFF on a failure.

## 3. Factory choice: gate for cwr-ae-dev-data-adf (adf_new_json)

**The JSON prime is only proven in the old factory, with adf_old_json.** Use the new factory only when all five checks pass. Otherwise run the live prime in the old factory: the steps below are identical apart from the import.

| Check | How | Pass when |
|---|---|---|
| G1 Snowflake reads the new account | `SELECT * FROM EDP_DEV.CONTROL.STORAGE_DETAILS;` and `DESC STORAGE INTEGRATION EDP_AZURE_SI_DEV;` | Both include `azure://cwraedevdata.blob.core.windows.net/staging/`. Ask the ATILDA owners how a FILE row picks its storage location |
| G2 Storage rights | Azure portal, cwraedevdata, Access control | The factory managed identity (or the SHIR's identity) has Storage Blob Data Contributor. Part 1 writes and deletes blobs |
| G3 Source DB | data-factory/factory/cwr-ae-dev-data-adf.json, `SourceConfig.STOCKDB` | It points at the database you intend. Here that is 192.168.29.58 / STOCKDB. The old factory pointed at a test DB (tdb) |
| G4 Snowflake role | linkedService cw_au_edp_snowflake_wh | Role DEV_ACCOUNT_ROLE_DATA_INGESTION (already true in the repo) |
| G5 Datasets | DS_Storage_Stage_JSON (Folder, File, Container; gzip), DS_STORAGE_V2_STAGE_BIN, DS_SQLServer_Shared, DS_Snowflake_Wh | They exist in data-factory/dataset (already true in the repo) |

In the new factory, **repeat a test prime first** (section 4 step 2). The old factory's passing test proves nothing about cwraedevdata/staging.

**Do not run the two factories against the same table at the same time.** They share the CONTROL rows. The lock on the initial row stops a second live prime, but the test row and the live row have separate locks.

## 4. Step by step

Regenerate after any change, from parquet_ingestion/:

```
python3 tools/build_pipelines.py . old_json && python3 tools/check.py adf_old_json/pipeline   # old factory
python3 tools/build_pipelines.py . new_json && python3 tools/check.py adf_new_json/pipeline   # cwr-ae-dev-data-adf
```

### Step 1. Finish and read the current test run (old factory)

1. Let it finish; do not cancel it. If you cancel it, row 790 stays locked and the next run needs `p_Force = true`.
2. Record End prime's output: STATUS, ROWS_COPIED, BRONZE_ROWS, BATCH_MISMATCH.
3. Record from ADF Monitor:
   - the Data in duration;
   - the Data out duration;
   - the number of load-file runs;
   - for one early and one late load-file run, the Copy to BLOB and "Provision SI artifacts and load data" durations.
4. Check open item A, using an early plan id and a late plan id of this run:
   ```sql
   SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_LOG WHERE EXTRACT_RUN_PLAN_ID = <plan_id> ORDER BY 1;
   ```
   Many provision or load entries on an early plan means each call reprocesses all open plans. Send me the counts before the live run.
5. Run the data checks from adf_old_json/README.md ("What this test proves"):
   - the column list matches the live table;
   - BRONZE text matches the delta;
   - the staged copy equals the landing file.
6. Clean up: run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST with `p_Tables = ["DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST"]`. Keep `p_Delete_Test_Row = false` if you will test again in the new factory.

### Step 2. Import (and, in the new factory, test)

1. Import order:
   - old factory: from adf_old_json/pipeline/;
   - new factory: from adf_new_json/pipeline/; this is git-integrated, so it saves to data-factory/pipeline/ on your branch.

   Import _JSON_LOAD_FILE, _JSON_OUT, _JSON_IN, PL_ATILDA_DATA_PRIME_JSON, then _JSON_CLEANUP_TEST.
2. **Save or publish every pipeline**, then run *Validate all*. A Debug run calls the saved or published children, not what is open in the editor.
3. In the old factory, re-import PL_ATILDA_DATA_PRIME_JSON in any case: Prepare and Begin changed in this round.
4. New factory only: run one test prime and pass it, using the same p_Objects as the earlier test, `p_Rows_Per_File = 50000` and a short window (at least 2 files). Then clean up.

### Step 3. Read-only checks before the live run

1. **Rows:** run sql/02_onboard_initial_load.sql. Save the output: it is your backup.
   - Expect delta 156 (TABLE, active, SCMergeCO).
   - Expect initial row 768 (FILE, PARQUET), with PREPARE = `LIVE_ROW_NEEDS_CONVERT (JSON build)`.
   - Section C must show 768's SOURCE_PATH used once.
2. **Delta idle:** delta 156 must have no PLANNED plans. Pick a time outside the SCMergeCO-DailyLoad window, and make sure the prime will finish before the next run:
   ```sql
   SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = 156 AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED';
   ```
3. **Size and p_Rows_Per_File.** Run this in SSMS against the source:
   ```sql
   SELECT COUNT_BIG(*) AS total_rows, SUM(CASE WHEN HistoryID IS NULL THEN 1 ELSE 0 END) AS null_keys,
          MIN(RefreshDatetime) AS min_wm, MAX(RefreshDatetime) AS max_wm
   FROM dbo.ProductNetworkCostsHistory WITH (NOLOCK);
   ```
   - Choose `p_Rows_Per_File` so that `total_rows / p_Rows_Per_File` gives about 10–100 files.
   - Use 1,000,000 unless that gives more than about 100 files. Then raise it, up to 2–3M.
   - Fewer files is faster while open item A is unresolved. The hard limit is 4999 files.
4. **Baseline of the live BRONZE table**, to compare afterwards:
   ```sql
   SELECT COUNT(*), MAX(REFRESHDATETIME) FROM EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;
   SELECT COLUMN_NAME, DATA_TYPE FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY' ORDER BY ORDINAL_POSITION;
   ```
   If a full copy of the table fits, keep one as a rollback point (any role with CREATE in a scratch schema):
   `CREATE TABLE <scratch>.PNCH_BACKUP_20261027 CLONE EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;`
5. **Grants:** run sql/01_grants.sql. The ADF role needs TRUNCATE and SELECT on the live BRONZE table.
6. Optional: dry-run the source query with sql/10_source_query_test_json.sql:
   - set `@ws = N''`, `@we = N''` and `@n` = your p_Rows_Per_File;
   - expect ERROR_MESSAGE = '';
   - run the RANGES_SQL it returns to see the batch count.

### Step 4. Dry run of the conversion (changes nothing)

Debug PL_ATILDA_DATA_PRIME_JSON with the parameters from step 5, but with `p_Prepare_Live_Row = false`.

- Expect it to fail at **Check prepare** with ACTION `LIVE_ROW_NEEDS_CONVERT`.
- The message lists 768's current OBJECT_NAME, STAGE_FILE_TYPE, STAGE_FILE_SPEC, SCHEMA_DRIFT_CODE, SQL_STATEMENT, PARALLEL_STAGE_LOAD_FLAG, RUN_GROUP and ACTIVE_FLAG.
- Nothing has changed.

### Step 5. Live prime

Debug (or trigger) **PL_ATILDA_DATA_PRIME_JSON** with:

| Parameter | Value | Why |
|---|---|---|
| p_Objects | `[{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID"}]` | No `source_from`, so this is live mode. The window column is the delta's watermark. HistoryID gives parallel batches |
| p_Window_Start | `""` | **Full history.** BRONZE is truncated, so a start date such as 2024-07-01 would **drop all older rows** from the live table |
| p_Window_End | `""` | Defaults to the source server's clock at extract start. This is the gap-free handover point for the delta (DESIGN section 8) |
| p_Rows_Per_File | from step 3.3 (for example 1000000) | Fewer files means a faster load |
| p_Enable_Delta | `true` | Delta 156 goes back ON after a passed reconcile |
| p_Prepare_Live_Row | `true` | Converts 768 to JSON (one-off). The OLD_VALUES output is the backup |
| p_Force | `false` | Only after a cancelled run |
| p_Batch_ID | e.g. `PRIME_LIVE_20261028` | Logged on every plan |

What each step should show:

1. **Prepare row:** ACTION `LIVE_ROW_CONVERTED`, with OLD_VALUES. Save them. On a re-run you will see `LIVE_ROW_READY` instead.
2. **Check pair:** ERROR_MESSAGE empty, TEST_MODE false, DELTA_META_ID 156, INITIAL_META_ID 768, SOURCE_PATH = 768's path.
3. **Get source columns:** WINDOW_FILTER has only an end bound: `AND [RefreshDatetime] < CONVERT(<column type>, '<server clock>', 126)`.
4. **Begin prime:** STATUS OK, "delta twin 156 is OFF". **The live BRONZE table is now empty.**
5. **Data in:** one `productnetworkcostshistory_part_<RunId>_b<k>_1.json` per batch under 768's SOURCE_PATH. `batch_rows` is returned.
6. **Data out:** plans = files. The first file loads alone, then the rest 4 at a time.
7. **End prime:**
   - STATUS OK;
   - ROWS_COPIED = BRONZE_ROWS;
   - OTHER_ROWS 0, DUP_KEYS 0, no batch mismatch;
   - DELTA_RESTORED true.
   - If MESSAGE says the delta was left OFF, read the reason (empty BRONZE, or the watermark is unusable).

### Step 6. After the run

1. Run sql/03_monitor.sql with the live table name and the RunId:
   - A: 768 inactive, 156 active;
   - B: every plan COMPLETED, rows per batch = batch_rows, MAX watermark just below the end bound.
2. Compare with the step 3.4 baseline:
   - the row count should be about the source total_rows from step 3.3, minus rows with a NULL RefreshDatetime, which a windowed prime excludes;
   - the column list should be the same. Extra columns are fine; any missing column means the JSON sink dropped all-null fields.
3. Let the next SCMergeCO delta run, then confirm it planned and loaded (normal delta behaviour, from the BRONZE MAX watermark).
4. Optional: delete the landing files under 768's SOURCE_PATH. The next prime clears them anyway.

### Step 7. If it fails

| Situation | What happened | What to do |
|---|---|---|
| Fails before Begin (Prepare, Check pair, source columns, batch ranges) | Nothing changed, except the 768 conversion if Prepare ran with `true` | Fix the cause and re-run |
| Fails after Begin | Abort released the lock. **Delta 156 stays OFF**, and BRONZE is partial or empty | Fix the cause and re-run the same live prime. Restore from the clone if you need the data back before then |
| End STATUS ERROR | The reconcile failed. Lock released, delta OFF | Read MESSAGE (rows_copied, bronze_rows, batches). Re-run |
| Run cancelled | 768 is still ACTIVE (locked) and the delta is OFF | Check in ADF Monitor that nothing is running, then re-run with `p_Force = true` |
| You want 768 back as Parquet | — | No pipeline does the reverse. A CONTROL_DEV_RW holder restores the OLD_VALUES from step 5.1, with sign-off |
