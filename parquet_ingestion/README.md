# Data priming: SQL Server → JSON → ATILDA → BRONZE

This is the run guide, current as of 2026-10-27. **It replaces README.md.docx, handover.md.docx and adf_old_json/README.md.docx.**
DESIGN.md.docx sections 4–9 still describe the design: the pair, test mode, the lock, Begin/End and the handover. Where DESIGN describes Parquet, section 13 (JSON) and this file take precedence.

The pipeline is a one-click ADF run that initial-loads ("primes") SQL Server tables into EDP_DEV.BRONZE through ATILDA, then hands each table over to its delta feed without a gap.

## Contents

| Path | What it is |
|---|---|
| adf_old_json/pipeline/ | The 5 pipelines for the old factory **cwr-ase-edp-dev-adf**. The container is the parameter p_Staging_Container (default snowflake-data). Proven by test run 4 |
| adf_new_json/pipeline/ | The same 5 pipelines for **cwr-ae-dev-data-adf**. The container is the global StagingContainer. Blocked until the section 3 gate passes |
| tools/build_pipelines.py | Generates both builds and sql/10. **Never hand-edit the pipeline JSON** |
| tools/source_query.tsql | 'Get source columns': validates the source, builds SOURCE_QUERY and RANGES_SQL |
| tools/sf_prepare.sql | 'Prepare row': creates the test row. Creates or converts the live initial row only with p_Prepare_Live_Row |
| tools/sf_check_pair.sql | 'Check pair': read-only validation of the INITIAL_LOAD / delta pair |
| tools/sf_begin.sql, sf_end.sql | Begin: lock, delta OFF, truncate. End: reconcile, then delta ON only if safe |
| tools/sf_cleanup_test.sql | Cleanup of *_PRIME_TEST targets |
| tools/check.py | Static checker for the generated JSON |
| sql/01_grants.sql | Read-only grant checks for the ADF role |
| sql/02_onboard_initial_load.sql | Read-only: a table's rows, and what Prepare would do |
| sql/03_monitor.sql | Read-only: pair state, plans, rows per batch, handover watermark |
| sql/10_source_query_test.sql | Generated SSMS dry run of 'Get source columns' |

Regenerate after any change:

```
python3 tools/build_pipelines.py . old && python3 tools/check.py adf_old_json/pipeline   # cwr-ase-edp-dev-adf
python3 tools/build_pipelines.py . new && python3 tools/check.py adf_new_json/pipeline   # cwr-ae-dev-data-adf
```

### How a run flows

The entry point is PL_ATILDA_DATA_PRIME_JSON. It runs per object, 2 objects at a time:

```
Prepare row ─ Check pair ─ Get source columns ─ Get batch ranges (0 batches = stop)   (read-only, except Prepare's opt-in row setup)
  └ Begin prime: lock the initial row, abandon its old open plans, delta OFF, TRUNCATE BRONZE
      └ Data in  (_JSON_IN):  SQL Server ─SHIR─> one gzip JSON file per key-range batch (4 batches at a time)
      └ Data out (_JSON_OUT): list this run's files → PR_CREATE_EXTRACT_RUN_PLAN_FILE (one plan per file)
                              → one file at a time (_JSON_LOAD_FILE): Copy to BLOB → source_rows_read → PR_CREATE_EXTRACT_ARTIFACTS → COMPLETED
      └ End prime: reconcile rows (total, per batch, other rows, duplicate keys, late delta plans), release lock,
                   delta ON only on a pass with a usable watermark
  on any failure after Begin: Abort prime (lock released, delta stays OFF) → Fail
```

Data out is the house file flow (TPL_E_FILE_PARQUET / DI_FILE_CSV_DFIO), including its **one-plan-at-a-time** loop.

### Where it deliberately differs from Altida_Skill.md

| Skill | Here | Why |
|---|---|---|
| Section 10.3 runs the DLA master pipeline for RUN_GROUP INITIAL | Calls PR_CREATE_EXTRACT_RUN_PLAN_FILE / PR_CREATE_EXTRACT_ARTIFACTS directly. The initial row uses RUN_GROUP **DATA_PRIME**, which no trigger runs | The prime selects rows by id. Nothing else picks up a row while the prime holds it ACTIVE |
| Recipe B: STAGE_FILE_TYPE PARQUET, SCHEMA_DRIFT_CODE 0, SQL_STATEMENT 1=1 | STAGE_FILE_TYPE/SPEC **NULL (JSON)**, SCHEMA_DRIFT_CODE copied from the delta (2), SQL_STATEMENT **NULL** | JSON is the RDBMS convention (section 3). 1=1 is not applied to FILE rows anyway (section 3), and Begin truncates BRONZE explicitly |
| Section 2: back up to EXTRACT_META_BKP_* before an UPDATE; OPS_DEV writes | Prepare returns the old values in its output (OLD_VALUES). All CONTROL DML runs as the ADF role (CONTROL_DEV_RW) | No new objects in CONTROL. OPS_DEV cannot write CONTROL in DEV |
| Section 8: abandon a plan with etlplan_status | Begin also logs extract_run_plan_status = ABANDONED | EXTRACT_RUN_PLAN_VW keeps a plan open until that status changes (test run 4) |
| Section 10.3 step 4/10: reconcile BRONZE against a separate source COUNT_BIG | End reconciles BRONZE against the rows ADF extracted per batch (rowsCopied) | The extract is the source of truth for what was loaded, so every lost or duplicated file is caught per batch. A separate COUNT on a live NOLOCK source would drift anyway |
| Section 10.3 step 11: restore the delta to its flag from before the prime | The delta ends ON when p_Enable_Delta is true and the checks pass | After a failed prime the delta is OFF, so "its flag before" would keep it OFF for good. For a delta that is **intentionally inactive**, pass `"enable_delta": "false"` |

## 1. Why JSON (the Parquet build is retired)

| Stage | Parquet | JSON |
|---|---|---|
| Extract on the SHIR | Needs 64-bit **Java** on the SHIR, which it does not have (JreNotFound) | No Java. The path the delta already uses. About 2–4x more bytes before gzip, which costs minutes, not hours |
| Snowflake load | Slightly faster per byte | Slightly slower per byte |
| **Per-file ATILDA work** (plan, copy, provision, load, logs) | Same | Same. **This dominates run time** and depends only on the number of files |
| BRONZE text | Typed CONVERTs, which can differ from the delta's text | Identical to the delta's text |

**Run time is driven by the number of files.** Files load one at a time, at roughly 2–5 minutes each.
- 20 files ≈ 1–1.5 hours, plus the extract.
- The 2024-07-01 to 2026-10-26 test at 50,000 rows per file made dozens of files.
- The default is now **1,000,000 rows per file**. Aim for **10–50 files**.

## 2. Can I run only "Data out"?

**Not for priming.** _JSON_OUT is a child pipeline: you could start it by hand with its parameters, but it is not a safe step on its own.

1. **It loads only the files you point it at.** It lists `<p_Source_Path>/<p_File_Prefix>_part_<p_Run_Id>_*.json` and plans them on the initial row named by p_Initial_Source_Name / p_Initial_Meta_Id.
   - The test run's files are in test row 790's folder, carry the test RunId, and hold **only the test window**.
   - Pointed at live row 768, Data out finds nothing in 768's folder. Pointing it at the test folder would load the wrong period through the wrong row.
2. **The parent's steps around it are what make it safe.**
   - **Prepare** converts 768 to JSON. Without this, 768's OBJECT_NAME `…_part_%.parquet` matches none of the .json files, so ATILDA plans nothing ("Check plan count" fails).
   - **Begin** locks the row, turns delta 156 **OFF** and **truncates** the live BRONZE table. Without it the files append onto the delta's rows (duplicates), and the delta keeps running.
   - **End** reconciles the rows against what was extracted and turns the delta back ON only if they match.
3. **End needs Data in's per-batch row counts** (batch_rows). Data out alone has no rows to reconcile against.

**If a live run fails partway, re-run the whole prime.** It re-extracts, truncates and reloads. Failed plans from the earlier attempt are abandoned by Begin. Resuming from Data out is not supported.

## 3. Which factory

| Factory | Build | Status |
|---|---|---|
| cwr-ase-edp-dev-adf (old) | adf_old_json | **Works** (test run 4: 163,204 rows, reconcile OK) |
| cwr-ae-dev-data-adf (new) | adf_new_json | **Blocked** until G1–G3 pass. Then test there first |

Gate for the new factory:

| Check | How | Pass when |
|---|---|---|
| G1 ATILDA reads the new account | `SELECT * FROM EDP_DEV.CONTROL.STORAGE_DETAILS;` and `DESC STORAGE INTEGRATION EDP_AZURE_SI_DEV;` | Both include `azure://cwraedevdata.blob.core.windows.net/staging/` (on 2026-10-27 only cwraseedpdevst/snowflake-data). The ATILDA owners change this; ask them how a FILE row picks its location |
| G2 Storage rights | cwraedevdata → Access control (IAM) | The factory identity has Storage Blob Data Contributor |
| G3 Source DB | `SourceConfig.STOCKDB` in data-factory/factory/cwr-ae-dev-data-adf.json | It is the DB you intend (192.168.29.58 / STOCKDB). The old factory used a test DB |
| G4 Snowflake role | linked service cw_au_edp_snowflake_wh | DEV_ACCOUNT_ROLE_DATA_INGESTION (true in the repo) |
| G5 Datasets | DS_Storage_Stage_JSON (Folder, File, Container; gzip), DS_STORAGE_V2_STAGE_BIN, DS_SQLServer_Shared, DS_Snowflake_Wh | They exist (true in the repo) |

**Never run both factories against the same table at the same time.** They share the CONTROL rows.

## 4. Prime DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY (live), step by step

### What changes in ATILDA (CONTROL), and who changes it

| Object | Before | During | After a pass | Changed by |
|---|---|---|---|---|
| EXTRACT_META 156 (delta, STOCKDB, SCMergeCO) | ACTIVE TRUE | **FALSE** (from Begin) | **TRUE** (End; only with p_Enable_Delta true and a usable watermark) | Pipeline |
| EXTRACT_META 768 (initial row) | Parquet: OBJECT_NAME `…_part_%.parquet`, STAGE_FILE_TYPE PARQUET + spec, SCHEMA_DRIFT_CODE 0, SQL_STATEMENT 1=1, RUN_GROUP INITIAL | Converted once (Prepare, p_Prepare_Live_Row = true): `productnetworkcostshistory_part_%.json`, STAGE_FILE_TYPE/SPEC NULL, SCHEMA_DRIFT_CODE 2, SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME. ACTIVE TRUE while running (the lock) | ACTIVE FALSE; stays JSON. Id, SOURCE_NAME, SOURCE_PATH, LVL2, PRIMARY_KEYS unchanged | Pipeline |
| EXTRACT_RUN_PLAN / logs for 768 | Any open old plans | Begin abandons the old open plans. One new plan per file | All of this run's plans COMPLETED | Pipeline + ATILDA procs |
| LOAD_META for 768 (auto, JOB_GROUP_SEQ 0, APPEND; skill: id 192) | Exists | Used as is | Unchanged | Nobody. ATILDA creates it if missing |
| BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY | Delta rows | **Truncated** (Begin), then loaded file by file | Full history up to the end bound. The typed _VW is kept by the delta row | Pipeline + ATILDA |
| STORAGE_DETAILS / EDP_AZURE_SI_DEV | — | — | — | ATILDA owners. **New factory only** (gate G1) |

Nothing in CONTROL is edited by hand. If you ever need to undo the 768 conversion, a CONTROL_DEV_RW holder restores the OLD_VALUES from step 5, with sign-off.

### Step 1. Close out the running test (old factory)

1. **Let it finish.** A cancelled run leaves 790 ACTIVE, so the next run would need `p_Force = true`.
2. **From End prime's output, record** STATUS, ROWS_COPIED, BRONZE_ROWS and BATCH_MISMATCH.
   - That run still loaded files 4 at a time. If End reports BRONZE_ROWS > ROWS_COPIED or a batch mismatch, that is the double-load which the new one-at-a-time loop prevents.
3. **Time per file.** In ADF Monitor, note the Data in and Data out durations and the number of _JSON_LOAD_FILE runs.
4. **Test checks.** Use a role that can read the test table.
   - **Columns.** This must return no rows:
     ```sql
     SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY'
     MINUS
     SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST';
     ```
   - **Text.** For one HistoryID, compare both tables: datetimes, decimals, floats, bits.
   - **Staged copy.** Download one landing file and its staged copy (`<LVL1_FOLDER_NAME>/<FILE_NAME>`), gunzip both and compare.
5. **Clean up.** Run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST with `p_Tables = ["DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST"]` and `p_Delete_Test_Row = false`.

### Step 2. Put the pipelines into ADF, then test the new build

Use the folder for your factory: `adf_old_json/pipeline/` (old) or `adf_new_json/pipeline/` (new). Load them **children first**:

1. PL_ATILDA_DATA_PRIME_JSON_LOAD_FILE
2. PL_ATILDA_DATA_PRIME_JSON_OUT
3. PL_ATILDA_DATA_PRIME_JSON_IN
4. PL_ATILDA_DATA_PRIME_JSON
5. PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST

**Old factory (ADF Studio):**
1. For each file, go to Author → Pipelines.
   - If the pipeline exists, open it.
   - If not, use **+ → Pipeline** and name it exactly as the file.
2. Click **{ } (Code)**, replace the whole content with the file, then **OK**.
3. **Save** (git mode) or **Publish** (live mode) after **each** pipeline. A Debug run calls the saved or published children, not what is open in the editor; runs 2 and 3 failed this way.
4. **Validate all** must show no errors. These must exist: DS_Snowflake_Wh, DS_SQLServer_Shared, DS_Storage_Stage_JSON, DS_STORAGE_V2_STAGE_BIN and the global SourceConfig.
5. **This round changed the entry pipeline, _JSON_IN, _JSON_OUT and _JSON_LOAD_FILE.** Re-load all five.
6. The retired Parquet pipelines PL_ATILDA_DATA_PRIME, _IN, _OUT, _LOAD_FILE and _CLEANUP_TEST can be deleted.
7. **Test prime with the new build. This is required before the live run:** the one-at-a-time load has not run yet.
   - Debug PL_ATILDA_DATA_PRIME_JSON with `p_Rows_Per_File = 50000`, `p_Prepare_Live_Row = false` and
     `p_Objects = [{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST","source_from":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","window_start":"2026-09-26","window_end":"2026-10-26","partition_column":"HistoryID"}]`.
     That gives about 4 files, so it proves several plans load one at a time.
   - Pass when End shows STATUS OK, ROWS_COPIED = BRONZE_ROWS and BATCH_MISMATCH empty.
   - Also check that each plan logged source_rows_read and that every plan of the run is COMPLETED (sql/03 B with the test name).
   - Then run the cleanup (step 1.5).

**New factory (git-integrated):**
1. Either import as above, or copy the five files into `data-factory/pipeline/` on a feature branch and open that branch in ADF Studio.
2. Run **Validate all**.
3. Run the test prime from point 7 above there, and pass it.

### Step 3. Read-only checks (Snowflake, SSMS)

1. **ATILDA rows.** Run `sql/02_onboard_initial_load.sql` and **save the output** (it is your backup).
   - A: 156 (TABLE, ACTIVE TRUE, SCMergeCO) and 768 (FILE, PARQUET). 790 is also listed (test).
   - B: 768 shows `LIVE_ROW_NEEDS_CONVERT (JSON build)`.
   - C: 768's SOURCE_PATH is used once.
2. **Auto LOAD_META of the initial row.** Expect one APPEND row with JOB_GROUP_SEQ 0. No row is also fine: ATILDA creates it on the first load.
   ```sql
   SELECT LOAD_META_ID, JOB_GROUP, JOB_ACTION, TARGET_PRIMARY_KEY, JOB_GROUP_SEQ FROM EDP_DEV.CONTROL.LOAD_META
   WHERE TARGET_TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY' ORDER BY 1;
   ```
3. **Delta idle.** This must return no rows:
   `SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = 156 AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED';`
   Start outside the SCMergeCO-DailyLoad window, with time to finish before its next run. Once Begin has run, the delta is OFF and a scheduled run plans nothing for it.
4. **Size.** In SSMS, against the database SourceConfig['STOCKDB'] points to:
   ```sql
   SELECT COUNT_BIG(*) AS total_rows, SUM(CASE WHEN RefreshDatetime IS NULL THEN 1 ELSE 0 END) AS null_wm,
          MIN(RefreshDatetime) AS min_wm, MAX(RefreshDatetime) AS max_wm
   FROM dbo.ProductNetworkCostsHistory WITH (NOLOCK);
   ```
   - p_Rows_Per_File = total_rows / 10–50 files, between 1,000,000 and 3,000,000.
   - If null_wm > 0, stop: those rows are not primed (windowed prime).
5. **Baseline and backup of the live BRONZE table:**
   ```sql
   SELECT COUNT(*), MAX(REFRESHDATETIME) FROM EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;
   CREATE TABLE <scratch schema>.PNCH_BKP_<yyyymmdd> CLONE EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;  -- zero-copy, if allowed
   ```
6. **Grants.** Run `sql/01_grants.sql`. The ADF role needs SELECT and TRUNCATE on the live BRONZE table.
7. **Optional dry run.** In `sql/10_source_query_test.sql`:
   - set `@ws = N''`, `@we = N''` and `@n` = your p_Rows_Per_File;
   - expect ERROR_MESSAGE = '';
   - run its RANGES_SQL to see the batches.

### Step 4. Dry run of the conversion (changes nothing)

Debug PL_ATILDA_DATA_PRIME_JSON with the step 5 parameters, but `p_Prepare_Live_Row = false`.

- It must fail at **Check prepare** with ACTION `LIVE_ROW_NEEDS_CONVERT`.
- MESSAGE lists 768's current values.

### Step 5. Live prime

In ADF: **Debug** (or **Add trigger → Trigger now**) on PL_ATILDA_DATA_PRIME_JSON with:

| Parameter | Value | Note |
|---|---|---|
| p_Objects | `[{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID"}]` | No `source_from`, so this is the live table |
| p_Window_Start | `""` | **Full history.** Begin empties the table, so a start date would permanently drop every older row. Clear any value left over from a test run, and do not put window_start in p_Objects |
| p_Window_End | `""` | The source server's clock at extract start: the delta's handover point |
| p_Rows_Per_File | from step 3.4 | |
| p_Enable_Delta | `true` | |
| p_Prepare_Live_Row | `true` | Converts 768 once |
| p_Force | `false` | `true` only after a cancelled run |
| p_Batch_ID | e.g. `PRIME_LIVE_20261028` | Logged on every plan |
| p_Staging_Container | `snowflake-data` | Old factory only |

Watch it in **Monitor → Pipeline runs** (the Debug tab for a Debug run). Open the ForEach "Loop objects", then each activity's output.

| Activity | Expect |
|---|---|
| Prepare row | ACTION `LIVE_ROW_CONVERTED`, OLD_VALUES = 768's Parquet values. **Copy OLD_VALUES somewhere safe.** Later runs show `LIVE_ROW_READY` |
| Check pair | ERROR_MESSAGE empty, TEST_MODE false, INITIAL_META_ID 768, DELTA_META_ID 156, FILE_PREFIX productnetworkcostshistory |
| Get source columns | ERROR_MESSAGE empty. WINDOW_FILTER has only the end bound, `AND [RefreshDatetime] < CONVERT(<type>, '<server clock>', 126)`. **Note that time: it is the handover point** |
| Get batch ranges | One row per batch (about total_rows / p_Rows_Per_File), plus batch 0 only if HistoryID can be NULL and is. Check batch ranges stops the run here if there are none (BRONZE untouched) |
| Begin prime | STATUS OK, "…delta twin 156 is OFF". **From here the live table is empty until the load finishes** |
| Data in (child) | One `productnetworkcostshistory_part_<RunId>_b<k>_1.json` per batch in 768's SOURCE_PATH. Output batch_rows |
| Data out (child) | Generate Run Plan → Get run plan (plans = files) → Load files, one at a time |
| End prime | STATUS OK; ROWS_COPIED = BRONZE_ROWS; OTHER_ROWS 0; OPEN_PLANS 0; DUP_KEYS 0; BATCH_MISMATCH empty; DELTA_RESTORED true. If the data reconciles but the delta cannot safely resume (BRONZE empty, or the watermark unusable), End returns **ERROR** and MESSAGE gives the reason |

### Step 6. After the run

1. **Monitor.** Run `sql/03_monitor.sql` with the live name and the RunId (the run's trigger time, UTC, yyyyMMddHHmmss).
   - A: 768 ACTIVE FALSE and 156 ACTIVE TRUE.
   - B: every plan COMPLETED and rows per batch match. MAX watermark is just below the end bound from Get source columns.
2. **Compare with the step 3.5 baseline.** Rows ≈ total_rows; rows written after the end bound arrive with the delta.
3. **Delta.** The next SCMergeCO run plans delta 156 from the BRONZE MAX watermark. Check that it COMPLETED:
   `SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = 156 ORDER BY EXTRACT_RUN_PLAN_ID DESC LIMIT 5;`
4. **Clean up.** Drop the backup clone once you are satisfied.

### Step 7. If it fails

| Situation | State | What to do |
|---|---|---|
| Fails before Begin | Nothing changed, except the 768 conversion if Prepare already ran with `true` | Fix the cause and re-run |
| The Begin prime activity itself fails (timeout or connection) | Begin may have committed: 768 ACTIVE, delta OFF, BRONZE possibly truncated. Abort does not run for this case | Check sql/03 A. If 768 is ACTIVE, re-run with `p_Force = true` |
| One file fails to load | The loop still processes the remaining files, then Abort runs | Read that file's _JSON_LOAD_FILE run, fix the cause and re-run the whole prime |
| Fails after Begin, or End STATUS ERROR | Lock released. **Delta 156 OFF.** BRONZE partial or empty | Read the failed activity, or End's MESSAGE. Fix the cause and re-run the **whole** prime. Restore from the clone if the data is needed meanwhile |
| Cancelled | 768 still ACTIVE, delta OFF | Check in ADF Monitor that nothing is running, then re-run with `p_Force = true` |

To read a failure, open the failed activity's error icon. For Data in or Data out, open the child run: Monitor → the child pipeline run → the failed activity.

## 5. Priming the next object

Every primed table is a pair of EXTRACT_META rows with the same STAGE_TABLE_NAME:
- the **delta twin** (SOURCE_TYPE TABLE), which already exists;
- the **initial row** (SOURCE_TYPE FILE, JSON), which Prepare creates or converts.

Nothing is configured by hand. Section 10.3 of the skill lists twins that already exist: 769 PRODUCTPRICINGMATRIX, 770 CATALOGUEITEMS, 773 SKU_SKUDETAILS and 779 CURRENTSOH. Prepare converts them, as it does 768.

1. **ATILDA rows.** Run sql/02 with `stage_table` = the new table. Requirements:
   - **exactly one** delta twin (TABLE row) whose SOURCE_NAME is a SourceConfig key of a **SQL Server** source in your factory;
   - OBJECT_NAME = `schema.table`;
   - PRIMARY_KEYS set, for the duplicate check;
   - SQL_SNIPPET watermarked from BRONZE MAX (recipe A2);
   - note the delta's ACTIVE_FLAG. If it is FALSE **on purpose** (for example a group that is all inactive), prime with `"enable_delta": "false"` so the prime does not start a feed nobody enabled.

   FILE rows for the table:
   - **none:** Prepare creates the JSON initial row at `data_prime/<table>/`;
   - **one Parquet INITIAL_LOAD row:** Prepare converts it;
   - **anything else:** ask the ATILDA owners.
2. **Choose the columns.**
   - **window_column:** the delta snippet's watermark column, of a date, datetime or integer type.
   - **partition_column:** an integer column that leads an index, ideally the clustered PK.
   - No partition_column means one file in one stream: fine for small tables only.
   - No window_column means no gap guarantee at handover.
3. **Grants.** Run sql/01 for the new BRONZE table (SELECT, TRUNCATE).
4. **Size and dry run** (step 3.4 and 3.7, with the new names).
5. **Test prime.**
   - `p_Objects = [{"table":"<T>_PRIME_TEST","source_from":"<T>","window_column":"<wm>","window_start":"<1 month ago>","window_end":"<today>","partition_column":"<key>"}]`.
   - Prepare creates the test row (`data_prime_test/<t>_prime_test/`, inactive, DATA_PRIME). The live table and its delta are never touched.
   - Pass the step 1.4 checks, then run the cleanup with `p_Tables = ["<T>_PRIME_TEST"]`. Use `p_Delete_Test_Row = true` once the test row is no longer needed.
6. **Live prime.** Steps 3–7, with that table's names and ids. Use `p_Prepare_Live_Row = true` on the first live run.

**Several objects in one run.** List them all in `p_Objects`.
- They run 2 at a time.
- Each table may appear only once.
- `p_Force` applies to every listed table, so after a cancel re-run only the cancelled table.

## 6. Fix log (2026-10-27)

| # | Issue | Fix |
|---|---|---|
| 1 | A blank p_Batch_ID arrives as null, and SET BATCH ID failed on replace(null) | `@empty(p_Batch_ID)` |
| 2 | Copy to BLOB read SOURCE_PATH/FILE_NAME (404). OBJECT_NAME is the landing file; FILE_NAME is ATILDA's target path | Read SOURCE_PATH/OBJECT_NAME, write LVL1_FOLDER_NAME/FILE_NAME (JSON to JSON) |
| 3 | Old plans stayed open (the view reads extract_run_plan_status), and ATILDA kept reprocessing them | Begin logs extract_run_plan_status = ABANDONED as well |
| 4 | A JSON live prime was blocked by the Parquet initial row 768 | Opt-in in-place conversion in Prepare (p_Prepare_Live_Row), with OLD_VALUES |
| 5 | No JSON build for the new factory | adf_new_json (target `new`) |
| 6 | The Parquet build needed Java, and its SQL had diverged in two copies | Parquet retired. One SQL set, two JSON targets |
| 7 | Many small files made the run slow | Default p_Rows_Per_File 1,000,000; aim for 10–50 files |
| 8 | Data out loaded files 4 at a time. PR_CREATE_EXTRACT_ARTIFACTS also processes the row's other open plans, so two concurrent calls could load one staged file twice | One plan at a time, as the house file pipelines do |
| 9 | PARALLEL_STAGE_LOAD_FLAG = TRUE was required and written to the live row. The skill's default is FALSE, and TRUE is only for parallel stage loads | No longer required or changed. New initial rows get FALSE |
| 10 | Minor: a redundant query in Begin, and an error message pointing at the retired Parquet cleanup | Removed and fixed |
| 11 | A live End returned OK while leaving the delta OFF (empty BRONZE, or an unusable watermark), so the run went green with the feed stopped | It returns ERROR when p_Enable_Delta is true and the delta could not be turned back on |
| 12 | A window or table with no rows still ran Begin, which truncated BRONZE | "Check batch ranges" stops the run before Begin |
| 13 | source_rows_read was not logged per plan, unlike the house file pipelines | "Update source rows read" after Copy to BLOB |
| 14 | Unused Data in parameters (p_Initial_Meta_Id, p_Rows_Per_File); the generator only checked 5 leftover tokens; stale doc pointers | Removed; any leftover `{{TOKEN}}` fails the build; pointers fixed |

**Open items:**

- **A. New factory gate** (section 3, G1–G3).
- **B. Copy to BLOB re-serialises every record (JSON to JSON).** Step 1.4 checks the text. If it changes, switch to a byte copy on a new uncompressed Binary dataset with a File parameter.
- **C. First live load into an existing, truncated BRONZE table.** Only a new-table load has run so far. End's reconcile is the safety net: on any mismatch the delta stays OFF.
- **E. Batch rows are collected with AppendVariable in a parallel ForEach.** ADF does not guarantee this is thread-safe. A lost append fails safe (End reports a batch mismatch and the delta stays OFF), but it would fail a long run for nothing. Proven so far with one batch only; the step 2.7 test covers several.
- **D. Staged copies are never deleted.** They pile up under LVL1_FOLDER_NAME, as they do for the house pipelines.
