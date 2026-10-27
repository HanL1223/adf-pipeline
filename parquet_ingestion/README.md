# Data priming: SQL Server → JSON → ATILDA → BRONZE

This is the run guide, current as of 2026-10-27. **It replaces README.md.docx, handover.md.docx, adf_old_json/README.md.docx and LIVE_PRIME_JSON.md.**

- The design reasoning in DESIGN.md.docx sections 4–9 still holds: the pair, test mode, the lock, Begin/End, the handover.
- Where DESIGN describes Parquet, section 13 (JSON) and this file take precedence.

The pipeline is a one-click ADF run that initial-loads ("primes") SQL Server tables into EDP_DEV.BRONZE through ATILDA, then hands each table over to its delta feed without a gap.

## Contents

| Path | What it is |
|---|---|
| adf_old_json/pipeline/ | The 5 pipelines for the old factory **cwr-ase-edp-dev-adf** (container = parameter p_Staging_Container, default snowflake-data). Proven: test run 4 passed |
| adf_new_json/pipeline/ | The same 5 pipelines for **cwr-ae-dev-data-adf** (container = global StagingContainer). Blocked until the gate in section 3 passes |
| tools/build_pipelines.py | Generates both builds and sql/10. **Never hand-edit the pipeline JSON** |
| tools/source_query.tsql | 'Get source columns': validates the source, builds SOURCE_QUERY and RANGES_SQL |
| tools/sf_prepare.sql | 'Prepare row': creates the test row; creates or converts the live initial row only with p_Prepare_Live_Row |
| tools/sf_check_pair.sql | 'Check pair': read-only validation of the INITIAL_LOAD / delta pair |
| tools/sf_begin.sql, sf_end.sql | Lock, delta OFF, truncate / reconcile, delta ON only if safe |
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

The pipelines are PL_ATILDA_DATA_PRIME_JSON (the entry point you run) plus _JSON_IN, _JSON_OUT, _JSON_LOAD_FILE and _JSON_CLEANUP_TEST. They live in the ADF folder `ALTIDA/00 Templates/01 Extract/03 Data Prime JSON` (children in `/Child`).

## 1. Why JSON (the Parquet build is retired)

**Performance is about the same, so JSON wins on the other points.**

| Stage | Parquet | JSON | Difference |
|---|---|---|---|
| Extract on the SHIR | Columnar + snappy. Needs 64-bit **Java** on the SHIR, which it does not have (JreNotFound). Staged copy does not help, because the sink runs on the SHIR | gzip, about 2–4x more bytes before compression. No Java. The same path the delta uses | Minutes on a multi-million-row table. The upload is somewhat larger |
| Copy to BLOB (landing → ATILDA path) | Re-serialised Parquet to Parquet (Java again) | Re-serialised JSON to JSON | Same order |
| Snowflake load (PR_CREATE_EXTRACT_ARTIFACTS) | Faster per byte | Slower per byte | Seconds to minutes per file on DEV_DATA_LOAD_WH |
| **Per-file ATILDA overhead** (plan, copy, provision, load, log calls) | Same | Same | **This dominates.** It depends on the number of files, not the format |
| BRONZE text | Typed CONVERTs, so it can differ from the delta's text | Identical to the delta's text (same SELECT *, same sink) | JSON keeps the primed and delta rows consistent |

The 5-hour test run (2024-07-01 to 2026-10-26) is explained by the file count, not the format: at `p_Rows_Per_File = 50000` it made dozens of files. See open item A. The default is now **1,000,000** rows per file.

## 2. Can I run only "Data out"?

**No.** Always run the entry pipeline, PL_ATILDA_DATA_PRIME_JSON. Data out cannot stand on its own:

1. **It only sees one row's files from one run.** It lists `<SOURCE_PATH>/<prefix>_part_<RunId>_*.json` for the initial row it is given. Files from the test run belong to test row 790, in `data_prime_test/...`, under another RunId. The live row 768 has its own path and id, so nothing would be planned.
2. **The safety steps are in the parent.** Without Prepare, Check pair, Begin and End, nothing would:
   - convert row 768 to JSON;
   - take the lock;
   - turn delta 156 OFF;
   - truncate the live BRONZE table, which still holds the delta's data, so a load would append and duplicate;
   - reconcile the rows, or turn the delta back ON.
3. **End needs Data in's per-batch row counts** (batch_rows) to reconcile. Without them a lost or duplicated file goes unnoticed.
4. **The window must be the live one.** The live prime takes the full history up to the source server's clock. That end bound is the delta's gap-free handover point (DESIGN section 8). The test's fixed window is not.

If a live run fails during Data out, re-run the whole prime. It re-extracts, truncates again and reloads.

A "resume from Data out" mode is possible later. It would need two things proven first:
- whether ATILDA re-plans a file name it planned before (RELOAD_SAME_FILE_FLAG is FALSE);
- a place to keep batch_rows between runs.

Ask for it if the Data in phase turns out to be the slow part.

## 3. Which factory

| Factory | Build | Status |
|---|---|---|
| cwr-ase-edp-dev-adf (old) | adf_old_json | **Works** (test run 4 passed: 163,204 rows, reconcile OK) |
| cwr-ae-dev-data-adf (new) | adf_new_json | **Blocked** until G1–G3 pass. Then test there first |

Gate for the new factory:

| Check | How | Pass when |
|---|---|---|
| G1 ATILDA reads the new account | `SELECT * FROM EDP_DEV.CONTROL.STORAGE_DETAILS;` and `DESC STORAGE INTEGRATION EDP_AZURE_SI_DEV;` | Both include `azure://cwraedevdata.blob.core.windows.net/staging/` (on 2026-10-27 only cwraseedpdevst/snowflake-data). Ask the ATILDA owners how a FILE row picks its location |
| G2 Storage rights | cwraedevdata → Access control | The factory identity can write and delete blobs (Storage Blob Data Contributor) |
| G3 Source DB | `SourceConfig.STOCKDB` in data-factory/factory/cwr-ae-dev-data-adf.json | It is the DB you mean (192.168.29.58 / STOCKDB). The old factory used a test DB |
| G4 Snowflake role | linked service cw_au_edp_snowflake_wh | DEV_ACCOUNT_ROLE_DATA_INGESTION (true in the repo) |
| G5 Datasets | DS_Storage_Stage_JSON (Folder, File, Container; gzip), DS_STORAGE_V2_STAGE_BIN, DS_SQLServer_Shared, DS_Snowflake_Wh | They exist (true in the repo) |

**Never run both factories against the same table at the same time.** They share the CONTROL rows.

## 4. Prime DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY (live), step by step

The table pair:
- delta twin **156**: STOCKDB, `dbo.ProductNetworkCostsHistory`, PK HistoryID, watermark RefreshDatetime, SCMergeCO;
- live initial row **768**: still a Parquet row; the prime converts it;
- test row **790**.

### Step 1. Close out the running test (old factory)

1. **Let it finish.** A cancelled run leaves 790 ACTIVE, so the next run would need `p_Force = true`.
2. **Record End prime's output:** STATUS, ROWS_COPIED, BRONZE_ROWS, BATCH_MISMATCH.
3. **Record from ADF Monitor:**
   - the Data in and Data out durations;
   - the number of _JSON_LOAD_FILE runs;
   - for one early and one late file, the "Copy to BLOB" and "Provision SI artifacts and load data" durations.
4. **Open item A:** for an early and a late plan id of this run, run
   `SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_LOG WHERE EXTRACT_RUN_PLAN_ID = <id> ORDER BY 1;`
   Many provision/load entries on an early plan means each call reprocesses every open plan.
5. **Test checks.** Use a role that can read the test table.
   - **Columns.** This must return no rows:
     ```sql
     SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY'
     MINUS
     SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = 'DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST';
     ```
   - **Text.** Compare one HistoryID in both tables: datetimes, decimals, floats, bits.
   - **Staged copy.** Download one landing file and its staged copy (LVL1_FOLDER_NAME/FILE_NAME), gunzip both and compare. They should hold the same records with the same text.
6. **Clean up.** Run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST with `p_Tables = ["DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST"]`, `p_Delete_Test_Row = false`. This keeps row 790 for later tests.

### Step 2. Import

1. **Import from the build for your factory** (section 3), in this order: _JSON_LOAD_FILE, _JSON_OUT, _JSON_IN, PL_ATILDA_DATA_PRIME_JSON, _JSON_CLEANUP_TEST.
2. **Save or publish every pipeline**, then run *Validate all*. A Debug run calls the **saved or published** children, not what is open in the editor; runs 2 and 3 failed this way.
3. **Old factory:** re-import at least PL_ATILDA_DATA_PRIME_JSON and _JSON_IN. Prepare (row conversion), Begin and the defaults changed. You can delete the retired Parquet pipelines PL_ATILDA_DATA_PRIME, _IN, _OUT, _LOAD_FILE and _CLEANUP_TEST from the factory.
4. **New factory:** the import saves to data-factory/pipeline/ on your branch. Run one **test prime** there first (step 1's parameters with `source_from`, and a 1-month window) and pass it.

### Step 3. Read-only checks

1. **Rows.** Run `sql/02_onboard_initial_load.sql` and **save the output** (it is the backup).
   - A: 156 (TABLE, active), 768 (FILE, PARQUET), 790 (test).
   - B: 768 shows `LIVE_ROW_NEEDS_CONVERT (JSON build)`.
   - C: 768's SOURCE_PATH is used once.
2. **Delta idle.** This must return no rows:
   `SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = 156 AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED';`
   Start outside the SCMergeCO-DailyLoad window, with enough time to finish before its next run. If it does start meanwhile, the delta is OFF and the run does nothing.
3. **Size.** Run in SSMS:
   ```sql
   SELECT COUNT_BIG(*) AS total_rows, SUM(CASE WHEN RefreshDatetime IS NULL THEN 1 ELSE 0 END) AS null_wm,
          MIN(RefreshDatetime) AS min_wm, MAX(RefreshDatetime) AS max_wm
   FROM dbo.ProductNetworkCostsHistory WITH (NOLOCK);
   ```
   Files = total_rows / p_Rows_Per_File.
   - Aim for **10–100 files**: 1,000,000 by default, 2–3M for very large tables.
   - The hard limit is 4999 files.
   - Rows with a NULL RefreshDatetime are **not** primed (windowed prime). If null_wm > 0, tell me before going live.
4. **Baseline the live BRONZE table:**
   ```sql
   SELECT COUNT(*), MAX(REFRESHDATETIME) FROM EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;
   ```
   If you can, keep a zero-copy backup: `CREATE TABLE <scratch schema>.PNCH_BKP_<yyyymmdd> CLONE EDP_DEV.BRONZE.DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY;`
5. **Grants.** Run `sql/01_grants.sql`. The ADF role needs SELECT and TRUNCATE on the live BRONZE table.
6. **Optional dry run.** In `sql/10_source_query_test.sql`:
   - set `@we = N''`, `@ws = N''` and `@n` = your p_Rows_Per_File;
   - expect ERROR_MESSAGE = '';
   - run the RANGES_SQL it returns to see the batches.

### Step 4. Dry run of the conversion (changes nothing)

Debug PL_ATILDA_DATA_PRIME_JSON with the step 5 parameters, but `p_Prepare_Live_Row = false`.

- It must fail at **Check prepare** with ACTION `LIVE_ROW_NEEDS_CONVERT`.
- MESSAGE lists 768's current values.

### Step 5. Live prime

Debug or trigger **PL_ATILDA_DATA_PRIME_JSON**:

| Parameter | Value | Note |
|---|---|---|
| p_Objects | `[{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID"}]` | No `source_from`, so this is the live table |
| p_Window_Start | `""` | **Full history.** Begin empties the table, so a start date would permanently drop every older row from BRONZE |
| p_Window_End | `""` | The source server's clock at extract start: the delta's handover point |
| p_Rows_Per_File | from step 3.3 (default 1000000) | |
| p_Enable_Delta | `true` | Delta 156 goes back ON after a passed reconcile |
| p_Prepare_Live_Row | `true` | Converts 768 to JSON once. **Save Prepare row's OLD_VALUES** |
| p_Force | `false` | `true` only after a cancelled run |
| p_Batch_ID | e.g. `PRIME_LIVE_20261028` | Logged on every plan |
| p_Staging_Container | `snowflake-data` (old factory only) | |

Expected outputs:

1. **Prepare row:** `LIVE_ROW_CONVERTED` with OLD_VALUES. Later runs show `LIVE_ROW_READY`.
2. **Check pair:** ERROR_MESSAGE empty, TEST_MODE false, INITIAL_META_ID 768, DELTA_META_ID 156.
3. **Get source columns:** WINDOW_FILTER is only `AND [RefreshDatetime] < CONVERT(<type>, '<server clock>', 126)`.
4. **Begin prime:** OK, "delta twin 156 is OFF". From here the live table is empty until the load finishes.
5. **Data in:** one `productnetworkcostshistory_part_<RunId>_b<k>_1.json` per batch in 768's SOURCE_PATH.
6. **Data out:** plans = files. The first file loads alone, then 4 at a time.
7. **End prime:** OK; ROWS_COPIED = BRONZE_ROWS; OTHER_ROWS 0; DUP_KEYS 0; no batch mismatch; DELTA_RESTORED true.

### Step 6. After the run

1. **Monitor.** Run `sql/03_monitor.sql` with the live name and the RunId (trigger time, UTC, yyyyMMddHHmmss).
   - A: 768 inactive, 156 active.
   - B: every plan COMPLETED; rows per batch match; MAX watermark just below the end bound.
2. **Compare with the baseline.**
   - Expect rows ≈ total_rows − null_wm (rows written after the end bound come in with the delta).
   - The column list must be the same.
3. **Delta.** Let the next SCMergeCO run plan and load delta 156 as normal.

### Step 7. If it fails

| Situation | State | What to do |
|---|---|---|
| Fails before Begin | Nothing changed, apart from the 768 conversion | Fix the cause and re-run |
| Fails after Begin, or End STATUS ERROR | Lock released. **Delta 156 OFF.** BRONZE partial or empty | Read the failed activity or End's MESSAGE, fix the cause and re-run the whole prime. Restore from the clone if you need the data meanwhile |
| Cancelled | 768 still ACTIVE, delta OFF | Check ADF Monitor that nothing is running, then re-run with `p_Force = true` |
| Undo the conversion | — | No pipeline does this. A CONTROL_DEV_RW holder restores OLD_VALUES, with sign-off |

## 5. Priming the next object

Every object is a pair of EXTRACT_META rows with the same STAGE_TABLE_NAME:
- the **delta twin** (SOURCE_TYPE TABLE), which already exists;
- the **initial row** (SOURCE_TYPE FILE, JSON), which Prepare creates.

Nothing is configured by hand.

1. **Check the delta twin** (sql/02 with `stage_table` set to the new table). There must be exactly one TABLE row, and:
   - SOURCE_NAME is a SourceConfig key of a **SQL Server** source in the factory you use;
   - OBJECT_NAME is `schema.table`;
   - PRIMARY_KEYS is set, for the duplicate-key check;
   - SQL_SNIPPET shows the watermark column.

   Other FILE rows for the table:
   - **none:** Prepare creates the JSON initial row at `data_prime/<table>/`;
   - **one Parquet INITIAL_LOAD row:** Prepare converts it, as for 768;
   - **anything else:** ask the ATILDA owners.
2. **Choose the columns.**
   - **window_column:** the delta's watermark column. It must be date, datetime, datetime2, smalldatetime or an integer type.
   - **partition_column:** an integer column that leads an index, ideally the clustered PK. Leave it empty if there is none; the table then extracts as one file in one stream.
   - **No window_column** (a full-pull delta) means no gap guarantee at handover (DESIGN section 8).
3. **Grants:** sql/01 for the new BRONZE table (SELECT, TRUNCATE).
4. **Dry run:** sql/10 with the new table, window and partition column, as in step 3.6.
5. **Size it** (step 3.3) and pick p_Rows_Per_File.
6. **Test prime.**
   - `p_Objects = [{"table":"<T>_PRIME_TEST","source_from":"<T>","window_column":"<wm>","window_start":"<1 month ago>","window_end":"<today>","partition_column":"<key>"}]`
   - Prepare creates the test row (`data_prime_test/<t>_prime_test/`, inactive, DATA_PRIME). Begin and End never touch the live table or its delta.
   - Pass the step 1.5 checks, then run the cleanup (`p_Tables = ["<T>_PRIME_TEST"]`; `p_Delete_Test_Row = true` once you no longer need it).
7. **Live prime:** steps 3–6, with `p_Prepare_Live_Row = true` on the first live run, and with the table's own names, delta id and initial row id.

**Several objects in one run.** List them all in `p_Objects`.
- They run 2 at a time, each up to 4 copies or loads at a time.
- Each table may appear only once.
- `p_Force` applies to every listed table, so after a cancel re-run only the cancelled table.

## 6. Fix log (2026-10-27)

| # | Issue | Fix |
|---|---|---|
| 1 | Blank p_Batch_ID arrives as null; SET BATCH ID failed on replace(null) | `@empty(p_Batch_ID)` |
| 2 | Copy to BLOB read SOURCE_PATH/FILE_NAME (404). OBJECT_NAME is the landing file; FILE_NAME is ATILDA's target path | Read SOURCE_PATH/OBJECT_NAME, write LVL1_FOLDER_NAME/FILE_NAME (JSON to JSON) |
| 3 | Old plans stayed open (the view reads extract_run_plan_status) and ATILDA reprocessed them | Begin logs extract_run_plan_status = ABANDONED as well |
| 4 | A JSON live prime was blocked by the Parquet initial row 768 | Opt-in in-place conversion in Prepare (p_Prepare_Live_Row), with OLD_VALUES |
| 5 | No JSON build for the new factory | adf_new_json (target `new`) |
| 6 | Parquet and JSON SQL diverged in two copies; the Parquet build needed Java | Parquet build retired. One set of SQL in tools/, two JSON targets |
| 7 | Many small files made the load slow (test: 50,000 rows per file) | Default p_Rows_Per_File 1,000,000; guidance of 10–100 files |
| 8 | A test-row error message pointed at the retired Parquet cleanup | It now points at PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST |
| 9 | Begin ran a redundant count query | Removed |

**Open items:**

- **A. Is the per-file load cost quadratic?** PR_CREATE_EXTRACT_ARTIFACTS(meta, plan) also logs provision/load for every other open plan of the row (test run 4), and Data out plans all files up front. Check this with step 1.4. If it is confirmed, the next change would plan and load files in smaller waves. Until then, keep files ≤ 100.
- **B. New factory gate** (section 3, G1–G3).
- **C. JSON-to-JSON Copy to BLOB re-serialises every record.** Step 1.5 checks that the text is unchanged. If it is not, switch to a byte copy on a new uncompressed Binary dataset with a File parameter.
- **D. First live load into an existing, truncated BRONZE table.** Only a new-table load has been proven so far. End's reconcile is the safety net: on any mismatch the delta stays OFF.
- **E. Staged copies are never deleted.** They pile up under LVL1_FOLDER_NAME, as they do for the house pipelines.
