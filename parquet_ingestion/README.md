# Data priming: SQL Server → JSON → ATILDA → BRONZE

**What it does.** It loads a table's full history ("primes" it) from on-prem SQL Server into Snowflake `EDP_DEV.BRONZE` through the ATILDA framework (`EDP_DEV.CONTROL`). It then hands the table back to its normal daily delta, with no gap and no overlap. One ADF pipeline, **PL_ATILDA_DATA_PRIME_JSON**, does everything. You give it the table name and a few column names.

**How to use this file.** Read sections 1–3 once. For each table you want to prime, copy the worksheet in section 4, fill it in, and follow section 5 step by step. Sections 6–7 are reference. Section 8 is a fully worked example (DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY).

**Framework rules.** These follow `src/cw_batch_priming/Altida_Skill.md.docx` (the "ATILDA guide"). Where this solution deliberately differs, section 9 says so and why.

Status: 2026-10-27. This file replaces README.md.docx, handover.md.docx and adf_old_json/README.md.docx. DESIGN.md.docx still holds the design history.

---

## Contents

- [Quick start](#quick-start)
- [1. Concepts](#1-concepts)
- [2. Run group: DATA_PRIME, never SCMergeCO](#2-run-group-data_prime-never-scmergeco)
- [3. One-time setup per factory](#3-one-time-setup-per-factory)
- [4. Table worksheet](#4-table-worksheet-copy-for-each-table)
- [5. Procedure: prime one table](#5-procedure-prime-one-table)
- [6. Reference: parameters and outputs](#6-reference-parameters-and-outputs)
- [7. Reference: what changes in ATILDA](#7-reference-what-changes-in-atilda-control)
- [8. Worked example: DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY](#8-worked-example-db_stockdb_productnetworkcostshistory)
- [9. Alignment with the ATILDA guide](#9-alignment-with-the-atilda-guide)
- [10. FAQ](#10-faq)
- [11. Files, build and change log](#11-files-build-and-change-log)

---

## Quick start

For table `<T>`, with its existing delta row in ATILDA:

1. **Checks (read-only).** In Snowflake, run `sql/02_onboard_initial_load.sql` with `<T>` and save the output. In SSMS, count the source rows. Fill in the worksheet (section 4).
2. **Test prime** into `<T>_PRIME_TEST`, which never touches the real table:
   `p_Objects = [{"table":"<T>_PRIME_TEST","source_from":"<T>","window_column":"<wm>","window_start":"<1 month ago>","window_end":"<today>","partition_column":"<key>"}]`
   Check the result, then run the cleanup pipeline.
3. **Dry run of the live setup.** Use the live `p_Objects` plus `"window_start":"2999-01-02","window_end":"2999-01-01"` (an impossible window), with `p_Prepare_Live_Row = false`. It always stops before Begin, so nothing changes, and it shows what Prepare would change.
4. **Live prime:**
   `p_Objects = [{"table":"<T>","window_column":"<wm>","partition_column":"<key>"}]`, `p_Prepare_Live_Row = true`, window start and end empty (integer watermark: set `window_end`, see section 5 D1).
5. **Verify** with `sql/03_monitor.sql`, then confirm the next daily delta run completes.

Every step, with what to expect, is in section 5.

---

## 1. Concepts

| Term | Meaning |
|---|---|
| **Delta row** ("delta twin") | The table's existing ATILDA row (`SOURCE_TYPE = 'TABLE'`) that loads it daily, for example id 156 in run group SCMergeCO. Its SQL_SNIPPET pulls rows where `<wm> > MAX(<wm>)` in BRONZE (ATILDA guide, recipe A2). **The prime never changes it, except its ACTIVE_FLAG** (OFF during a live prime, ON after a pass) |
| **Initial row** | A second ATILDA row with the **same STAGE_TABLE_NAME** (`SOURCE_TYPE = 'FILE'`, SOURCE_NAME `…_INITIAL_LOAD`). The prime loads its files. Prepare creates it, or converts an old Parquet one, as JSON (STAGE_FILE_TYPE NULL) in run group **DATA_PRIME** |
| **Test target** | `<T>_PRIME_TEST`: a separate BRONZE table with its own initial row, created automatically. A test prime never touches `<T>` or its delta row |
| **Watermark column** `<wm>` | The column the delta's SQL_SNIPPET filters on, for example RefreshDatetime. Pass it as `window_column`. That is what makes the handover gap-free |
| **Partition column** `<key>` | An integer column that leads an index, ideally the clustered primary key. Batches are split on it: one JSON file per `p_Rows_Per_File` rows |
| **Landing container** `json-priming` | The prime's own container, on the same storage account as ATILDA's `snowflake-data`. Each table has one folder in it, named after the table in lowercase: `json-priming/<t>/`. That folder is the initial row's SOURCE_PATH. Only ADF reads it (section 3.1) |
| **Run** | One trigger of PL_ATILDA_DATA_PRIME_JSON. Its **RunId** is the trigger time in UTC as `yyyyMMddHHmmss`. It appears in every file name (`<prefix>_part_<RunId>_b<k>_1.json`) and in `sql/03` |

What one run does, per table (2 tables at a time):

```
Prepare row        set up the initial row (test: create it; live: create/convert/fix only with p_Prepare_Live_Row)
Check pair         read-only: validate the delta row + initial row pair
Get source columns read-only, SQL Server: validate table/columns/window, build the SELECT and batch query
Get batch ranges   read-only, SQL Server: key ranges, one per file; 0 batches = stop here
── nothing has changed in BRONZE or the delta row up to this point ──
Begin prime        lock (initial row ACTIVE), close old plans, delta row OFF, TRUNCATE BRONZE
Data in            SQL Server ─SHIR─> one gzip JSON file per batch (4 at a time) into json-priming/<table>/
Data out           ATILDA plans one plan per file, then loads them one at a time:
                   copy json-priming/<table>/<file> → snowflake-data/<ATILDA path> → log rows
                   → PR_CREATE_EXTRACT_ARTIFACTS → COMPLETED
End prime          reconcile (rows, per batch, other rows, duplicate keys, late delta plans), unlock,
                   delta row ON only if p_Enable_Delta and everything passed
on failure after Begin: Abort (unlock, delta row stays OFF) → run fails
```

---

## 2. Run group: DATA_PRIME, never SCMergeCO

- The initial rows always use **RUN_GROUP = 'DATA_PRIME'**:
  - Prepare sets it when it creates or converts a row;
  - Check pair refuses to run if the initial row has any other group.
- **SCMergeCO** is the group of the live daily delta. In this repo, trigger SCMergeCOSTOCKDB-DailyLoad runs DI_RDBMS_Shared_SQL_Server with `p_Run_Group = SCMergeCO` daily at 06:00 Sydney time. Other triggers use SCMergeCO or GRP:INCREMENTAL.
- **No trigger or master pipeline runs DATA_PRIME.** So nothing scheduled can pick up an initial row while a prime holds it ACTIVE, and the prime never runs inside the delta's group.
- The prime finds its rows **by table name and id, never by run group**.
- **The delta row keeps SCMergeCO.** That is how the daily delta continues after the prime. Do not move it.

Before each live prime, check that DATA_PRIME is still unused. Expect only initial rows (SOURCE_TYPE FILE, `…_INITIAL_LOAD`):

```sql
SELECT EXTRACT_META_ID, SOURCE_NAME, SOURCE_TYPE, STAGE_TABLE_NAME, ACTIVE_FLAG
FROM EDP_DEV.CONTROL.EXTRACT_META WHERE UPPER(RUN_GROUP) = 'DATA_PRIME' ORDER BY 1;
```

In ADF, check that no trigger passes DATA_PRIME (Manage → Triggers → each trigger's parameters). The house pipelines match run groups with CONTAINS, so also check that no trigger passes a fragment of it (DATA, PRIME) or a value ending in ALL.

---

## 3. One-time setup per factory

### 3.1 Choose the factory

| Factory | Pipelines from | Storage account | Landing container | ATILDA (staging) container | Status |
|---|---|---|---|---|---|
| **cwr-ase-edp-dev-adf** (old) | `adf_old_json/pipeline/` | cwraseedpdevst | `p_Landing_Container`, default `json-priming` | `p_Staging_Container`, default `snowflake-data` | **Works.** Test run 4 passed (with the earlier layout) |
| **cwr-ae-dev-data-adf** (new) | `adf_new_json/pipeline/` | cwraedevdata | `p_Landing_Container`, default `json-priming` | global `StagingContainer` (staging) | **Blocked** until checks G1–G5 below pass |

**Storage layout.** Every file is in two places:

```
cwraseedpdevst
├─ json-priming/                         the prime's landing (only ADF reads and writes it)
│   └─ <lowercase table>/                = the initial row's SOURCE_PATH
│       └─ <prefix>_part_<RunId>_b<k>_1.json
└─ snowflake-data/                       ATILDA's container (the only one Snowflake reads, through EDP_AZURE_SI_DEV)
    └─ <initial SOURCE_NAME>/<LVL2>/<META_ID>_<PLAN_NO>/…_json_<plan>_<ts>.json   written by "Copy to BLOB"
```

The landing folder can live anywhere ADF can reach; the staged copy must be in `snowflake-data`. This is the house pattern: DI_FILE_CSV_DFIO lands in the SFTP account and copies into `snowflake-data`. Landing files are disposable once a prime passes. Staged copies stay, as for every ATILDA feed.

**One-time container setup** (storage admin):

1. Create the container `json-priming` in the storage account (private, no public access).
2. Give the factory identity Storage Blob Data Contributor on it. If the role is already granted on the whole account, nothing is needed.
3. Optional: add a lifecycle rule that deletes blobs in `json-priming` older than 30 days.

No Snowflake or ATILDA change is needed.

New factory gate:

| Check | How | Pass when |
|---|---|---|
| G1 ATILDA can read the new storage | `SELECT * FROM EDP_DEV.CONTROL.STORAGE_DETAILS;` and `DESC STORAGE INTEGRATION EDP_AZURE_SI_DEV;` | Both include `azure://cwraedevdata.blob.core.windows.net/staging/`. On 2026-10-27 only cwraseedpdevst/snowflake-data was allowed. The ATILDA owners change this |
| G2 Storage rights | Azure portal → cwraedevdata → Access control (IAM) | The factory identity has Storage Blob Data Contributor |
| G3 Source database | `SourceConfig.<SOURCE_NAME>` in data-factory/factory/cwr-ae-dev-data-adf.json | It points at the database you intend |
| G4 Snowflake role | linked service cw_au_edp_snowflake_wh | Role DEV_ACCOUNT_ROLE_DATA_INGESTION |
| G5 Datasets | DS_Snowflake_Wh, DS_SQLServer_Shared, DS_Storage_Stage_JSON (Folder, File, Container; gzip), DS_STORAGE_V2_STAGE_BIN | They exist |

**Never prime the same table from both factories at once.** They share the ATILDA rows.

### 3.2 Load the pipelines into ADF

Load these four files, **children first**:

1. PL_ATILDA_DATA_PRIME_JSON_OUT
2. PL_ATILDA_DATA_PRIME_JSON_IN
3. PL_ATILDA_DATA_PRIME_JSON
4. PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST

For each one, in ADF Studio:

1. Go to **Author → Pipelines**. Open the pipeline if it exists; otherwise use **+ → Pipeline** and name it exactly as the file.
2. Click **{ } (Code)**, replace the whole content with the file's content, then click **OK**.
3. **Save** (git mode) or **Publish** (live mode) **before loading the next one.** A Debug run calls the *saved or published* children, not what is open in the editor.

Then:

- Run **Validate all**. It must show no errors.
- They appear under `ALTIDA/00 Templates/01 Extract/03 Data Prime JSON` (children in `/Child`).
- New factory (git-integrated): you can also copy the four files into `data-factory/pipeline/` on a feature branch and open that branch in ADF Studio.
- Reload all four whenever this repo changes them. The change log is in section 11.
- PL_ATILDA_DATA_PRIME_JSON_LOAD_FILE is no longer used (its steps are now inside _JSON_OUT). Delete it from the factory after loading the new _JSON_OUT.

### 3.3 Grants (Snowflake)

Run `sql/01_grants.sql`. The ADF role (DEV_ACCOUNT_ROLE_DATA_INGESTION) needs:

- CONTROL write through database role CONTROL_DEV_RW;
- USAGE on the ATILDA procedures;
- SELECT and TRUNCATE on each BRONZE table you will prime.

---

## 4. Table worksheet (copy for each table)

Fill this in during step A of section 5. Every later step uses these values.

| Item | Where it comes from | Value |
|---|---|---|
| `<T>` STAGE_TABLE_NAME | sql/02 A (the delta row) | |
| Delta row id, SOURCE_NAME, RUN_GROUP, ACTIVE_FLAG | sql/02 A | |
| Source object (schema.table) | delta row OBJECT_NAME | |
| Source database | ADF global SourceConfig[delta SOURCE_NAME] | |
| PRIMARY_KEYS | delta row (for the duplicate check) | |
| Watermark column `<wm>` | delta row SQL_SNIPPET (the column in `WHERE <wm> > …`) | |
| Partition column `<key>` | source table: integer, leads an index | |
| Initial row id and state | sql/02 A/B: none (Prepare creates it), Parquet (`LIVE_ROW_NEEDS_CONVERT`), or JSON (`LIVE_ROW_READY` / `LIVE_ROW_NEEDS_PREPARE`) | |
| Initial row SOURCE_PATH | Always `<lowercase t>/` (its folder in `json-priming`). Prepare sets it | |
| total_rows / null_wm / min_wm / max_wm | step A4 count | |
| p_Rows_Per_File | 1,000,000 by default. For big tables, raise it (up to about 3,000,000) to keep files at ≤ 100 | |
| enable_delta | `true` if the delta row is ACTIVE today; `false` if it is inactive on purpose | |
| Delta schedule to avoid | the trigger for the delta's SOURCE_NAME and RUN_GROUP (e.g. SCMergeCOSTOCKDB-DailyLoad, 06:00 Sydney time) | |

---

## 5. Procedure: prime one table

Snowflake queries run as a read role (OPS_DEV_RO) unless stated otherwise. Debug runs are started in ADF Studio:

1. Open PL_ATILDA_DATA_PRIME_JSON.
2. Click **Debug**, fill in the parameters and click **OK**.
3. Watch the run in **Monitor → Pipeline runs → Debug**. Open "Loop objects" to see each activity's output.

### A. Checks (read-only)

**A1. ATILDA rows.** Edit `SET stage_table` in `sql/02_onboard_initial_load.sql` and run it. **Save the output**: it is the backup of the rows before anything changes. Required:

- **Exactly one delta row** (SOURCE_TYPE TABLE) with:
  - SOURCE_NAME a SourceConfig key of a **SQL Server** source;
  - OBJECT_NAME `schema.table`;
  - PRIMARY_KEYS set;
  - SQL_SNIPPET with a BRONZE-MAX watermark (ATILDA guide, recipe A2).
- **FILE rows for `<T>`.** Section B of sql/02 shows the state:

  | FILE rows | What happens |
  |---|---|
  | None | Prepare creates the JSON initial row, with SOURCE_PATH `<t>/` |
  | One Parquet INITIAL_LOAD row (`LIVE_ROW_NEEDS_CONVERT`) | Prepare converts it |
  | One JSON INITIAL_LOAD row | Ready, or Prepare fixes it |
  | Anything else | Stop and ask the ATILDA owners |

- **Section C of sql/02:** the initial row's SOURCE_PATH is used once. If it is not `<t>/` yet, Prepare moves it (test rows automatically, the live row with `p_Prepare_Live_Row = true`).

**A2. Run group.** Run the query in section 2. DATA_PRIME must hold only initial rows.

**A3. Delta idle.** This must return no rows:

```sql
SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW
WHERE EXTRACT_META_ID = <delta id> AND EXTRACT_RUN_PLAN_STATUS = 'PLANNED';
```

Plan the live run outside the delta's schedule, so it finishes before the next run.

**A4. Size.** In SSMS, against the source database:

```sql
SELECT COUNT_BIG(*) AS total_rows, SUM(CASE WHEN <wm> IS NULL THEN 1 ELSE 0 END) AS null_wm,
       MIN(<wm>) AS min_wm, MAX(<wm>) AS max_wm
FROM <schema.table> WITH (NOLOCK);
```

- **Files** = total_rows / p_Rows_Per_File. Fewer files is faster, so aim for **≤ 100**: 1,000,000 rows per file by default, raised to about 3,000,000 for very large tables. The maximum is 4999.
- **Run time:** files load one at a time at about 2–5 minutes each, plus the extract. For example, 70 files ≈ 2.5–6 hours.
- **If null_wm > 0, stop.** Rows with a NULL watermark are not primed (windowed prime), so decide with the data owner first.

**A5. Dry run of the source query (optional).** In `sql/10_source_query_test.sql`:

1. Set `@obj`, `@wcol`, `@pcol` and `@n` (= p_Rows_Per_File), with `@ws = N''` and `@we = N''`.
2. Run it in SSMS. Expect `ERROR_MESSAGE = ''`.
3. Run the returned RANGES_SQL in a new window to see the batches.

### B. Test prime (into `<T>_PRIME_TEST`)

**B1. Debug** PL_ATILDA_DATA_PRIME_JSON:

| Parameter | Value |
|---|---|
| p_Objects | `[{"table":"<T>_PRIME_TEST","source_from":"<T>","window_column":"<wm>","window_start":"<yyyy-mm-dd, ~1 month ago>","window_end":"<yyyy-mm-dd>","partition_column":"<key>"}]` |
| p_Rows_Per_File | small enough to give **3–5 files** for that month (e.g. 50000) |
| everything else | default (`p_Prepare_Live_Row` false; it is not needed for a test) |

**B2. Expect:**

- **Prepare row:** `CREATED_TEST_ROW` the first time, `TEST_ROW_EXISTS` after that.
- **Check pair:** ERROR_MESSAGE empty and TEST_MODE true.
- **End prime:**
  - STATUS OK;
  - ROWS_COPIED = BRONZE_ROWS;
  - BATCH_MISMATCH empty, DUP_KEYS 0;
  - MESSAGE "Test prime: … Delta twin … untouched".

**B3. Data checks.** Use a role that can read the test table (e.g. DEV_ACCOUNT_ROLE_DATA_INGESTION).

- **Columns.** This must return no rows:
  ```sql
  SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = '<T>'
  MINUS
  SELECT COLUMN_NAME FROM EDP_DEV.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = '<T>_PRIME_TEST';
  ```
- **Text.** Pick one key value and compare the row in `<T>` and in `<T>_PRIME_TEST`: dates, decimals, floats, bits.
- **Plans.** In `sql/03_monitor.sql` (stage_table = `<T>_PRIME_TEST`, run_id = the RunId), section B shows every plan COMPLETED and BRONZE rows per batch matching.

**B4. Clean up.** Run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST with `p_Tables = ["<T>_PRIME_TEST"]`.

- It drops the test table and deletes the test landing files (`json-priming/<t>_prime_test/*.json`).
- Keep `p_Delete_Test_Row = false` to re-test later. Set it to `true` to also remove the test row and its auto LOAD_META row.

### C. Before the live prime

**C1. Backup and baseline** of the live table:

```sql
SELECT COUNT(*), MAX(<WM>) FROM EDP_DEV.BRONZE.<T>;
CREATE TABLE <scratch schema>.<T>_BKP_<yyyymmdd> CLONE EDP_DEV.BRONZE.<T>;   -- zero-copy, if your role may
```

**C2. Auto LOAD_META** for the initial row. Expect one APPEND row with JOB_GROUP_SEQ 0, or none (ATILDA then creates it on the first load). **Do not edit it** (ATILDA guide section 1).

```sql
SELECT LOAD_META_ID, JOB_GROUP, JOB_ACTION, TARGET_PRIMARY_KEY, JOB_GROUP_SEQ
FROM EDP_DEV.CONTROL.LOAD_META WHERE TARGET_TABLE_NAME = '<T>' ORDER BY 1;
```

**C3. Live dry run (changes nothing).** Debug with the D1 parameters, with two changes:

- p_Objects = `[{"table":"<T>","window_column":"<wm>","partition_column":"<key>","window_start":"2999-01-02","window_end":"2999-01-01"}]`. The impossible window (start after end) makes the run stop at *Check source columns* at the latest. It **never reaches Begin**, whatever state the row is in.
- `p_Prepare_Live_Row = false`.

**Expected result** depends on the initial row's state (A1):

  | Initial row state | Result |
  |---|---|
  | Parquet row | Fails at *Check prepare* with ACTION `LIVE_ROW_NEEDS_CONVERT` |
  | JSON row that needs fixing | Fails with `LIVE_ROW_NEEDS_PREPARE` |
  | No initial row | Fails with `LIVE_ROW_MISSING` |
  | Already `LIVE_ROW_READY` | Prepare and Check pair pass, then it fails at *Check source columns* (PRIME_SOURCE_INVALID, "Window start must be before window end"; for an integer watermark, "Window bound 2999-01-02 is not an integer"). BRONZE and the delta row are untouched |

For the first three states, Prepare row's MESSAGE lists exactly what Prepare would change. **Remove the two window keys before D1.**

### D. Live prime

**D1. Debug** (or Add trigger → Trigger now) PL_ATILDA_DATA_PRIME_JSON:

| Parameter | Value | Why |
|---|---|---|
| p_Objects | `[{"table":"<T>","window_column":"<wm>","partition_column":"<key>"}]` (add `"enable_delta":"false"` if the worksheet says so) | No `source_from`, so this is the live table |
| p_Window_Start | `""`, and **no** `window_start` in p_Objects | **Full history.** Begin empties the table; a start date would permanently drop every older row. Clear any value left over from a test |
| p_Window_End | `""` | For a date or datetime watermark (recipe A2), this defaults to the source server's clock at extract start: the delta's handover point. **For an integer watermark there is no automatic end:** set `"window_end"` in p_Objects to the current `MAX(<wm>) + 1` |
| p_Rows_Per_File | from the worksheet | |
| p_Enable_Delta | `true` | The delta row goes back ON after a pass |
| p_Prepare_Live_Row | `true` | Lets Prepare create, convert or fix the initial row (once) |
| p_Force | `false` | `true` only after a cancelled run (section 5 F) |
| p_Batch_ID | e.g. `PRIME_LIVE_<T>_<yyyymmdd>` | Logged on every ATILDA plan |
| p_Landing_Container | `json-priming` | The landing container (section 3.1) |
| p_Staging_Container | `snowflake-data` | Old factory only: ATILDA's container |

**D2. Expect.** The "Must show" column below is the full list of outputs to check.

| Activity | Must show |
|---|---|
| Prepare row | STATUS OK. ACTION `LIVE_ROW_CONVERTED`, `CREATED_LIVE_ROW`, `LIVE_ROW_PREPARED` or `LIVE_ROW_READY`. OLD_VALUES = the row before the change |
| Check pair | ERROR_MESSAGE empty, TEST_MODE false, INITIAL_META_ID and DELTA_META_ID as in the worksheet |
| Get source columns | ERROR_MESSAGE empty. WINDOW_FILTER has only the end bound, `AND [<wm>] < CONVERT(<type>, '<server clock>', 126)`, or `< <window_end>` for an integer watermark |
| Get batch ranges | One row per batch (≈ total_rows / p_Rows_Per_File), plus batch 0 if the key has NULLs |
| Begin prime | STATUS OK, "…delta twin <id> is OFF". **From here the live table is empty until the load finishes** |
| Data in (child run) | One `<prefix>_part_<RunId>_b<k>_1.json` per batch in `json-priming/<t>/`. Output `batch_rows` |
| Data out (child run) | Plans = files, then "Load files" runs them one at a time: each copies its file to `snowflake-data/<SOURCE_NAME>/…` and loads it |
| End prime | STATUS OK; ROWS_COPIED = BRONZE_ROWS; OTHER_ROWS 0; OPEN_PLANS 0; DUP_KEYS 0; BATCH_MISMATCH empty; **DELTA_RESTORED true** (false, with STATUS OK, when enable_delta is false) |

**D3. Save the outputs.** Copy **Prepare row → OLD_VALUES** and **Get source columns → WINDOW_FILTER** (the handover time) somewhere safe.

### E. After the live prime

1. **sql/03_monitor.sql** (set stage_table = `<T>`, run_id = RunId, wm_col = `<WM>` in upper case):
   - A: initial row ACTIVE FALSE, delta row ACTIVE TRUE (unless enable_delta was false).
   - B: all plans COMPLETED, and rows per batch match.
   - The MAX watermark is just below the WINDOW_FILTER end time (integer watermark: compare `MAX(<wm>)` with `window_end` instead).
2. **Row count** ≈ total_rows from A4. Rows written after the end time arrive with the next delta.
3. **Next delta run.** After its scheduled time, check:
   ```sql
   SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = <delta id>
   ORDER BY EXTRACT_RUN_PLAN_ID DESC LIMIT 5;   -- latest plan COMPLETED
   ```
4. **Drop the backup clone** once you are satisfied.
5. **Leave the initial row** as it is (inactive, DATA_PRIME). It is only used by the next prime.

### F. If something fails

Open the failed activity's error icon. For Data in or Data out, open the child pipeline run in Monitor and read its failed activity.

| Situation | State | What to do |
|---|---|---|
| Fails before Begin prime | Nothing changed (except Prepare's row setup, if `p_Prepare_Live_Row` was true) | Fix the cause and re-run |
| Fails after Begin, or End STATUS ERROR | Lock released. **Delta row OFF** (live). BRONZE empty or partial | Fix the cause and re-run the **whole** prime. Restore the clone if the data is needed meanwhile |
| End STATUS ERROR "left OFF … watermark not usable" | Data loaded and reconciled, delta row OFF | The delta's watermark column has values Snowflake cannot parse. Fix it at the source, or decide with the ATILDA owners. Re-enabling the delta without a re-prime is the one manual exception: only a CONTROL_DEV_RW holder, with sign-off (sql/03 C) |
| One file fails to load | The remaining files still load, then Abort runs | In the _JSON_OUT run, open the failed "Load files" iteration, fix the cause and re-run |
| Data in fails at "Check landing folder" or the extract with an authorization or container error | Begin has run: BRONZE truncated, delta row OFF | Check the `json-priming` container exists and the factory identity can write to it (section 3.1), then re-run |
| The Begin prime activity itself fails (timeout or connection) | Begin may have committed (initial row ACTIVE, delta row OFF). Abort does not run | Check sql/03 A. If the initial row is ACTIVE, re-run with `p_Force = true` |
| Run cancelled | Initial row ACTIVE (lock), delta row OFF (live) | Check in Monitor that nothing is running, then re-run with `p_Force = true` |
| "… is ACTIVE: another prime is running, or one was cancelled" | Lock held | As above: confirm, then `p_Force = true` |
| "Delta twin … has open (PLANNED) plan(s)" | A delta run is in progress | Wait for it to finish, then re-run |

**Never** fix ATILDA rows by hand. Re-running the prime is the recovery. To undo a Prepare conversion, a CONTROL_DEV_RW holder restores OLD_VALUES, with sign-off.

### G. Several tables in one run

List them all in `p_Objects`. Each needs its own worksheet and its own passed test.

- They run 2 at a time.
- Each table may appear once.
- `p_Force` applies to all of them, so after a cancel re-run only the cancelled table.

---

## 6. Reference: parameters and outputs

### PL_ATILDA_DATA_PRIME_JSON

| Parameter | Default | Meaning |
|---|---|---|
| p_Objects | `[]` | One element per table (keys below). Empty fails with PRIME_NO_OBJECTS, so an accidental trigger does nothing |
| p_Window_Start | `""` | Inclusive lower bound for every object. `""` = none (full history) |
| p_Window_End | `""` | Exclusive upper bound. `""` = the source server's clock (date windows) |
| p_Rows_Per_File | 1000000 | Rows per JSON file = rows per ATILDA plan. Minimum 1000 |
| p_Enable_Delta | true | Live: turn the delta row ON after a pass |
| p_Prepare_Live_Row | false | Live: allow Prepare to create, convert or fix the initial row |
| p_Force | false | Take over a lock left by a cancelled run |
| p_Batch_ID | `""` | Logged on each ATILDA plan |
| p_Landing_Container | json-priming | The prime's landing container (all factories) |
| p_Staging_Container | snowflake-data | Old factory only: ATILDA's container, where "Copy to BLOB" writes |

`p_Objects` element keys:

| Key | Required | Meaning |
|---|---|---|
| table | yes | STAGE_TABLE_NAME (`<T>`, or `<T>_PRIME_TEST` for a test) |
| source_from | test only | `<T>`. Required with a `_PRIME_TEST` table; never set for live |
| window_column | recommended | The delta's watermark column (date, datetime, datetime2, smalldatetime or integer) |
| window_start / window_end | no | Override p_Window_Start / p_Window_End. Dates as `yyyy-mm-dd` or `yyyy-mm-ddThh:mi:ss`, with no offset or Z |
| partition_column | recommended | An integer column that leads an index. Without it, the whole table is one file in one stream |
| enable_delta | no | `"true"` / `"false"`; overrides p_Enable_Delta |

### PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST

| Parameter | Default | Meaning |
|---|---|---|
| p_Tables | `[]` | `["<T>_PRIME_TEST", …]`. Only `*_PRIME_TEST` names whose rows are inactive test rows are accepted |
| p_Delete_Test_Row | false | Also delete the test initial row and its auto LOAD_META row |

### Prepare row ACTION values

| ACTION | Meaning |
|---|---|
| CREATED_TEST_ROW / TEST_ROW_EXISTS | Test row created / already there (its SOURCE_PATH is moved to `<t>/` if needed) |
| LIVE_ROW_READY | The initial row is already JSON, SQL_STATEMENT NULL, DATA_PRIME, SOURCE_PATH `<t>/` |
| LIVE_ROW_NEEDS_PREPARE | It needs SQL_STATEMENT NULL / RUN_GROUP DATA_PRIME / SOURCE_PATH `<t>/`; re-run with p_Prepare_Live_Row = true |
| LIVE_ROW_PREPARED | Those columns were fixed (OLD_VALUES has the previous values) |
| LIVE_ROW_NEEDS_CONVERT | It is a Parquet row; re-run with p_Prepare_Live_Row = true |
| LIVE_ROW_CONVERTED | Converted to JSON (OLD_VALUES has the previous values) |
| LIVE_ROW_MISSING / CREATED_LIVE_ROW | No initial row / one created, with SOURCE_PATH `<t>/` |

### Error codes (ADF Fail activities)

| Code | Raised by |
|---|---|
| PRIME_NO_OBJECTS | Empty p_Objects |
| PRIME_DUPLICATE_OBJECTS | The same table listed twice |
| PRIME_PREPARE_FAILED | Check prepare |
| PRIME_PAIR_INVALID | Check pair result |
| PRIME_SOURCE_INVALID | Check source columns |
| PRIME_NO_ROWS | Check batch ranges |
| PRIME_BEGIN_FAILED | Check begin |
| PRIME_TOO_MANY_FILES | More than 5000 files |
| PRIME_PLAN_COUNT_MISMATCH | Plans ≠ files |
| PRIME_STAGE_COPY_FAILED | A file's copy to the ATILDA path |
| PRIME_RECONCILE_FAILED | Check end |
| PRIME_FAILED | The summary after Abort |
| PRIME_NO_TABLES | Cleanup pipeline: empty p_Tables |
| PRIME_CLEANUP_FAILED | Cleanup pipeline: Check cleanup |

---

## 7. Reference: what changes in ATILDA (CONTROL)

Nothing is changed by hand. All writes run inside the pipeline as the ADF role.

| Object | Test prime | Live prime |
|---|---|---|
| Delta row (`SOURCE_TYPE TABLE`) | Never touched | ACTIVE_FLAG FALSE from Begin; TRUE after a pass (p_Enable_Delta). Nothing else changes; its RUN_GROUP stays (e.g. SCMergeCO) |
| Initial row (`FILE`, `…_INITIAL_LOAD`) | A separate test row `<T>_PRIME_TEST`, created once with SOURCE_PATH `<t>_prime_test/`. An existing test row is moved to that path automatically | Once, with p_Prepare_Live_Row. **Created or converted from Parquet** to: OBJECT_NAME `<t>_part_%.json`, STAGE_FILE_TYPE/SPEC NULL, SCHEMA_DRIFT_CODE = the delta's (2), SQL_STATEMENT NULL, **RUN_GROUP DATA_PRIME**, **SOURCE_PATH `<t>/`**; a converted row keeps its id, SOURCE_NAME, LVL2 and PRIMARY_KEYS. **An existing JSON row is only fixed** in SQL_STATEMENT, RUN_GROUP and SOURCE_PATH. ACTIVE TRUE only while running |
| EXTRACT_RUN_PLAN / logs | One plan per file. Old open plans of the row are abandoned | Same |
| LOAD_META (auto, JOB_GROUP_SEQ 0) | Created by ATILDA for the test table; removed by cleanup with p_Delete_Test_Row | Used as is; created by ATILDA if missing |
| BRONZE | `<T>_PRIME_TEST` created and filled; dropped by cleanup | `<T>` truncated, then loaded. The typed `<T>_VW` stays (the delta row maintains it) |
| STORAGE_DETAILS / EDP_AZURE_SI_DEV | — | — (new factory: ATILDA owners, gate G1) |

---

## 8. Worked example: DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY

### Worksheet

| Item | Value |
|---|---|
| `<T>` | DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY |
| Delta row | **156**, SOURCE_NAME STOCKDB, RUN_GROUP **SCMergeCO**, ACTIVE TRUE |
| Source object / database | dbo.ProductNetworkCostsHistory / SourceConfig['STOCKDB'] |
| PRIMARY_KEYS | HistoryID |
| `<wm>` | RefreshDatetime (delta snippet: `WHERE RefreshDatetime > MAX(REFRESHDATETIME)` from BRONZE) |
| `<key>` | HistoryID |
| Initial row | **768**, Parquet (`productnetworkcostshistory_part_%.parquet`, SQL_STATEMENT 1=1, RUN_GROUP INITIAL), so `LIVE_ROW_NEEDS_CONVERT` |
| SOURCE_PATH | Now `PDB08/stockdb/dbo.productnetworkcostshistory/`; the conversion moves it to `db_stockdb_productnetworkcostshistory/`, so files land in `json-priming/db_stockdb_productnetworkcostshistory/` |
| Test row | **790** (`DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST`). Its SOURCE_PATH `data_prime_test/…` is moved to `db_stockdb_productnetworkcostshistory_prime_test/` on the next test run, so files land in `json-priming/db_stockdb_productnetworkcostshistory_prime_test/` |
| total_rows | from A4 (BRONZE held about 215M rows in Oct 2026) |
| p_Rows_Per_File | e.g. 3000000 for about 215M rows (≈ 72 files, ≈ 2.5–6 hours of loading) |
| enable_delta | true |
| Delta schedule to avoid | SCMergeCOSTOCKDB-DailyLoad, 06:00 Sydney time daily (as defined in this repo for the new factory; check the old factory's trigger in ADF → Manage → Triggers) |

### Status (2026-10-27)

- Test run 4 passed in the old factory: 163,204 rows, one file.
- A longer test (2024-07-01 to 2026-10-26, 50,000 rows per file) was run with the earlier build, which loaded files 4 at a time.
- **Run section 5 B again with the current build before going live.** The one-at-a-time load and the `json-priming` landing container are new.
- The old landing files under `snowflake-data/data_prime_test/` are no longer used and can be deleted by hand.

### Exact parameters

- **Test (B1):**
  - `p_Objects = [{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST","source_from":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","window_start":"2026-09-26","window_end":"2026-10-26","partition_column":"HistoryID"}]`
  - `p_Rows_Per_File = 50000`. That gives about 4 files.
- **Cleanup (B4):** `p_Tables = ["DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST"]`.
- **Live dry run (C3):** `p_Objects = [{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID","window_start":"2999-01-02","window_end":"2999-01-01"}]`, `p_Prepare_Live_Row = false`. Expect `LIVE_ROW_NEEDS_CONVERT`.
- **Live (D1):**
  - `p_Objects = [{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID"}]`
  - `p_Window_Start = ""`, `p_Window_End = ""`, `p_Prepare_Live_Row = true`, `p_Enable_Delta = true`.
  - `p_Rows_Per_File` from the worksheet; `p_Batch_ID = PRIME_LIVE_PNCH_<yyyymmdd>`.
  - Start well after the delta's daily run (06:00 Sydney time), so it finishes before the next one.
- **Expected Prepare (D2):** `LIVE_ROW_CONVERTED`. 768 becomes `productnetworkcostshistory_part_%.json`, JSON, SCHEMA_DRIFT_CODE 2, SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME, SOURCE_PATH `db_stockdb_productnetworkcostshistory/`.

### Other tables ready to prime

The ATILDA guide (section 10.3) lists more Parquet initial rows that were created but never run:

- 769 PRODUCTPRICINGMATRIX
- 770 CATALOGUEITEMS
- 773 SKU_SKUDETAILS
- 779 CURRENTSOH

Prepare converts each of them the same way. Start each with section 5 A.

---

## 9. Alignment with the ATILDA guide

**Followed:**

- **Pairing (sections 3 and 10).** The initial row shares STAGE_TABLE_NAME (and LVL2) with the delta row, and every FILE OBJECT_NAME contains `%`.
- **New rows (section 3, checklist item 8).** ACTIVE_FLAG starts FALSE. Nothing writes TRAN_WATERMARK, IDs or audit columns. SQL_COL_LIST stays NULL on initial rows, because the delta row maintains the typed view.
- **Cutover (section 10.2).** The delta is inactive from the truncate until the load passes. The window end is fixed once, so the delta continues from BRONZE MAX with no gap.
- **File loads.** One plan per file through PR_CREATE_EXTRACT_RUN_PLAN_FILE, loaded **one at a time** in a single ForEach: copy → source_rows_read → PR_CREATE_EXTRACT_ARTIFACTS → extract_run_plan_status COMPLETED. This is the per-plan loop of the house file pipelines (DI_FILE_INITIAL_Shared_Parquet, DI_FILE_CSV_DFIO, TPL_E_FILE_PARQUET). Those cannot be called directly: TPL_E_FILE_PARQUET has no parameters, DI_FILE_CSV_DFIO is bound to the DFIO SFTP account and CSV, and DI_FILE_INITIAL_Shared_Parquet uses Parquet datasets and ends with PR_BULKLOAD_PARQUET.
- **Landing outside the ATILDA container.** Files are extracted to `json-priming` and copied into `snowflake-data` per plan, as DI_FILE_CSV_DFIO does from the SFTP account.
- **Repeat priming (section 3).** File names are unique per run (RunId), so RELOAD_SAME_FILE_FLAG stays FALSE. BRONZE is cleared explicitly (checklist item 10).
- **LOAD_META (section 1).** It is auto-generated and never hand-written.

**Deliberate differences:**

| ATILDA guide | Here | Why |
|---|---|---|
| Recipe B / section 10.3: initial rows in RUN_GROUP INITIAL, run through the DLA master pipeline. Section 3 ("re-use an existing tag") and checklist item 7 ("a tag the master pipeline runs") | RUN_GROUP **DATA_PRIME**; the ATILDA procedures are called directly | No trigger runs DATA_PRIME, so nothing else picks up a row the prime holds ACTIVE, and the delta's SCMergeCO is never used (section 2) |
| Recipe B: STAGE_FILE_TYPE PARQUET, SCHEMA_DRIFT_CODE 0, SQL_STATEMENT 1=1 | JSON (STAGE_FILE_TYPE/SPEC NULL), SCHEMA_DRIFT_CODE = the delta's (2), SQL_STATEMENT NULL | JSON is the RDBMS convention and needs no Java on the SHIR. 1=1 is not applied to FILE rows anyway (section 3); Begin truncates instead |
| Section 2: back up to EXTRACT_META_BKP_*; OPS_DEV writes | Prepare returns OLD_VALUES in its output; all DML runs as the ADF role (CONTROL_DEV_RW) | No new objects in CONTROL. OPS_DEV cannot write CONTROL in DEV |
| Section 8: abandon a plan with etlplan_status | Begin also logs extract_run_plan_status = ABANDONED | EXTRACT_RUN_PLAN_VW keeps a plan open until that status changes |
| Section 10.3 steps 4/10: reconcile against a separate source COUNT | End reconciles against the rows ADF extracted per batch | Catches every lost or duplicated file per batch. A separate NOLOCK count of a live source drifts |
| Section 10.3 step 11: restore the delta to its earlier flag | The delta ends ON when p_Enable_Delta is true and the checks pass | After a failed prime the delta is OFF, so "earlier flag" would keep it OFF for good. Use `"enable_delta":"false"` for deltas that are inactive on purpose |
| PARALLEL_STAGE_LOAD_FLAG | Left at its value (new rows FALSE, the default) | Files load one at a time |
| House pipelines log `dataRead` (bytes) as source_rows_read | Logs `rowsRead` (rows) | It is a row count |
| Section 10.3 HistoryMonths (partial history with ClearTarget) | A live prime loads the full history only | Truncating with a start date drops older rows for good. This is a procedure rule (section 5 D1); the code does not enforce it |
| Section 10.3 DryRun parameter | No DryRun parameter. The live dry run (section 5 C3) uses an impossible window and stops before Begin | No extra parameter or code path |
| Section 10.3 step 10: bad_ts = 0 and max_wm ≤ Cutoff | End runs the delta's own snippet and requires its MAX watermark ≥ 1901-01-01. Single unparseable values are not counted | The delta's next run depends only on that MAX |

---

## 10. FAQ

**Can I run only "Data out" (for example to reuse the test files for the live table)?**
No. _JSON_OUT is a child pipeline. You could start it by hand, but it is not safe on its own:

- **It only finds the files you point it at:** `<SOURCE_PATH>/<prefix>_part_<RunId>_*.json`, planned on one initial row.
  - Test files sit in the test row's folder, carry the test RunId and cover only the test window.
  - Pointed at the live row, it finds nothing.
- **It cannot load into an unconverted row.** If the live row is still Parquet (`…_part_%.parquet`), none of the .json files match, so ATILDA plans nothing.
- **It skips the safety steps.** Without Begin, nothing locks the row, turns the delta OFF or truncates BRONZE: the files would append onto the delta's rows while the delta keeps running. Without End, nothing reconciles or turns the delta back ON.
- **End needs Data in's per-batch row counts.**

If a live run fails partway, re-run the whole prime. Begin abandons the failed attempt's plans.

**Why JSON and not Parquet?** Writing Parquet on the SHIR needs 64-bit Java, which is not installed. JSON is the path the delta already uses, and BRONZE text matches the delta's exactly. Performance is about the same: the per-file ATILDA work dominates, and that is the same for both formats.

**How long does it take?** Roughly the extract time (minutes per few million rows) plus files × 2–5 minutes. Keep files to ≤ 100 with p_Rows_Per_File.

**Does a prime change the delta's schedule or group?** No. Only the delta row's ACTIVE_FLAG changes during a live prime.

**Can I prime only part of the history?** Only in test mode. A live prime truncates the table, so any window start loses older rows permanently (section 5 D1).

---

## 11. Files, build and change log

| Path | What it is |
|---|---|
| adf_old_json/pipeline/ | The 4 pipelines for cwr-ase-edp-dev-adf |
| adf_new_json/pipeline/ | The 4 pipelines for cwr-ae-dev-data-adf |
| tools/build_pipelines.py | Generates both builds and sql/10. **Never hand-edit the pipeline JSON** |
| tools/source_query.tsql | Get source columns (T-SQL) |
| tools/sf_prepare.sql, sf_check_pair.sql, sf_begin.sql, sf_end.sql, sf_cleanup_test.sql | The Snowflake blocks (EXECUTE IMMEDIATE) used by the Lookups |
| tools/check.py | Static checker for the generated JSON |
| sql/01_grants.sql | Read-only grant checks |
| sql/02_onboard_initial_load.sql | Read-only: a table's ATILDA rows and what Prepare would do |
| sql/03_monitor.sql | Read-only: pair state, plans, rows per batch, handover watermark |
| sql/10_source_query_test.sql | Generated SSMS dry run of Get source columns |

Regenerate after any change to tools/ (run from `parquet_ingestion/`), then reload the changed pipelines (section 3.2):

```
python3 tools/build_pipelines.py . old && python3 tools/check.py adf_old_json/pipeline   # cwr-ase-edp-dev-adf
python3 tools/build_pipelines.py . new && python3 tools/check.py adf_new_json/pipeline   # cwr-ae-dev-data-adf
```

### Change log (2026-10-27)

| # | Change |
|---|---|
| 1 | Blank p_Batch_ID handled (`@empty`) |
| 2 | Copy to the ATILDA path reads SOURCE_PATH/OBJECT_NAME and writes LVL1_FOLDER_NAME/FILE_NAME |
| 3 | Begin closes old plans with extract_run_plan_status = ABANDONED |
| 4 | Prepare converts a Parquet initial row to JSON (opt-in, OLD_VALUES returned) |
| 5 | JSON build for the new factory (adf_new_json) |
| 6 | Parquet build retired; one SQL set |
| 7 | Default p_Rows_Per_File 1,000,000 |
| 8 | Files load one at a time (house pattern); PARALLEL_STAGE_LOAD_FLAG no longer required |
| 9 | Live End returns ERROR if the delta cannot be turned back on |
| 10 | Run stops before Begin when there are no rows to load |
| 11 | source_rows_read logged per plan |
| 12 | Unused parameters removed; leftover template tokens fail the build |
| 13 | Landing moved to its own container `json-priming` (p_Landing_Container); SOURCE_PATH is always `<lowercase table>/`. ATILDA staging stays in `snowflake-data` |
| 14 | PL_ATILDA_DATA_PRIME_JSON_LOAD_FILE merged into _JSON_OUT ("Load files" ForEach, house shape): 4 pipelines instead of 5 |

### Open items

- **A. New factory gate** (section 3.1, G1–G5).
- **B. Copy to the ATILDA path re-serialises JSON** (JSON to JSON). Section 5 B3 checks the text. If it differs, switch to a byte copy on a new uncompressed Binary dataset with a File parameter.
- **C. First live load into an existing, truncated BRONZE table.** Only new-table loads have run so far. End's reconcile is the safety net.
- **D. Staged copies are never deleted.** They accumulate under the initial row's SOURCE_NAME folder, as for the house pipelines.
- **E. Batch row counts are collected with AppendVariable in a parallel ForEach**, which ADF does not guarantee is thread-safe. A lost count fails safe (End reports a batch mismatch). Section 5 B, with 3–5 files, exercises it.
