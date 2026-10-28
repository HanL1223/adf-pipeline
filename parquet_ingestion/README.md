# Data priming: SQL Server → JSON → ATILDA → BRONZE

**What it does.** It loads a table's full history ("primes" it) from on-prem SQL Server into Snowflake `EDP_DEV.BRONZE` through the ATILDA framework (`EDP_DEV.CONTROL`). It then hands the table back to its normal daily delta, with no gap and no overlap. One ADF pipeline, **PL_ATILDA_DATA_PRIME_JSON**, does everything. You give it the table name and a few column names.

**How to use this file.** Read sections 1–3 once. For each table you want to prime, copy the worksheet in section 4, fill it in, and follow section 5 step by step. Sections 6–7 are reference. Section 8 is the plan for the three tables being primed now (PNCH, ILS LOCATION_INVENTORY, SCAX2012 INVENTSUM), with every difference between them called out.

**Framework rules.** These follow `src/cw_batch_priming/Altida_Skill.md.docx` (the "ATILDA guide"). Where this solution deliberately differs, section 9 says so and why.

Status: 2026-10-28. This file is the only current guide (adf_new_json/README.md only notes the new-factory build). The superseded README.md.docx, handover.md.docx and adf_old_json/README.md.docx were removed (they remain in git history). DESIGN.md.docx still holds the design history.

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
- [8. Current batch: PNCH, ILS LOCATION_INVENTORY, SCAX2012 INVENTSUM](#8-current-batch-pnch-ils-location_inventory-scax2012-inventsum)
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
Check landing      read probe of json-priming/<t>/ (no read access stops here)
── nothing has changed in BRONZE or the delta row up to this point ──
Begin prime        lock (initial row ACTIVE), close old plans, delta row OFF, TRUNCATE BRONZE
Data in            clear old part files in json-priming/<t>/, then SQL Server ─SHIR─> one gzip JSON file per batch
                   (4 at a time) into json-priming/<t>/
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

In ADF, check that no trigger passes DATA_PRIME (Manage → Triggers → each trigger's parameters). The house pipelines match run groups with CONTAINS, so also check that no trigger passes a fragment of it (DATA, PRIME) or a value ending in ALL. Check **both** factories: they share the ATILDA rows, and only the new factory's triggers are in this repo.

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
| Delta trigger and schedule | a **started** trigger, in the factory the delta runs from, that starts DI_RDBMS_Shared_SQL_Server **or** E_RDBMS_Shared_SQL_Server with `p_Source_Name` = the delta's SOURCE_NAME exactly and a `p_Run_Group` (without its `GRP:` prefix) contained in the delta's RUN_GROUP, or ending in ALL (ADF → Manage → Triggers, in both factories). E.g. SCMergeCOSTOCKDB-DailyLoad, 06:00 Sydney time. It keeps the table current after the prime. Avoid running the prime across it. **If no factory has one, nothing loads the table after the prime**: get a trigger added before the live prime | |

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

- **Section C of sql/02:** only this table's own initial and test rows may use the paths `<t>/` and `<t>_prime_test/`. If the initial row's SOURCE_PATH is not `<t>/` yet, Prepare moves it (test rows automatically, the live row with `p_Prepare_Live_Row = true`).
- **ACTIVE_FLAG** of the initial and test rows is FALSE. If one was left TRUE by a cancelled run, check ADF Monitor that nothing is running and use `p_Force = true` on the next run (Prepare then moves or converts it anyway).

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

**C2. Auto LOAD_META.** The query returns the delta row's own auto LOAD_META (JOB_GROUP `EXTRACT_RUN_PLAN_<delta SOURCE_NAME>_<T>`). It may also return one APPEND row, JOB_GROUP_SEQ 0, whose JOB_GROUP contains the initial row's SOURCE_NAME. If that second row is missing, ATILDA creates it on the first load. **Do not edit either** (ATILDA guide section 1).

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
| p_Window_Start | `""`, and **no** `window_start` in p_Objects, for full history | Begin empties the table, so BRONZE ends up holding only the window. Set `"window_start"` in p_Objects (e.g. `"2024-07-01"`) only if you **deliberately** want BRONZE to start there: older rows are then gone from BRONZE, and the delta never back-fills them. Clear any value left over from a test |
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
| Check pair | ERROR_MESSAGE empty, TEST_MODE false, DELTA_META_ID as in the worksheet, INITIAL_META_ID = the existing initial row, or the id Prepare just created (ILS, INVENTSUM) |
| Get source columns | ERROR_MESSAGE empty. WINDOW_FILTER has only the end bound, `AND [<wm>] < CONVERT(<type>, '<server clock>', 126)`, or `< <window_end>` for an integer watermark |
| Get batch ranges | One row per batch (≈ total_rows / p_Rows_Per_File), plus batch 0 if the key has NULLs |
| Begin prime | STATUS OK, "…delta twin <id> is OFF". **From here the live table is empty until the load finishes** |
| Data in (child run) | One `<prefix>_part_<RunId>_b<k>_1.json` per batch in `json-priming/<t>/`. Output `batch_rows` |
| Data out (child run) | Plans = files, then "Load files" runs them one at a time: each copies its file to `<SOURCE_NAME>/…` in ATILDA's container (`snowflake-data` in the old factory) and loads it |
| End prime | STATUS OK; ROWS_COPIED = BRONZE_ROWS; OTHER_ROWS 0; OPEN_PLANS 0; DUP_KEYS 0; BATCH_MISMATCH empty; **DELTA_RESTORED true** (false, with STATUS OK, when enable_delta is false) |

**D3. Save the outputs.** Copy **Prepare row → OLD_VALUES** and **Get source columns → WINDOW_FILTER** (the handover time) somewhere safe.

### E. After the live prime

1. **sql/03_monitor.sql** (set stage_table = `<T>`, run_id = RunId, wm_col = `<WM>` in upper case):
   - A: initial row ACTIVE FALSE, delta row ACTIVE TRUE (unless enable_delta was false).
   - B: all plans COMPLETED, and rows per batch match.
   - The MAX watermark is just below the WINDOW_FILTER end time (integer watermark: compare `MAX(<wm>)` with `window_end` instead).
2. **Row count** ≈ total_rows from A4. Rows written after the end time arrive with the next delta.
3. **Next delta run.** You do not run anything for this. The house delta pipeline (DI_RDBMS_Shared_SQL_Server or E_RDBMS_Shared_SQL_Server, started by the trigger in the worksheet) plans every **active** delta row, and loads only rows with `<wm> > MAX(<wm>)` in BRONZE. After its scheduled time, check:
   ```sql
   SELECT * FROM EDP_DEV.CONTROL.EXTRACT_RUN_PLAN_STATUS_VW WHERE EXTRACT_META_ID = <delta id>
   ORDER BY EXTRACT_RUN_PLAN_ID DESC LIMIT 5;   -- latest plan COMPLETED
   ```
4. **Drop the backup clone** once you are satisfied.
5. **Leave the initial row** as it is (inactive, DATA_PRIME). It is only used by the next prime.
6. **Do not re-prime to stay current.** The delta keeps BRONZE up to date from here (FAQ, "How do I keep the gap small until deployment, without re-priming?"). Re-prime only for a clean full reload.

### F. If something fails

Open the failed activity's error icon. For Data in or Data out, open the child pipeline run in Monitor and read its failed activity.

| Situation | State | What to do |
|---|---|---|
| Fails before Begin prime | Nothing changed (except Prepare's row setup, if `p_Prepare_Live_Row` was true) | Fix the cause and re-run |
| Fails after Begin, or End STATUS ERROR | Lock released. **Delta row OFF** (live). BRONZE empty or partial | Fix the cause and re-run the **whole** prime. Restore the clone if the data is needed meanwhile |
| End STATUS ERROR "left OFF … watermark not usable" | Data loaded and reconciled, delta row OFF | The delta's watermark column has values Snowflake cannot parse. Fix it at the source, or decide with the ATILDA owners. Re-enabling the delta without a re-prime is allowed only as a sign-off exception. The two sign-off exceptions (a CONTROL_DEV_RW holder, sql/03 C) are re-enabling the delta after a passed prime, and restoring a Prepare row from OLD_VALUES. |
| One file fails to load | The remaining files still load, then Abort runs | In the _JSON_OUT run, open the failed "Load files" iteration to read the error. Fix the cause and re-run the **whole prime** (never _JSON_OUT alone) |
| "Check landing folder" fails (no read access) | Before Begin: nothing changed | Grant the factory identity access to the landing container (section 3.1), then re-run |
| Data in fails writing or deleting in the landing container (container missing, no write access) | After Begin: BRONZE empty, delta row OFF | Create the container or grant the factory identity Storage Blob Data Contributor on it, then re-run the whole prime. Write access is first used here, which is why a passed test prime in the same factory and container is required before a live prime |
| The Begin prime activity itself fails (timeout or connection) | Begin may have committed (initial row ACTIVE, delta row OFF). Abort does not run | Check sql/03 A. If the initial row is ACTIVE, re-run with `p_Force = true` |
| Run cancelled | Initial row ACTIVE (lock), delta row OFF (live) | Check in Monitor that nothing is running, then re-run with `p_Force = true` |
| "… is ACTIVE: another prime is running, or one was cancelled" | Lock held | As above: confirm, then `p_Force = true` |
| "Delta twin … has open (PLANNED) plan(s)" | A delta run is in progress | Wait for it to finish, then re-run |

**Never** fix ATILDA rows by hand. Re-running the prime is the recovery. The two sign-off exceptions (a CONTROL_DEV_RW holder, sql/03 C) are re-enabling the delta after a passed prime, and restoring a Prepare row from OLD_VALUES.

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

## 8. Current batch: PNCH, ILS LOCATION_INVENTORY, SCAX2012 INVENTSUM

These are the three tables being primed now:

| Short name | STAGE_TABLE_NAME |
|---|---|
| **PNCH** | DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY |
| **ILS** | DB_ILS_LOCATION_INVENTORY |
| **INVENTSUM** | DB_SCAX2012_INVENTSUM |

Section 8.3 goes through them step by step.
- Steps that are the same for all three are written once.
- Every difference is in a **per-table table** or marked **Different:**.

Values marked *from sql/02* or *from A4* are not in this repo. Fill them in during step 1, before anything runs.

### 8.1 The three tables at a glance

| | PNCH | ILS | INVENTSUM |
|---|---|---|---|
| Source (SourceConfig key → server / database in the new factory's repo) | STOCKDB → 192.168.29.58 / STOCKDB | ILS → 192.168.29.79 / ILS | SCAX2012 → 192.168.29.164 / SCAX2012 |
| Source object | dbo.ProductNetworkCostsHistory | the delta row's OBJECT_NAME *from sql/02* (expected dbo.LOCATION_INVENTORY) | the delta row's OBJECT_NAME *from sql/02* (expected dbo.INVENTSUM) |
| Delta row | **156**, RUN_GROUP SCMergeCO, ACTIVE | id, RUN_GROUP, ACTIVE *from sql/02* | id, RUN_GROUP, ACTIVE *from sql/02* |
| Delta schedule to avoid | SCMergeCOSTOCKDB-DailyLoad, 06:00 Sydney time | SCMergeCOILS- Dailyload, **16:20** Sydney time (DI_RDBMS, if the delta's group contains SCMergeCO). If the group contains INCREMENTAL: "ILS - Daily delta load", 04:15 (E_RDBMS). These are the new factory's triggers; check the old factory too | **No trigger for SCAX2012 in this repo** (the new factory). Find the delta row's RUN_GROUP (sql/02), then look in **both** factories for a started trigger matching the worksheet rule. **If neither has one, nothing keeps INVENTSUM current after the prime**, so get one added before step 6. Check the old factory first, so you don't request a duplicate |
| Initial row today | **768**, Parquet, so Prepare **converts** it | expected **none**, so Prepare **creates** it | expected **none**, so Prepare **creates** it |
| Landing folder (SOURCE_PATH) | `json-priming/db_stockdb_productnetworkcostshistory/` | `json-priming/db_ils_location_inventory/` | `json-priming/db_scax2012_inventsum/` |
| Test table and folder | `…_PRIME_TEST`, existing test row **790** (moved to the new folder automatically) | `DB_ILS_LOCATION_INVENTORY_PRIME_TEST`, created on the first test | `DB_SCAX2012_INVENTSUM_PRIME_TEST`, created on the first test |
| `<wm>` (window_column) | RefreshDatetime, server **local** time | the column in the delta's SQL_SNIPPET *from sql/02*; clock check in 8.2 Q2 | the column in the delta's SQL_SNIPPET *from sql/02*. AX 2012 stores datetimes in **UTC**, so 8.2 Q2 is expected to say UTC |
| `<key>` (partition_column) | HistoryID | *from 8.2 Q3* | *from 8.2 Q3* (AX tables normally have a unique index led by RECID, bigint) |
| PRIMARY_KEYS (duplicate check) | HistoryID | *from sql/02* | *from sql/02* |
| total_rows / p_Rows_Per_File | ~215M (Oct 2026) / 3000000 (≈ 72 files) | *from A4* / 1,000,000 unless that gives more than 100 files | *from A4* / 1,000,000 unless that gives more than 100 files |

### 8.2 Three questions to answer per table before any run

**Q1. Is the delta a watermark delta, or a full reload?** Look at the delta row in sql/02 A.

| What you see | Delta type | What to do |
|---|---|---|
| SQL_SNIPPET has `WHERE <col> > '…MAX(…)…'` read from BRONZE (ATILDA guide, recipe A2) | **Watermark delta** | Prime it. The rest of this section applies |
| SQL_SNIPPET is only `SELECT concat('WITH (NOLOCK)',''),''` (no WHERE), usually with SQL_STATEMENT `1=1` (recipe A) | **Full reload**: every delta run already reloads the whole table | **Do not prime it.** The next delta run would replace the primed data anyway. Leave that table to its delta and record it in the worksheet |
| Anything else | — | Stop and ask before priming |

**Q2. Is the watermark column in local time or UTC?** Run in SSMS against that table's source database:

```sql
SELECT MAX(<wm>) AS max_wm, SYSDATETIME() AS server_local, SYSUTCDATETIME() AS server_utc
FROM <schema.table> WITH (NOLOCK);
```

- **max_wm hours or days old** (a static, weekly-refreshed source): nothing changes during the extract, so leave `window_end` empty.
- **max_wm close to server_local** (PNCH expected): leave `window_end` empty. The default end is the server's local clock.
- **max_wm close to server_utc** (INVENTSUM expected): **Different:** set `"window_end"` in the live p_Objects to the current UTC time, rounded down to the minute, e.g. `"2026-10-29T03:15:00"`. Take it from `SYSUTCDATETIME()` just before you start the live run.
  - Why: the default end is the local clock, which is 10–11 hours ahead of UTC. Rows changed during the extract would then fall inside the prime window, and the delta's `> MAX` could miss some of them.
- **Integer watermark:** set `window_end` to `MAX(<wm>) + 1` (section 5 D1).

**Q3. Which partition column?** Run in SSMS. It lists the integer columns that lead an index:

```sql
SELECT c.name AS column_name, TYPE_NAME(c.system_type_id) AS type_name, i.name AS index_name, i.type_desc, i.is_unique
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE ic.object_id = OBJECT_ID('<schema.table>') AND ic.key_ordinal = 1
  AND TYPE_NAME(c.system_type_id) IN ('tinyint', 'smallint', 'int', 'bigint');
```

- Pick a **unique, high-cardinality** column; for Dynamics AX tables (SCAX2012) that is **RECID**.
- **Never PARTITION** (AX 2012 R2/R3 lead almost every index with it, and it has one value), or any column with few distinct values: you would get a single batch.
- Use the clustered index's lead column only if it is near-unique.
- If no rows come back, leave `partition_column` out. The table is then extracted as one file in one stream, which is fine below a few million rows only.

### 8.3 Step by step

**Step 0. One-time setup (same for all three).**
1. **Factory.** Use the old factory (cwr-ase-edp-dev-adf, `adf_old_json`) until the new-factory gate G1–G5 passes (section 3.1).
2. Create the container and grant access (section 3.1).
3. Load the four pipelines (section 3.2).
4. **SourceConfig.** In the factory you will use, the global parameter **SourceConfig** must have the keys **STOCKDB**, **ILS** and **SCAX2012**, each pointing at the database you intend to prime from (ADF → Manage → Global parameters).
5. **Grants.** Run sql/01 (section 3.3), then `SHOW GRANTS ON TABLE EDP_DEV.BRONZE.<T>;` for **each** of DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY, DB_ILS_LOCATION_INVENTORY and DB_SCAX2012_INVENTSUM. Expect SELECT and TRUNCATE for DEV_ACCOUNT_ROLE_DATA_INGESTION, directly or through its database role.
6. **Run group.** Run the DATA_PRIME query in section 2. Only initial rows may appear.
7. **Delta triggers.** For each table, confirm a started trigger runs its delta row (worksheet row "Delta trigger and schedule", both factories). In this repo (the new factory): STOCKDB at 06:00 and ILS at 16:20 (DI_RDBMS, SCMergeCO), plus "StockDB/ILS - Daily delta load" at 04:05/04:15 (E_RDBMS, GRP:INCREMENTAL), and none for SCAX2012. Check the old factory before requesting one.

**Step 1. Read-only checks (once per table).** Do section 5 A for each table, then:
- Fill in the *from …* cells in 8.1.
- Answer Q1–Q3 in 8.2.
- Save each sql/02 output; it is that table's backup.

Expected result of sql/02 B per table:

| | PNCH | ILS | INVENTSUM |
|---|---|---|---|
| sql/02 B shows | 768 with `LIVE_ROW_NEEDS_CONVERT (JSON build)` | **no row** (no initial row yet) | **no row** (no initial row yet) |
| If it shows something else | — | Follow the table in section 5 A1 | Follow the table in section 5 A1 |

**Step 2. Test primes (section 5 B).** One run can test all three, two at a time. p_Rows_Per_File applies to the whole run: 50000 by default, but see below for large tables.

First, count each table's rows in the test window (A4 with a `WHERE <wm> >= '2026-09-26' AND <wm> < '2026-10-26'`; for an integer watermark, use the integer bounds). Inventory tables can change millions of rows a month. If 50000 gives more than about 10 files for a table, test that table in its own run with a larger p_Rows_Per_File, or with a shorter window.
- `p_Rows_Per_File = 50000`
- `p_Objects` (fill in `<wm>` and `<key>` for ILS and INVENTSUM):

```json
[{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST","source_from":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","window_start":"2026-09-26","window_end":"2026-10-26","partition_column":"HistoryID"},
 {"table":"DB_ILS_LOCATION_INVENTORY_PRIME_TEST","source_from":"DB_ILS_LOCATION_INVENTORY","window_column":"<wm>","window_start":"2026-09-26","window_end":"2026-10-26","partition_column":"<key>"},
 {"table":"DB_SCAX2012_INVENTSUM_PRIME_TEST","source_from":"DB_SCAX2012_INVENTSUM","window_column":"<wm>","window_start":"2026-09-26","window_end":"2026-10-26","partition_column":"<key>"}]
```

- **Different:**
  - Drop any table whose Q1 answer was "full reload".
  - Drop `partition_column` for a table with no candidate in Q3.
  - For an **integer** watermark, use integer window_start/window_end covering about a month of values instead of dates.
- **Expected Prepare row per table:**

  | | PNCH | ILS | INVENTSUM |
  |---|---|---|---|
  | First test run | `TEST_ROW_EXISTS`, plus "SOURCE_PATH moved to db_stockdb_productnetworkcostshistory_prime_test/" | `CREATED_TEST_ROW` | `CREATED_TEST_ROW` |
  | Later test runs | `TEST_ROW_EXISTS` | `TEST_ROW_EXISTS` | `TEST_ROW_EXISTS` |

- **Pass** when, for every object, End shows STATUS OK, ROWS_COPIED = BRONZE_ROWS and BATCH_MISMATCH empty.
- If one object fails, the others still finish. Fix it and re-test that object alone.

**Step 3. Test checks and cleanup (section 5 B3–B4, same for all three).**
1. Run the B3 column and text checks per table, using each table's own name.
2. Then run PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST with:
   `p_Tables = ["DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY_PRIME_TEST","DB_ILS_LOCATION_INVENTORY_PRIME_TEST","DB_SCAX2012_INVENTSUM_PRIME_TEST"]`

**Step 4. Backups (section 5 C1–C2).** Take a clone and baseline count, and check the auto LOAD_META, **per table**.
- PNCH shows two rows: 60 (delta 156) and 192 (initial row 768).
- ILS and INVENTSUM show only their delta's row. The initial row's LOAD_META is created by ATILDA on the first load.

**Step 5. Live dry runs (section 5 C3).**
1. Use the step 6 p_Objects with `p_Prepare_Live_Row = false`. In each object, set `"window_start":"2999-01-02"` and `"window_end":"2999-01-01"`, **replacing** any existing window_end (INVENTSUM's UTC one). One run can carry all three.
2. **Expected:**

   | | PNCH | ILS | INVENTSUM |
   |---|---|---|---|
   | Check prepare | fails with `LIVE_ROW_NEEDS_CONVERT` | fails with `LIVE_ROW_MISSING` | fails with `LIVE_ROW_MISSING` |

3. Nothing changes. For step 6, start again from the step 6 p_Objects, taking a fresh UTC window_end where Q2 requires one.

**Step 6. Live primes: one table per run.** Prime them separately. Their p_Rows_Per_File and delta schedules differ, and a failure then leaves only one delta OFF. Suggested order: **ILS, then INVENTSUM, then PNCH** (largest last), each after the previous one has passed step 7.

Same for all three:

| Parameter | Value |
|---|---|
| p_Window_Start | `""`. Full history, or a deliberate `"window_start"` in p_Objects (see section 5 D1) |
| p_Window_End | `""` |
| p_Prepare_Live_Row | `true` |
| p_Enable_Delta | `true`. Use `"enable_delta":"false"` in p_Objects only if that delta row is inactive on purpose (sql/02) |
| p_Force | `false` |
| p_Landing_Container | `json-priming` |
| p_Staging_Container | `snowflake-data` (old factory only) |

Different per table:

| | PNCH | ILS | INVENTSUM |
|---|---|---|---|
| p_Objects | `[{"table":"DB_STOCKDB_PRODUCTNETWORKCOSTSHISTORY","window_column":"RefreshDatetime","partition_column":"HistoryID"}]` | `[{"table":"DB_ILS_LOCATION_INVENTORY","window_column":"<wm>","partition_column":"<key>"}]`, plus `"window_end":"<UTC now>"` only if Q2 said UTC | `[{"table":"DB_SCAX2012_INVENTSUM","window_column":"<wm>","partition_column":"<key>","window_end":"<UTC now, rounded down to the minute>"}]` (drop window_end only if Q2 said local) |
| p_Rows_Per_File | 3000000 (or from A4) | from 8.1 | from 8.1 |
| p_Batch_ID | `PRIME_LIVE_PNCH_<yyyymmdd>` | `PRIME_LIVE_ILS_LOCINV_<yyyymmdd>` | `PRIME_LIVE_INVENTSUM_<yyyymmdd>` |
| When to start | After the 06:00 Sydney delta run has finished, early enough to finish (2.5–6 h of loading plus the extract) before the next 06:00 run | After the 16:20 Sydney ILS delta run has finished | Outside the SCAX2012 delta's schedule (from 8.1) |
| Expected Prepare row | `LIVE_ROW_CONVERTED`. 768 becomes `productnetworkcostshistory_part_%.json`, JSON, SCHEMA_DRIFT_CODE 2, SQL_STATEMENT NULL, RUN_GROUP DATA_PRIME, SOURCE_PATH `db_stockdb_productnetworkcostshistory/` | `CREATED_LIVE_ROW`: a new inactive JSON initial row, SOURCE_PATH `db_ils_location_inventory/` | `CREATED_LIVE_ROW`: a new inactive JSON initial row, SOURCE_PATH `db_scax2012_inventsum/` |
| Expected WINDOW_FILTER | `AND [RefreshDatetime] < CONVERT(datetime…, '<server local now>', 126)` | as for PNCH if local; the UTC value you set if UTC | `AND [<wm>] < CONVERT(…, '<the UTC value you set>', 126)` |

Every other activity output is as in section 5 D2.

**Step 7. After each live prime (section 5 E, per table).**
1. Run sql/03 with that table's `stage_table`, `run_id` and `wm_col`.
2. Compare with its baseline.
3. Confirm its next delta run completes:
   - PNCH: delta 156, the next 06:00 run;
   - ILS: its delta row, the next 16:20 run;
   - INVENTSUM: its delta row, its next scheduled run.
4. Only then start the next table.

### 8.4 PNCH status and leftovers

- Test run 4 passed in the old factory: 163,204 rows, one file.
- A longer test, 2024-07-01 to 2026-10-26 at 50,000 rows per file, ran with the earlier build.
- The one-at-a-time load and the `json-priming` landing are new, so step 2 must pass with the current build.
- The old landing folders in `snowflake-data`, `data_prime_test/` and 768's `PDB08/stockdb/dbo.productnetworkcostshistory/`, are no longer used by the prime. Delete them by hand once step 2 has passed.

### 8.5 Other tables ready to prime

The ATILDA guide (section 10.3) lists more Parquet initial rows that were created but never run:

- 769 PRODUCTPRICINGMATRIX
- 770 CATALOGUEITEMS
- 773 SKU_SKUDETAILS
- 779 CURRENTSOH

Prepare converts each of them as it does 768. Start each with section 5 A and the questions in 8.2.

---

## 9. Alignment with the ATILDA guide

**Followed:**

- **Pairing (sections 3 and 10).** The initial row shares STAGE_TABLE_NAME (and LVL2) with the delta row, and every FILE OBJECT_NAME contains `%`.
- **New rows (section 3, checklist item 8).** ACTIVE_FLAG starts FALSE. Nothing writes TRAN_WATERMARK, IDs or audit columns. SQL_COL_LIST stays NULL on initial rows, because the delta row maintains the typed view.
- **Cutover (section 10.2).** The delta is inactive from the truncate until the load passes. The window end is fixed once, so the delta continues from BRONZE MAX with no gap.
- **File loads.** One plan per file through PR_CREATE_EXTRACT_RUN_PLAN_FILE, loaded **one at a time** in a single ForEach: copy → source_rows_read → PR_CREATE_EXTRACT_ARTIFACTS → extract_run_plan_status COMPLETED. This is the per-plan loop of the house file pipelines (DI_FILE_INITIAL_Shared_Parquet, DI_FILE_CSV_DFIO, TPL_E_FILE_PARQUET). Those cannot be called directly: TPL_E_FILE_PARQUET has no parameters, DI_FILE_CSV_DFIO is bound to the DFIO SFTP account and CSV, and DI_FILE_INITIAL_Shared_Parquet uses Parquet datasets and ends with PR_BULKLOAD_PARQUET.
- **Pipeline count.** _JSON_OUT stays a child pipeline because the entry pipeline already loops over tables in a ForEach, and ADF does not allow a ForEach inside a ForEach.
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
| Section 10.3 HistoryMonths (partial history with ClearTarget) | Same idea: an optional window_start, with BRONZE always truncated | A start date means BRONZE holds only that period from then on (section 5 D1) |
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

**After the prime, do I run anything daily or weekly?** No. The existing house delta pipeline does it. DI_RDBMS_Shared_SQL_Server or E_RDBMS_Shared_SQL_Server, started by the trigger for the delta's SOURCE_NAME and RUN_GROUP, calls `PR_CREATE_EXTRACT_RUN_PLAN(<source>, <group>)` for every **active** delta row, and loads only `<wm> > MAX(<wm>)` from BRONZE. When the source only changes weekly, most runs find nothing (NODATA, COMPLETED). Check that a started trigger exists for each table's delta (worksheet).

**How do I keep the gap small until deployment, without re-priming?** Prime once with `p_Enable_Delta = true`. End turns the delta on after a passed reconcile, and from then on each delta run loads only what changed since the prime, so BRONZE stays current up to deployment. A re-prime reloads the whole window file by file, which is slow, so keep it for a clean full reload.
- If the delta must stay off until deployment, prime with `p_Enable_Delta = false`. Its first run after it is turned on then loads everything since the prime in one go. Turning it on by hand is a sign-off exception. The two sign-off exceptions (a CONTROL_DEV_RW holder, sql/03 C) are re-enabling the delta after a passed prime, and restoring a Prepare row from OLD_VALUES.
- This only works for a **watermark** delta (section 8.2 Q1). A full-reload delta replaces the table on every run anyway.
- If the delta's LOAD_TYPE is APPEND, each change to an existing row adds another row version in BRONZE. That is the delta's normal behaviour, not something the prime adds.

**The source database is refreshed weekly (for example on Mondays). When should I prime?** After a refresh. The source is then static until the next one, so any prime in that week extracts the same rows. Two primes in the same week, for example into two Snowflake accounts, get identical data.

**Can I re-use the landing files to prime the same period into another Snowflake account?** No. Re-run the whole pipeline there, from a factory whose DS_Snowflake_Wh points at that account. Otherwise change SF_DATASET in tools/build_pipelines.py (and `EDP_DEV` in tools/sf_*.sql if the database name differs) and regenerate.
- Data out only plans the current run's files (by RunId).
- End needs Data in's per-batch counts to reconcile.
- Data in clears the table's old part files before it extracts.

The extract is the cheap part anyway: the time goes into the per-file ATILDA load, which happens per account either way. The other account needs its own setup once: an ADF linked service and dataset for it, its own CONTROL rows and grants, and a storage integration that can read the staging container. If both accounts' primes share a storage account, run them one after the other, or give the second its own `p_Landing_Container`.

**Can I prime only part of the history (e.g. from 2024-07-01)?** Yes, with `"window_start"` in p_Objects. A live prime truncates the table first, so BRONZE then holds only that period, and older rows are not back-filled by anything (section 5 D1).

---

## 11. Files, build and change log

| Path | What it is |
|---|---|
| adf_old_json/pipeline/ | The 4 pipelines for cwr-ase-edp-dev-adf |
| adf_new_json/pipeline/ | The 4 pipelines for cwr-ae-dev-data-adf |
| adf_new_json/README.md | Short note on the new-factory build (its staging container and gate) |
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
| 15 | Read probe of the landing folder before Begin (no read access fails before anything changes); old part files are cleared by Data in after the lock |
| 16 | Prepare honours p_Force for a row left ACTIVE by a cancelled run, refuses to move a row onto a path another row uses, and stops with the reason when a test row cannot be moved |
| 17 | The Snowflake V2 connector refused Prepare row (SnowflakeQueryStringNotSupported) because two comments contained the words "copy" and "put"; reworded, and tools/check.py now fails the build on PUT/COPY in any Snowflake query |

### Open items

- **A. New factory gate** (section 3.1, G1–G5).
- **B. Copy to the ATILDA path re-serialises JSON** (JSON to JSON). Section 5 B3 checks the text. If it differs, switch to a byte copy on a new uncompressed Binary dataset with a File parameter.
- **C. First live load into an existing, truncated BRONZE table.** Only new-table loads have run so far. End's reconcile is the safety net.
- **D. Staged copies are never deleted.** They accumulate under the initial row's SOURCE_NAME folder, as for the house pipelines.
- **E. Batch row counts are collected with AppendVariable in a parallel ForEach**, which ADF does not guarantee is thread-safe. A lost count fails safe (End reports a batch mismatch). Section 5 B, with 3–5 files, exercises it.
