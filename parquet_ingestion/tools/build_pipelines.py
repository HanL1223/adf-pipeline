"""Generates the data prime pipelines (JSON part files) and sql/10_source_query_test.sql from source_query.tsql.

Run from parquet_ingestion/:
    python3 tools/build_pipelines.py . old && python3 tools/check.py adf_old_json/pipeline   # cwr-ase-edp-dev-adf
    python3 tools/build_pipelines.py . new && python3 tools/check.py adf_new_json/pipeline   # cwr-ae-dev-data-adf
Edit the pipelines here, not in the JSON, so the T-SQL in the pipeline and the SSMS test script stay identical.
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = sys.argv[1]  # parquet_ingestion folder
# Target factory (optional 2nd arg). "old" = cwr-ase-edp-dev-adf: no StagingContainer global, so the ATILDA staging
# container is the pipeline parameter p_Staging_Container; output adf_old_json/pipeline. "new" = cwr-ae-dev-data-adf: the
# house global StagingContainer (cwraedevdata/staging); output adf_new_json/pipeline.
# Both extract to JSON part files (the house delta's SqlServerSource -> JsonSink path on DS_Storage_Stage_JSON): no Java
# on the SHIR. The Parquet build was retired on 2026-10-27 (README.md, "Why JSON").
TARGET = sys.argv[2] if len(sys.argv) > 2 else "old"
assert TARGET in ("old", "new"), TARGET
PIPELINE_DIR = {"old": "adf_old_json", "new": "adf_new_json"}[TARGET]
OLD_FACTORY = TARGET == "old"
# Two containers on the same storage account (linked service cwraseedpst):
#   LANDING: the prime's own extract files, <p_Landing_Container>/<SOURCE_PATH = lowercase table>/. Only ADF reads them.
#   STAGING: the ATILDA container, the only one Snowflake reads (EDP_AZURE_SI_DEV). Only 'Copy to BLOB' writes there.
LANDING = "@pipeline().parameters.p_Landing_Container"
LANDING_DEFAULT = "json-priming"
STAGING = ("@pipeline().parameters.p_Staging_Container" if OLD_FACTORY
           else "@pipeline().globalParameters.StagingContainer")
PL = "PL_ATILDA_DATA_PRIME_JSON"   # entry pipeline; children are PL + _IN / _OUT / _CLEANUP_TEST (already imported)
EXT = ".json"
EXTRACT = "Extract to json"
FOLDER = "ALTIDA/00 Templates/01 Extract/03 Data Prime JSON"  # entry point: the one you trigger
CHILD_FOLDER = FOLDER + "/Child"                              # called only by the entry point
RUN_ID = "@{formatDateTime(pipeline().TriggerTime, 'yyyyMMddHHmmss')}"
# Snowflake dataset for every Lookup. Its linked service must run as a role holding EDP_DEV.CONTROL_DEV_RW (via database
# role DEV_ROLE_DATA_INGESTION): DS_Snowflake_Wh -> cw_au_edp_snowflake_wh -> DEV_ACCOUNT_ROLE_DATA_INGESTION. The live
# file ingest DI_FILE_CSV_DFIO uses it for all ATILDA calls. DS_Snowflake runs as OPS_DEV, which cannot write CONTROL.
SF_DATASET = "DS_Snowflake_Wh"
ROWS_PER_FILE = 1000000   # default p_Rows_Per_File: fewer, larger files load faster through ATILDA (README.md)


def container_params(staging=False):
    """Container parameters of a pipeline: always the landing container; the staging one only where it is written."""
    params = {"p_Landing_Container": sp("string", LANDING_DEFAULT)}
    if staging and OLD_FACTORY:
        params["p_Staging_Container"] = sp("string", "snowflake-data")
    return params


def container_args(staging=False):
    """Pass the caller's container parameters through to a child pipeline."""
    return {k: f"@{p(k)}" for k in container_params(staging)}


def E(v):
    return {"value": v, "type": "Expression"}


def p(name):
    return f"pipeline().parameters.{name}"


def policy(timeout="0.12:00:00", retry=0):
    return {"timeout": timeout, "retry": retry, "retryIntervalInSeconds": 30,
            "secureOutput": False, "secureInput": False}


def dep(*pairs):
    return [{"activity": a, "dependencyConditions": c if isinstance(c, list) else [c]} for a, c in pairs]


def sf_lookup(name, query, depends=(), retry=0, first_row=True, timeout="0.12:00:00", desc=None):
    a = {"name": name, "type": "Lookup", "dependsOn": dep(*depends), "policy": policy(timeout, retry),
         "userProperties": [],
         "typeProperties": {"source": {"type": "SnowflakeV2Source", "query": E(query),
                                       "exportSettings": {"type": "SnowflakeExportCopyCommand"}},
                            "dataset": {"referenceName": SF_DATASET, "type": "DatasetReference"},
                            "firstRowOnly": first_row}}
    if desc:
        a["description"] = desc
    return a


def sql_ds(source_name_expr):
    def cfg(k):
        return E(f"@pipeline().globalParameters.SourceConfig[{source_name_expr}].{k}")
    return {"referenceName": "DS_SQLServer_Shared", "type": "DatasetReference",
            "parameters": {k: cfg(k) for k in ("ServerName", "DatabaseName", "UserNameSecretName", "PasswordSecretName")}}


def bin_ds(folder_expr):
    """Landing folder, for listing and deleting files."""
    return {"referenceName": "DS_STORAGE_V2_STAGE_BIN", "type": "DatasetReference",
            "parameters": {"Folder": E(folder_expr), "Container": E(LANDING)}}


def json_ds(folder_expr, file_expr, container):
    return {"referenceName": "DS_Storage_Stage_JSON", "type": "DatasetReference",
            "parameters": {"Folder": E(folder_expr), "File": E(file_expr), "Container": E(container)}}


def fail(name, message_expr, code, depends=()):
    return {"name": name, "type": "Fail", "dependsOn": dep(*depends), "userProperties": [],
            "typeProperties": {"message": E(message_expr), "errorCode": code}}


def if_(name, expr, true=(), false=(), depends=(), desc=None):
    tp = {"expression": E(expr)}
    if true:
        tp["ifTrueActivities"] = list(true)
    if false:
        tp["ifFalseActivities"] = list(false)
    a = {"name": name, "type": "IfCondition", "dependsOn": dep(*depends), "userProperties": [], "typeProperties": tp}
    if desc:
        a["description"] = desc
    return a


def exec_pl(name, pipeline, params, depends=()):
    return {"name": name, "type": "ExecutePipeline", "dependsOn": dep(*depends), "userProperties": [],
            "typeProperties": {"pipeline": {"referenceName": pipeline,
                                            "type": "PipelineReference"},
                               "waitOnCompletion": True, "parameters": {k: E(v) for k, v in params.items()}}}


def pipeline(name, desc, params, variables, activities):
    return {"name": name, "properties": {
        "description": desc, "activities": activities, "parameters": params,
        "variables": variables,
        "folder": {"name": FOLDER if name == PL else CHILD_FOLDER}, "annotations": []}}


def sp(t, default=None):
    d = {"type": t}
    if default is not None:
        d["defaultValue"] = default
    return d


def const(v):
    return {"type": "String", "defaultValue": v}


def esc(expr):
    """ADF expression that doubles single quotes, for a value placed inside a SQL '...' literal."""
    return f"replace({expr}, '''', '''''')"


# ---------------------------------------------------------------- PL_ATILDA_DATA_PRIME_JSON_OUT
def load_plan_activities():
    """One ATILDA file plan (item() of 'Load files'): the per-plan body of the house file pipelines
    (DI_FILE_INITIAL_Shared_Parquet, DI_FILE_CSV_DFIO), on JSON."""
    plan = "item().EXTRACT_RUN_PLAN_ID"
    log = lambda act, status, msg: (
        f"CALL CONTROL.PR_LOG_EXTRACT_RUN_PLAN(@{{{plan}}}, '@{{variables('{act}')}}', "
        f"'@{{variables('{status}')}}', {msg});")
    # ATILDA's file plan (EXTRACT_RUN_PLAN_VW): OBJECT_NAME = the landing file in SOURCE_PATH; FILE_NAME = the target path
    # under LVL1_FOLDER_NAME (<LVL2>/<META_ID>_<PLAN_NO>/<renamed>.json), which ATILDA loads. Read the one, write the
    # other, as the house file pipelines do. JSON to JSON, because the Binary dataset (no File parameter) cannot set a
    # file name.
    copy = {
        "name": "Copy to BLOB", "type": "Copy", "dependsOn": [], "policy": policy("0.12:00:00", 0),
        "userProperties": [],
        "description": "Copies this plan's landing file (landing container, SOURCE_PATH/OBJECT_NAME) to the ATILDA path "
                       "(staging container, LVL1_FOLDER_NAME/FILE_NAME), which PR_CREATE_EXTRACT_ARTIFACTS loads.",
        "typeProperties": {
            "source": {"type": "JsonSource",
                       "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
                       "formatSettings": {"type": "JsonReadSettings"}},
            "sink": {"type": "JsonSink", "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
                     "formatSettings": {"type": "JsonWriteSettings"}},
            "enableStaging": False},
        "inputs": [json_ds("@item().SOURCE_PATH", "@item().OBJECT_NAME", LANDING)],
        "outputs": [json_ds("@item().LVL1_FOLDER_NAME", "@item().FILE_NAME", STAGING)]}
    return [
        # A blank p_Batch_ID can arrive as a JSON null through ExecutePipeline; equals(null, '') is false and coalesce
        # passes the null through, so SET BATCH ID ran replace() on null. empty() is true for null and ''.
        if_("If BATCH ID exists", f"@empty({p('p_Batch_ID')})", false=[
            sf_lookup("SET BATCH ID",
                      f"CALL CONTROL.PR_LOG_EXTRACT_RUN_PLAN(@{{{plan}}}, '@{{variables('vConst_Batch_Id')}}', "
                      f"'@{{{esc(p('p_Batch_ID'))}}}', 'Successful');", retry=2)]),
        if_("Check if blobcopy is COMPLETE",
            "@equals(coalesce(item().SOURCE_EXTRACT_STATUS, ''), variables('vConst_Complete'))",
            depends=[("If BATCH ID exists", "Succeeded")],
            false=[
                copy,
                # House file pipelines log source_rows_read after the copy (they log dataRead; rowsRead is the row count).
                sf_lookup("Update source rows read",
                          f"CALL CONTROL.PR_LOG_EXTRACT_RUN_PLAN(@{{{plan}}}, '@{{variables('vConst_Rows_Read')}}', "
                          "'@{string(coalesce(activity('Copy to BLOB').output?.rowsRead, 0))}', 'Successful');",
                          depends=[("Copy to BLOB", "Succeeded")], retry=2),
                sf_lookup("Complete BLOB copy", log("vConst_BlobCopy_Status", "vConst_Complete", "'Successful'"),
                          depends=[("Update source rows read", "Succeeded")], retry=2),
                sf_lookup("Fail BLOB copy",
                          log("vConst_BlobCopy_Status", "vConst_Fail",
                              "$$@{replace(activity('Copy to BLOB').error.message, '$$', '')}$$"),
                          depends=[("Copy to BLOB", "Failed")], retry=2),
                fail("Raise BLOB copy error",
                     f"@concat('Stage copy failed for plan ', string({plan}), ': ', activity('Copy to BLOB').error.message)",
                     "PRIME_STAGE_COPY_FAILED", depends=[("Fail BLOB copy", "Completed")]),
            ]),
        sf_lookup("Provision SI artifacts and load data",
                  f"CALL CONTROL.PR_CREATE_EXTRACT_ARTIFACTS(@{{item().EXTRACT_META_ID}}, @{{{plan}}});",
                  depends=[("Check if blobcopy is COMPLETE", "Succeeded")]),
        sf_lookup("Complete ETL run", log("vConst_Run_Status", "vConst_Complete", "'Successful'"),
                  depends=[("Provision SI artifacts and load data", "Succeeded")], retry=2),
    ]


def data_out():
    files = "activity('Keep run files').output.Value"
    plans = "activity('Get run plan').output.value"
    run_prefix = f"concat({p('p_File_Prefix')}, '_part_', {p('p_Run_Id')}, '_')"
    acts = [
        {"name": "Get File Metadata", "type": "GetMetadata", "dependsOn": [], "policy": policy("0.01:00:00", 2),
         "userProperties": [],
         "description": "exists is listed so a missing folder (0 rows extracted) does not fail the activity.",
         "typeProperties": {"dataset": bin_ds(f"@{p('p_Source_Path')}"), "fieldList": ["exists", "childItems"],
                            "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
                            "formatSettings": {"type": "BinaryReadSettings"}}},
        {"name": "Keep run files", "type": "Filter", "dependsOn": dep(("Get File Metadata", "Succeeded")),
         "userProperties": [],
         "typeProperties": {
             "items": E("@coalesce(activity('Get File Metadata').output?.childItems, json('[]'))"),
             "condition": E(f"@and(equals(item().type, 'File'), and(startswith(item().name, {run_prefix}), "
                            f"endswith(toLower(item().name), '{EXT}')))")}},
        if_("Check file count", f"@greater(length({files}), int(variables('vConst_Max_Files')))",
            depends=[("Keep run files", "Succeeded")],
            true=[fail("Raise too many files",
                       f"@concat(string(length({files})), ' files exceed the ', variables('vConst_Max_Files'), "
                       f"'-row Lookup limit. Raise p_Rows_Per_File and re-run.')", "PRIME_TOO_MANY_FILES")]),
        if_("Plan new files", f"@greater(length({files}), 0)", depends=[("Check file count", "Succeeded")],
            true=[sf_lookup("Generate Run Plan",
                            f"CALL CONTROL.PR_CREATE_EXTRACT_RUN_PLAN_FILE('@{{{esc(p('p_Initial_Source_Name'))}}}', "
                            f"'@{{{esc(p('p_Source_Path'))}}}', $$@{{string({files})}}$$, '0', '0');")]),
        sf_lookup("Get run plan",
                  "SELECT EXTRACT_RUN_PLAN_ID, EXTRACT_META_ID, FILE_NAME, SOURCE_PATH, LVL1_FOLDER_NAME, OBJECT_NAME, "
                  "SOURCE_EXTRACT_STATUS FROM CONTROL.EXTRACT_RUN_PLAN_VW "
                  f"WHERE EXTRACT_META_ID = @{{{p('p_Initial_Meta_Id')}}} "
                  f"AND CONTAINS(FILE_NAME, '_part_@{{{p('p_Run_Id')}}}_') ORDER BY FILE_NAME",
                  depends=[("Plan new files", "Succeeded")], retry=2, first_row=False),
        if_("Check plan count", f"@not(equals(length({plans}), length({files})))",
            depends=[("Get run plan", "Succeeded")],
            true=[fail("Raise plan count mismatch",
                       f"@concat('ATILDA planned ', string(length({plans})), ' of ', string(length({files})), "
                       f"' files for run ', {p('p_Run_Id')}, '. Check CONTROL.EXTRACT_RUN_PLAN_STATUS_VW.')",
                       "PRIME_PLAN_COUNT_MISMATCH")]),
        # One plan at a time, as the house file pipelines do: PR_CREATE_EXTRACT_ARTIFACTS also processes the row's other
        # open plans, so concurrent calls could load one staged file twice. Sequential also lets the first file create
        # the BRONZE table alone.
        {"name": "Load files", "type": "ForEach", "dependsOn": dep(("Check plan count", "Succeeded")),
         "userProperties": [],
         "typeProperties": {"items": E(f"@{plans}"), "isSequential": True, "activities": load_plan_activities()}},
    ]
    return pipeline(
        PL + "_OUT",
        "Data prime, part 2 (data out): plan this run's JSON files in ATILDA (one plan per file), then per plan copy "
        "the file to the ATILDA path and load it into BRONZE, one at a time (house file-pipeline loop). "
        "Called by PL_ATILDA_DATA_PRIME_JSON.",
        {"p_Initial_Meta_Id": sp("string"), "p_Initial_Source_Name": sp("string"), "p_Source_Path": sp("string"),
         "p_File_Prefix": sp("string"), "p_Run_Id": sp("string"), "p_Batch_ID": sp("string", ""),
         **container_params(staging=True)},
        {"vConst_Max_Files": const("5000"), "vConst_Complete": const("COMPLETED"), "vConst_Fail": const("FAILED"),
         "vConst_BlobCopy_Status": const("source_extract_status"), "vConst_Rows_Read": const("source_rows_read"),
         "vConst_Run_Status": const("extract_run_plan_status"), "vConst_Batch_Id": const("batch_id")},
        acts)


# ---------------------------------------------------------------- PL_ATILDA_DATA_PRIME_JSON_IN
def data_in():
    # House delta sink (TPL_E_RDBMS_J4_V2): JsonSink to DS_Storage_Stage_JSON (gzip), no Java, no staging. The JSON sink
    # has no maxRowsPerFile/fileNamePrefix, so each batch writes one named file. The '_1' suffix keeps the
    # '_part_<RunId>_b<k>_' tag that 'End prime' reconciles on.
    copy = {
        "name": EXTRACT, "type": "Copy", "dependsOn": [], "policy": policy("1.00:00:00", 0),
        "userProperties": [],
        "description": "One key-range batch (p_Rows_Per_File rows) to one JSON file "
                       "<prefix>_part_<RunId>_b<k>_1.json (gzip content, as the delta writes). No retry needed: "
                       "a re-run overwrites the same name, but the prime is re-run as a whole.",
        "typeProperties": {
            "source": {"type": "SqlServerSource",
                       "sqlReaderQuery": E(f"@{{{p('p_Source_Query')}}}@{{item().BATCH_FILTER}}"),
                       "queryTimeout": "12:00:00", "partitionOption": "None"},
            "sink": {"type": "JsonSink", "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
                     "formatSettings": {"type": "JsonWriteSettings"}},
            "enableStaging": False},
        "inputs": [sql_ds(p("p_Source_Name"))],
        "outputs": [json_ds(f"@{p('p_Source_Path')}",
                            f"@{{{p('p_File_Prefix')}}}_part_@{{{p('p_Run_Id')}}}_b@{{item().BATCH_NO}}_1.json",
                            LANDING)]}
    acts = [
        if_("Clear landing folder", f"@{p('p_Clear_Landing')}",
            desc="Removes this object's part files from earlier runs (SOURCE_PATH is the object's own folder).",
            true=[{"name": "Delete old part files", "type": "Delete", "dependsOn": [],
                   "policy": policy("0.01:00:00", 2), "userProperties": [],
                   "typeProperties": {"dataset": bin_ds(f"@{p('p_Source_Path')}"), "enableLogging": False,
                                      "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False,
                                                        "wildcardFileName": E(f"@concat({p('p_File_Prefix')}, "
                                                                              f"'_part_*{EXT}')")}}}]),
        {"name": "Extract batches", "type": "ForEach",
         "dependsOn": dep(("Clear landing folder", "Succeeded")),
         "userProperties": [],
         "typeProperties": {"items": E(f"@{p('p_Batches')}"), "isSequential": False,
                            "batchCount": 4, "activities": [
                                copy,
                                {"name": "Collect batch rows", "type": "AppendVariable",
                                 "dependsOn": dep((EXTRACT, "Succeeded")), "userProperties": [],
                                 "typeProperties": {"variableName": "vBatch_Rows", "value": E(
                                     "@json(concat('{\"b\":\"', string(item().BATCH_NO), '\",\"rows\":', "
                                     f"string(activity('{EXTRACT}').output.rowsCopied), '}}'))")}}]}},
        {"name": "Return batch rows", "type": "SetVariable", "dependsOn": dep(("Extract batches", "Succeeded")),
         "userProperties": [], "policy": {"secureOutput": False, "secureInput": False},
         "description": "Returns [{b, rows}] per batch to PL_ATILDA_DATA_PRIME_JSON, which reconciles them against BRONZE.",
         "typeProperties": {"variableName": "pipelineReturnValue", "setSystemVariable": True,
                            "value": [{"key": "batch_rows",
                                       "value": {"type": "Expression", "content": "@variables('vBatch_Rows')"}}]}},
    ]
    return pipeline(
        PL + "_IN",
        "Data prime, part 1 (data in): extract one SQL Server table to JSON part files in "
        "<p_Landing_Container>/<SOURCE_PATH>/, in parallel key-range batches. ADF version of the PDF Python script. "
        "The source query is validated and built by 'Get source columns' in PL_ATILDA_DATA_PRIME_JSON before anything "
        "changes. Returns the rows extracted per batch. Called by PL_ATILDA_DATA_PRIME_JSON.",
        {"p_Source_Name": sp("string"), "p_Source_Path": sp("string"),
         "p_File_Prefix": sp("string"), "p_Run_Id": sp("string"), "p_Source_Query": sp("string"),
         "p_Batches": sp("array"), "p_Clear_Landing": sp("bool", False),
         **container_params()},
        {"vBatch_Rows": {"type": "Array", "defaultValue": []}}, acts)


# ---------------------------------------------------------------- PL_ATILDA_DATA_PRIME_JSON
def fill(template, values):
    """Replace {{TOKEN}}s in a SQL template with ADF interpolations; every token must be used."""
    assert "@{" not in template and not template.startswith("@"), "template text must not look like an ADF expression"
    for k, v in values.items():
        assert "{{" + k + "}}" in template, k
        template = template.replace("{{" + k + "}}", v)
    left = re.findall(r"\{\{[A-Z_]+\}\}", template)
    assert not left, left
    return template


def parent(tsql):
    pair = "activity('Check pair').output.firstRow"
    cols = "activity('Get source columns').output.firstRow"
    begin = "activity('Begin prime').output.firstRow"
    end = "activity('End prime').output.firstRow"
    ok = "variables('vConst_Status_OK')"
    opt = lambda key, default: f"coalesce(item()?.{key}, {default})"
    i = lambda expr: f"@{{{expr}}}"
    lit = lambda expr: f"@{{{esc(expr)}}}"
    subst = {
        "__OBJ__": lit(pair + ".SOURCE_OBJECT"),
        "__WCOL__": lit(opt("window_column", "''")),
        "__WS__": lit(opt("window_start", p("p_Window_Start"))),
        "__WE__": lit(opt("window_end", p("p_Window_End"))),
        "__PCOL__": lit(opt("partition_column", "''")),
        # One file per batch, so a batch is p_Rows_Per_File rows (the JSON sink has no maxRowsPerFile).
        "__N__": i(f"string({p('p_Rows_Per_File')})"),
    }
    assert "@{" not in tsql
    q = tsql
    for k, v in subst.items():
        assert q.count(k) == 1, k
        q = q.replace(k, v)
    sf = lambda name: open(os.path.join(HERE, name)).read()
    common = {"INIT": i(f"{pair}.INITIAL_META_ID"), "DELTA": i(f"{pair}.DELTA_META_ID"), "RUN": RUN_ID,
              "BRONZE": lit(f"{pair}.BRONZE_TABLE")}
    check_sql = fill(sf("sf_check_pair.sql"), {"TABLE": lit("trim(string(item().table))"), "FORCE": i(p("p_Force")),
                                               "SOURCE_FROM": lit(opt("source_from", "''")),
                                               "ENABLE_DELTA": lit(f"string({opt('enable_delta', p('p_Enable_Delta'))})")})
    common["TEST_MODE"] = i(f"{pair}.TEST_MODE")
    begin_sql = fill(sf("sf_begin.sql"), dict(common, FORCE=i(p("p_Force")), DELTA_ACTIVE=i(f"{pair}.DELTA_ACTIVE")))
    end_sql = fill(sf("sf_end.sql"), dict(
        common, TS=lit(f"{pair}.TARGET_SCHEMA"), ST=lit(f"{pair}.STAGE_TABLE"),
        WM_QUERY=i(f"coalesce({pair}.WM_QUERY, '')"), DUP_QUERY=i(f"coalesce({pair}.DUP_QUERY, '')"),
        BATCH_JSON=i("string(activity('Data in').output.pipelineReturnValue.batch_rows)"),
        ENABLE_DELTA=i(f"{pair}.ENABLE_DELTA"), DELTA_MAX_PLAN=i(f"{begin}.DELTA_MAX_PLAN")))
    prepare_sql = fill(sf("sf_prepare.sql"), {"TABLE": lit("trim(string(item().table))"),
                                              "SOURCE_FROM": lit(opt("source_from", "''")),
                                              "PREPARE_LIVE": i(p("p_Prepare_Live_Row")), "FORCE": i(p("p_Force")),
                                              "ENABLE_DELTA": lit(f"string({opt('enable_delta', p('p_Enable_Delta'))})"),
                                              "ROWS_PER_FILE": i(f"string({p('p_Rows_Per_File')})")})
    prep = "activity('Prepare row').output.firstRow"
    body = [
        sf_lookup("Prepare row", prepare_sql,
                  desc="Setup DML as the ADF role (tools/sf_prepare.sql): test mode creates the test row if missing; "
                       "live mode fixes or creates the initial row only with p_Prepare_Live_Row = true, returning the "
                       "old values. Idempotent (guarded inserts/updates)."),
        if_("Check prepare", f"@not(equals({prep}.STATUS, {ok}))", depends=[("Prepare row", "Succeeded")],
            true=[fail("Raise prepare error", f"@concat(item().table, ': ', {prep}.MESSAGE)", "PRIME_PREPARE_FAILED")]),
        sf_lookup("Check pair", check_sql, retry=2, depends=[("Check prepare", "Succeeded")],
                  desc="Read-only SELECT on CONTROL.EXTRACT_META: finds and validates the INITIAL_LOAD / delta pair "
                       "(tools/sf_check_pair.sql)."),
        if_("Check pair result", f"@not(equals(coalesce({pair}.ERROR_MESSAGE, ''), ''))",
            depends=[("Check pair", "Succeeded")],
            true=[fail("Raise pair error", f"@concat(item().table, ': ', {pair}.ERROR_MESSAGE)", "PRIME_PAIR_INVALID")]),
        {"name": "Get source columns", "type": "Lookup", "dependsOn": dep(("Check pair result", "Succeeded")),
         "policy": policy("0.01:00:00", 2), "userProperties": [],
         "description": "Read-only T-SQL over sys.columns: validates the source object, window and partition column "
                        "and builds SOURCE_QUERY (raw SELECT list + window) and RANGES_SQL. Runs before anything "
                        "changes. Same text as sql/10_source_query_test.sql.",
         "typeProperties": {"source": {"type": "SqlServerSource", "sqlReaderQuery": E(q),
                                       "queryTimeout": "00:30:00", "partitionOption": "None"},
                            "dataset": sql_ds(f"{pair}.SOURCE_NAME"), "firstRowOnly": True}},
        # No estimate guard: one file without a partition column; with one, RANGES_SQL caps the batches at 4999.
        if_("Check source columns", f"@not(equals(coalesce({cols}.ERROR_MESSAGE, ''), ''))",
            depends=[("Get source columns", "Succeeded")],
            true=[fail("Raise source error", f"@concat(item().table, ': ', {cols}.ERROR_MESSAGE)",
                       "PRIME_SOURCE_INVALID")]),
        {"name": "Get batch ranges", "type": "Lookup", "dependsOn": dep(("Check source columns", "Succeeded")),
         "policy": policy("0.06:30:00", 2), "userProperties": [],
         "description": "Read-only, before anything changes: half-open key ranges (ROW_NUMBER modulus over the indexed "
                        "partition key) plus a NULL batch 0; one empty-filter batch when no partition column is set.",
         "typeProperties": {"source": {"type": "SqlServerSource", "sqlReaderQuery": E(f"@{{{cols}.RANGES_SQL}}"),
                                       "queryTimeout": "06:00:00", "partitionOption": "None"},
                            "dataset": sql_ds(f"{pair}.SOURCE_NAME"), "firstRowOnly": False}},
        if_("Check batch ranges", "@equals(length(activity('Get batch ranges').output.value), 0)",
            depends=[("Get batch ranges", "Succeeded")],
            true=[fail("Raise no rows", f"@concat(item().table, ': no rows in the window; nothing to prime. BRONZE not touched.')",
                       "PRIME_NO_ROWS")]),
        # Read probe of the landing container before Begin: a missing container or no read access fails while nothing
        # has changed. Old part files are cleared by Data in, after the lock.
        {"name": "Check landing folder", "type": "GetMetadata", "dependsOn": dep(("Check batch ranges", "Succeeded")),
         "policy": policy("0.01:00:00", 2), "userProperties": [],
         "typeProperties": {"dataset": bin_ds(f"@{pair}.SOURCE_PATH"), "fieldList": ["exists"],
                            "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
                            "formatSettings": {"type": "BinaryReadSettings"}}},
        sf_lookup("Begin prime", begin_sql, depends=[("Check landing folder", "Succeeded")],
                  desc="Lock (initial row ACTIVE), abandon its old plans, delta OFF, truncate BRONZE "
                       "(tools/sf_begin.sql). Everything before this is read-only."),
        if_("Check begin", f"@not(equals({begin}.STATUS, {ok}))", depends=[("Begin prime", "Succeeded")],
            true=[fail("Raise begin error", f"@concat(item().table, ': ', {begin}.MESSAGE)", "PRIME_BEGIN_FAILED")]),
        exec_pl("Data in", PL + "_IN", {
            "p_Source_Name": f"@{pair}.SOURCE_NAME",
            "p_Source_Path": f"@{pair}.SOURCE_PATH",
            "p_File_Prefix": f"@{pair}.FILE_PREFIX",
            "p_Run_Id": RUN_ID,
            "p_Source_Query": f"@{cols}.SOURCE_QUERY",
            "p_Batches": "@activity('Get batch ranges').output.value",
            "p_Clear_Landing": "@activity('Check landing folder').output.exists",
            **container_args()}, depends=[("Check begin", "Succeeded")]),
        exec_pl("Data out", PL + "_OUT", {
            "p_Initial_Meta_Id": f"@string({pair}.INITIAL_META_ID)",
            "p_Initial_Source_Name": f"@{pair}.INITIAL_SOURCE_NAME",
            "p_Source_Path": f"@{pair}.SOURCE_PATH",
            "p_File_Prefix": f"@{pair}.FILE_PREFIX",
            "p_Run_Id": RUN_ID,
            "p_Batch_ID": f"@{p('p_Batch_ID')}",
            **container_args(staging=True)}, depends=[("Data in", "Succeeded")]),
        sf_lookup("End prime", end_sql, depends=[("Data out", "Succeeded")],
                  desc="Reconcile, check the delta watermark, release the lock, delta ON only if safe "
                       "(tools/sf_end.sql)."),
        if_("Check end", f"@not(equals({end}.STATUS, {ok}))", depends=[("End prime", "Succeeded")],
            true=[fail("Raise end error", f"@concat(item().table, ': ', {end}.MESSAGE)", "PRIME_RECONCILE_FAILED")]),
        sf_lookup("Abort prime",
                  f"UPDATE EDP_DEV.CONTROL.EXTRACT_META SET ACTIVE_FLAG = FALSE "
                  f"WHERE EXTRACT_META_ID = @{{{pair}.INITIAL_META_ID}};",
                  depends=[("Check begin", "Succeeded"), ("Check end", ["Failed", "Skipped"])], retry=2,
                  desc="Runs only after a successful begin when a later step failed (ADF ANDs the two dependencies). "
                       "Releases the lock; the delta twin stays OFF so it cannot full-pull into a partial BRONZE."),
        fail("Raise prime error",
             "@concat(item().table, ': data prime failed (RunId ', "
             "formatDateTime(pipeline().TriggerTime, 'yyyyMMddHHmmss'), "
             "'). See the failed activity. Re-run the prime; until a prime passes, the delta twin stays OFF "
             "(a test prime never touches it).')",
             "PRIME_FAILED", depends=[("Abort prime", "Completed")]),
    ]
    tables = "variables('vTables')"
    acts = [
        if_("Check objects", f"@equals(length({p('p_Objects')}), 0)",
            true=[fail("Raise no objects", "p_Objects is empty. List at least one object: [{\"table\": \"<STAGE_TABLE_NAME>\"}].",
                       "PRIME_NO_OBJECTS")]),
        {"name": "Collect tables", "type": "ForEach", "dependsOn": dep(("Check objects", "Succeeded")),
         "userProperties": [],
         "description": "Sequential, so appending to the variable is safe. Feeds the duplicate check.",
         "typeProperties": {"items": E(f"@{p('p_Objects')}"), "isSequential": True, "activities": [
             {"name": "Add table", "type": "AppendVariable", "dependsOn": [], "userProperties": [],
              "typeProperties": {"variableName": "vTables", "value": E("@toUpper(trim(string(item().table)))")}}]}},
        if_("Check duplicates", f"@not(equals(length(union({tables}, {tables})), length({tables})))",
            depends=[("Collect tables", "Succeeded")],
            true=[fail("Raise duplicate objects",
                       f"@concat('A table is listed more than once in p_Objects: ', string({tables}))",
                       "PRIME_DUPLICATE_OBJECTS")]),
        {"name": "Loop objects", "type": "ForEach", "dependsOn": dep(("Check duplicates", "Succeeded")),
         "userProperties": [],
         "description": "One iteration per object, 2 in parallel (each runs up to 4 copies). No variables are set here. "
                        "Everything up to 'Begin prime' is read-only, so invalid input never truncates BRONZE.",
         "typeProperties": {"items": E(f"@{p('p_Objects')}"), "isSequential": False, "batchCount": 2,
                            "activities": body}},
    ]
    return pipeline(
        PL,
        "One-click data priming (initial load) of SQL Server tables into BRONZE via ATILDA. "
        "p_Objects: [{\"table\": \"<STAGE_TABLE_NAME>\", \"window_column\": \"<col>\", \"window_start\": \"\", "
        "\"window_end\": \"\", \"partition_column\": \"<int key>\", \"enable_delta\": true, \"source_from\": \"<live table, test prime only>\"}]. "
        "See parquet_ingestion/README.md.",
        {"p_Objects": sp("array", []),
         "p_Window_Start": sp("string", ""), "p_Window_End": sp("string", ""),
         "p_Rows_Per_File": sp("int", ROWS_PER_FILE),
         "p_Enable_Delta": sp("bool", True), "p_Prepare_Live_Row": sp("bool", False), "p_Force": sp("bool", False),
         "p_Batch_ID": sp("string", ""), **container_params(staging=True)},
        {"vConst_Status_OK": const("OK"), "vTables": {"type": "Array", "defaultValue": []}},
        acts)


# ---------------------------------------------------------------- PL_ATILDA_DATA_PRIME_JSON_CLEANUP_TEST
def cleanup_test():
    res = "activity('Cleanup test target').output.firstRow"
    sql = fill(open(os.path.join(HERE, "sf_cleanup_test.sql")).read(),
               {"TABLE": f"@{{{esc('trim(string(item()))')}}}", "DELETE_ROW": f"@{{{p('p_Delete_Test_Row')}}}"})
    body = [
        sf_lookup("Cleanup test target", sql,
                  desc="Drops BRONZE.<name> and optionally the test row; only for an inactive *_PRIME_TEST row "
                       "(tools/sf_cleanup_test.sql)."),
        if_("Check cleanup", f"@not(equals({res}.STATUS, 'OK'))", depends=[("Cleanup test target", "Succeeded")],
            true=[fail("Raise cleanup error", f"@concat(item(), ': ', {res}.MESSAGE)", "PRIME_CLEANUP_FAILED")]),
        {"name": "Check test folder", "type": "GetMetadata", "dependsOn": dep(("Check cleanup", "Succeeded")),
         "policy": policy("0.01:00:00", 2), "userProperties": [],
         "description": "A missing SOURCE_PATH is probed as a folder that cannot exist, never the container root.",
         "typeProperties": {"dataset": bin_ds(f"@coalesce({res}.SOURCE_PATH, '__none__/')"),
                            "fieldList": ["exists"],
                            "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
                            "formatSettings": {"type": "BinaryReadSettings"}}},
        if_("Delete test files", f"@and(not(empty(coalesce({res}.SOURCE_PATH, ''))), "
                                 f"activity('Check test folder').output.exists)",
            depends=[("Check test folder", "Succeeded")],
            desc="Removes the test landing files (SOURCE_PATH of the test row), only if the folder exists.",
            true=[{"name": "Delete test landing files", "type": "Delete", "dependsOn": [],
                   "policy": policy("0.01:00:00", 2), "userProperties": [],
                   "typeProperties": {"dataset": bin_ds(f"@{res}.SOURCE_PATH"), "enableLogging": False,
                                      "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False,
                                                        "wildcardFileName": "*" + EXT}}}]),
    ]
    pl = pipeline(
        PL + "_CLEANUP_TEST",
        "Removes test prime targets (*_PRIME_TEST): drops the test BRONZE table, deletes its landing files and, with "
        "p_Delete_Test_Row = true, the test EXTRACT_META row and its auto LOAD_META row. Never touches a live table.",
        {"p_Tables": sp("array", []), "p_Delete_Test_Row": sp("bool", False),
         **container_params()}, {},
        [if_("Check tables", f"@equals(length({p('p_Tables')}), 0)",
             true=[fail("Raise no tables", "p_Tables is empty. List test targets, e.g. [\"<TABLE>_PRIME_TEST\"].",
                        "PRIME_NO_TABLES")]),
         {"name": "Loop tables", "type": "ForEach", "dependsOn": dep(("Check tables", "Succeeded")), "userProperties": [],
          "typeProperties": {"items": E(f"@{p('p_Tables')}"), "isSequential": True, "activities": body}}])
    pl["properties"]["folder"] = {"name": FOLDER}
    return pl


def main():
    tsql = open(os.path.join(HERE, "source_query.tsql")).read()
    os.makedirs(os.path.join(OUT, PIPELINE_DIR, "pipeline"), exist_ok=True)
    for pl in (parent(tsql), data_in(), data_out(), cleanup_test()):
        with open(os.path.join(OUT, PIPELINE_DIR, "pipeline", pl["name"] + ".json"), "w") as f:
            json.dump(pl, f, indent=4)
            f.write("\n")
    if not OLD_FACTORY:
        return  # sql/10 is factory-independent; the old target writes it
    test = tsql
    for k, v in {"__OBJ__": "dbo.ProductNetworkCostsHistory", "__WCOL__": "RefreshDatetime",
                 "__WS__": "2025-10-01", "__WE__": "", "__PCOL__": "HistoryID",
                 "__N__": str(ROWS_PER_FILE)}.items():
        test = test.replace(k, v)
    src = "tools/source_query.tsql"
    header = (f"-- Dry run of the 'Get source columns' Lookup in {PL} (README.md section 5 A5).\n"
              "-- Read-only. Run in SSMS against the source database as the ADF SQL login.\n"
              "-- Edit the six DECLARE values below, then run. Expect ERROR_MESSAGE = '' and copy RANGES_SQL into a\n"
              "-- new window to see the batch ranges.\n"
              f"-- Generated by tools/build_pipelines.py from {src} (the same text as the pipeline).\n"
              f"-- Do not edit the logic here: change {src} and regenerate.\n")
    name = "10_source_query_test.sql"
    with open(os.path.join(OUT, "sql", name), "w") as f:
        f.write(header + test)


if __name__ == "__main__":
    main()
