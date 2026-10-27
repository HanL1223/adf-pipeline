# adf_new_json: JSON data prime for factory cwr-ae-dev-data-adf

This is the same build as adf_old_json/, generated for the current factory in ../../data-factory/. The only difference is the
container: it uses the house global `@pipeline().globalParameters.StagingContainer` (cwraedevdata/staging), so
p_Staging_Container is removed from all five pipelines.

Regenerate from parquet_ingestion/:

```
python3 tools/build_pipelines.py . new_json && python3 tools/check.py adf_new_json/pipeline
```

Do not hand-edit the JSON.

**Gate before the first run.** ATILDA must be able to read cwraedevdata/staging (CONTROL.STORAGE_DETAILS and
EDP_AZURE_SI_DEV). On 2026-10-27 they allowed only cwraseedpdevst/snowflake-data. See ../LIVE_PRIME_JSON.md section 3
for all five checks.

Import order: _JSON_LOAD_FILE, _JSON_OUT, _JSON_IN, PL_ATILDA_DATA_PRIME_JSON, _JSON_CLEANUP_TEST. Save every one,
then run *Validate all*.
