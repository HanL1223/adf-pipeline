# adf_new_json: data prime pipelines for factory cwr-ae-dev-data-adf

This is the same JSON build as ../adf_old_json, generated for the current factory in ../../data-factory/. The only
difference is ATILDA's staging container: the house global `@pipeline().globalParameters.StagingContainer`
(cwraedevdata/staging), so there is no p_Staging_Container parameter. The landing container is p_Landing_Container
(default json-priming), which must exist on cwraedevdata.

Regenerate from parquet_ingestion/:

```
python3 tools/build_pipelines.py . new && python3 tools/check.py adf_new_json/pipeline
```

Before the first run, pass the gate in ../README.md section 3.1. ATILDA must be able to read cwraedevdata/staging. Then
run a test prime here, before the live prime.
