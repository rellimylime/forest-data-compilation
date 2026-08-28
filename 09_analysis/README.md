# 09 Analysis

This module contains one active condition-level analysis path. It builds
first-to-last climate-niche change, cumulative agent-attributed mortality, site
climatic water deficit, and the current preliminary models.

The active scripts are numbered in execution order:

1. `00_build_remeasurement_components.R` — official FIA PREV visit histories
2. `01_build_condition_histories_and_cwm.R` — eligible stable conditions and
   individual-abundance CWM responses
3. `02_build_interval_mortality.R` — verified T1-risk-set mortality by interval
4. `03_select_complete_condition_histories.sql` — complete first-to-last
   condition histories
5. `04_build_cumulative_mortality.R` — cumulative lineage risk sets and deaths
6. `05_prepare_site_cwd_inputs.sql` — dates and FIA site locations
7. TerraClimate extraction — monthly `def` for the selected sites
8. `06_add_cumulative_site_cwd.sql` — cumulative site CWD and model input
9. `07_build_pooled_community_cwm.sql` — pooled live-community response
10. `08_fit_preliminary_models_and_report.R` — all 12 models and one report

Run the TerraClimate extraction after script 05:

```powershell
Rscript site_climate/scripts/extract_terraclimate_points.R `
  --input=09_analysis/data/intermediate/model_site_locations.csv `
  --output-dir=09_analysis/data/cache/terraclimate_site_cwd `
  --qa-dir=09_analysis/qa/predictors `
  --variables=def --start-year=1958 --end-year=2024
```

The R scripts use the repository `renv` library. The SQL scripts are DuckDB
scripts and should be run from the repository root.

See [methods](docs/METHODS.md), [data products](docs/PRODUCTS.md), and
[future work](docs/FUTURE_WORK.md). Model runs are indexed in
[results/model_runs/README.md](results/model_runs/README.md).

`qa/` contains small validation summaries for the active products. It is not a
second analysis pipeline. Separate live-plus-dead severity remains undefined
and is not implemented here.
