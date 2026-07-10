# Testing And QA

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page summarizes the repository's automated tests and workflow-specific QA scripts. Use it to decide which checks to run before sharing outputs, moving data to a server, or starting modeling.

## Test Runner

The general R test suite uses `testthat` and is run through:

```bash
Rscript scripts/run_tests.R
Rscript scripts/run_tests.R --strict
Rscript scripts/run_tests.R 05_fia
```

Modes:

- Non-strict mode skips tests that require missing local outputs. Use this during development or on a partially built repo.
- Strict mode treats missing expected outputs as failures. Use this when validating a complete local or server build.

Shared test helpers live in `tests/testthat/helpers.R`. They check common conditions such as required files, required columns, unique keys, value ranges, and year coverage.

## Workflow QA Scripts

| Workstream | QA script or location | When to run | What it checks |
|---|---|---|---|
| IDS | [validate_ids.R](../01_ids/scripts/qc/validate_ids.R) | After `01_ids/scripts/03_clean_ids.R` | IDS layer structure, cleaning actions, geometry validity, missing data, and merge readiness |
| IDS | [explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R) | After `01_ids/scripts/01_download_ids.R` or when source data change | Schema availability by era, value distributions, and temporal coverage |
| FIA site climate | [03_validate_site_climate.R](../05_fia/scripts/site_climate/03_validate_site_climate.R) | After optional FIA site-climate extraction | Site/pixel matching, month/year completeness, and basic value checks |
| Species niches | [01_validate_species_niche_workflow.R](../06_species_niches/qa/01_validate_species_niche_workflow.R) | After scripts `01`-`03`, and again after scripts `04`-`05` | Handoffs among species universe, BIEN availability, polygons, range climate, compact niches, and CWM coverage |
| Species niches | Gap documentation scripts in [06_species_niches/qa/](../06_species_niches/qa/) | After the niche products are current | Missing-data categories, taxonomic review queues, study-area climate gaps, global fallback species, and taxon-level coverage |
| Thermophilization | [01_validate_thermophilization_products.R](../07_thermophilization/qa/01_validate_thermophilization_products.R) | After rebuilding thermophilization scripts `01`-`06` | File presence, documented row grains, required columns, valid proportions, niche coverage fields, and repeated-survey rate calculations |

## Module Test Suites

| Module | Test file | Main checks |
|---|---|---|
| IDS | `01_ids/tests/testthat/test_ids_outputs.R` | Lookup files, cleaned geopackage layers, IDS assignment products, year ranges, geometry fields, and code lookup consistency |
| TerraClimate | `02_terraclimate/tests/testthat/test_terraclimate_outputs.R` | Pixel maps, yearly pixel-value files, damage-area summaries, year coverage, and plausible value ranges |
| PRISM | `03_prism/tests/testthat/test_prism_outputs.R` | Pixel maps, yearly pixel-value files, summary schema, year coverage, and plausible CONUS climate ranges |
| WorldClim | `04_worldclim/tests/testthat/test_worldclim_outputs.R` | Pixel maps, yearly pixel-value files, summary schema, year coverage, and plausible global climate ranges |
| FIA | `05_fia/tests/testthat/test_fia_outputs.R` | FIA lookup products, partitioned extracts, national summaries, site pixel maps, site climate completeness, and value ranges |

## QA Outputs To Read First

| Workstream | Start with | Why |
|---|---|---|
| Species niches | `06_species_niches/qa/outputs/species_niche_validation_decision.csv` | One-line workflow gate for structural blockers and unresolved warnings |
| Species niches | `06_species_niches/qa/outputs/species_niche_gap_summary.csv` | Plain-language counts of species with usable niches and missing-data reasons |
| Species niches | `06_species_niches/qa/outputs/species_taxon_resolution_summary.csv` | Deduplicated biological-taxon counts, not just source-code counts |
| Thermophilization | `07_thermophilization/qa/outputs/thermophilization_validation_summary.csv` | One-line structural validation summary for the current thermophilization products |
| Thermophilization | `07_thermophilization/qa/outputs/plot_year_community_cwm_summary_<layer>.csv` | Plot-year CWM row counts, niche coverage, missing species, and fallback usage |
| Thermophilization | `07_thermophilization/qa/outputs/plot_year_climate_change_summary_<layer>.csv` | Repeated-survey interval counts, coverage threshold counts, and disturbance interval counts |

## Known QA Gaps

These checks are useful future additions:

- Recompute a random sample of plot-year CWMs directly from source species rows and species niches, then compare against `plot_year_community_cwm_<layer>.parquet`.
- Spot-check disturbance-proportion aggregation directly from condition rows and compare against `plot_disturbance_severity.parquet`.
- Add dedicated PRISM and WorldClim validation scripts comparable to the IDS and species-niche validators.
- Add automated checks for cross-dataset consistency among TerraClimate, PRISM, and WorldClim summaries.

## Practical Review Sequence

Before using outputs for modeling or external review:

1. Run the relevant production scripts from [Reproduce](REPRODUCE.md).
2. Run the module-specific QA gate for the workstream.
3. Run `Rscript scripts/run_tests.R --strict` if the complete expected output set exists locally.
4. Inspect the QA summary files listed above.
5. Confirm product locations and row meanings in [Data Products](DATA_PRODUCTS.md) and the relevant module README.
