# QC and Validation

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This repository relies on manual diagnostics and inspection scripts rather than a formal automated test suite. This page shows what is covered, what is optional, and where the main validation gaps still are.

## IDS

IDS has the strongest QC coverage in the repo.

| Script | When to run | What it checks |
|---|---|---|
| [validate_ids.R](../01_ids/scripts/qc/validate_ids.R) | After `03_clean_ids.R` | Field structure, missing data, cleaning actions, geometry validity, and merge readiness |
| [explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R) | After `01_download_ids.R` | Column availability by era, value distributions, and regional temporal coverage |

Related docs:

- [01_ids/scripts/qc/README.md](../01_ids/scripts/qc/README.md)
- [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md)

## Climate workstreams

There is no dedicated QC script for PRISM or WorldClim.

Current checks are mainly documented in cleaning logs and exploratory notes:

| Location | What it covers |
|---|---|
| [02_terraclimate/cleaning_log.md](../02_terraclimate/cleaning_log.md) | Scale factors, coastal NoData, degenerate geometries |
| [01_ids/cleaning_log.md](../01_ids/cleaning_log.md) | IDS-side issues that affect downstream climate joins |
| [02_terraclimate/scripts/explore/00_explore_terraclimate.R](../02_terraclimate/scripts/explore/00_explore_terraclimate.R) | Small-sample GEE sanity check before full extraction |

## FIA

FIA does not currently have a standalone QC script. The closest validation step is:

- [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R), which checks schema availability and builds lookup parquets before the heavy extraction steps.

See also:

- [05_fia/README.md](../05_fia/README.md)
- [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md)

## What Is Not Covered

| Gap | Notes |
|---|---|
| Climate summary outputs | No automated checks for completeness, value ranges, or expected row counts in final summary parquets |
| PRISM and WorldClim QC | No dedicated validation or exploratory scripts beyond workflow notes |
| FIA summary calculations | No automated checks for plot-level metric correctness |
| Cross-dataset consistency | No automated comparison of TerraClimate, PRISM, and WorldClim outputs |

## Practical Review Notes

If you want a quick manual validation pass before a review:

1. Run the IDS QC scripts after the IDS foundation pipeline.
2. Spot-check one climate workstream against its `WORKFLOW.md` output descriptions.
3. Confirm expected output files using [Data Products](DATA_PRODUCTS.md).
4. Use the demo scripts in [scripts/demos/](../scripts/demos/) for sanity checks on completed outputs.

## See also

- [Docs Hub](README.md)
- [Reproduce](REPRODUCE.md)
- [IDS technical workflow](../01_ids/WORKFLOW.md)
- [Data products](DATA_PRODUCTS.md)
# Testing & QA/QC Reference

## Overview

The repo uses an R `testthat`-based suite organized per module. A single runner script
executes any combination of modules and exits with a non-zero status on failure, making
it suitable for both interactive development and release QA.

**Framework:** R `testthat`
**Runner:** `scripts/run_tests.R`
**Shared helpers:** `tests/testthat/helpers.R`

---

## Running tests

```bash
# All modules, non-strict (missing outputs skip — good for development)
Rscript scripts/run_tests.R

# All modules, strict (missing outputs fail — use for release QA)
Rscript scripts/run_tests.R --strict

# Single module
Rscript scripts/run_tests.R 05_fia

# Multiple modules
Rscript scripts/run_tests.R 02_terraclimate 05_fia

# Strict via environment variable
STRICT_OUTPUT_CHECKS=true Rscript scripts/run_tests.R
```

**Exit codes:** `0` = all pass, `1` = any failure or missing test directory.

**Strict vs non-strict:** In non-strict mode, tests that require a missing output file
call `skip()` instead of `fail()`. This lets you run the suite on a partially-built repo
without false failures. Use `--strict` when validating a complete dataset.

---

## Shared helpers (`tests/testthat/helpers.R`)

| Helper | What it does |
|--------|-------------|
| `qa_require_file(path, label)` | Skips or fails (based on mode) if a file is missing |
| `qa_require_dir(path, label)` | Skips or fails if a directory is missing |
| `qa_expect_cols(data, cols)` | Fails if any required column names are absent |
| `qa_read_parquet_head(path, n, cols)` | Reads first `n` rows from a parquet (uses Arrow lazy eval) |
| `qa_list_yearly_parquets(dir, prefix)` | Lists files matching `prefix_YYYY.parquet` |
| `qa_year_from_filename(path)` | Extracts the 4-digit year from a filename |
| `qa_expect_unique_key(data, cols, label)` | Fails if any combination of key columns appears more than once |
| `qa_calendar_to_water_year(year, month)` | Returns `water_year` and `water_year_month` using Oct–Sep convention |
| `qa_load_config()` | Loads `config.yaml`; used to get variable lists and year ranges dynamically |
| `qa_expect_year_coverage(dir, prefix, years)` | Fails if any year in the expected range has no corresponding parquet file |
| `qa_expect_value_range(data, col, lo, hi)` | Fails if any non-NA value in a column falls outside `[lo, hi]` |

---

## Module test suites

### 01_ids — `01_ids/tests/testthat/test_ids_outputs.R`

| Test | What it checks |
|------|---------------|
| Lookup tables exist and are non-empty | All 6 reference CSVs (DCA codes, host codes, damage types, percent affected, legacy severity, regions) exist and have rows |
| Cleaned geopackage has expected layers and core fields | `ids_layers_cleaned.gpkg` contains exactly the three layers (`damage_areas`, `damage_points`, `surveyed_areas`), each with the required column names |
| Derived assignment and area metrics look sane | The two derived parquets exist, have the right columns, overlap areas are non-negative, and match quality flags are only `"matched"` or `"no_survey"` |
| Feature counts and year ranges are plausible | `damage_areas` > 1M rows, `damage_points` > 100k rows, `surveyed_areas` > 10k rows; `SURVEY_YEAR` is within 1997–present; every `DCA_CODE` in the data appears in the reference lookup |

---

### 02_terraclimate — `02_terraclimate/tests/testthat/test_terraclimate_outputs.R`

| Test | What it checks |
|------|---------------|
| Pixel maps have valid geometry-to-pixel attributes | All three pixel map parquets exist; no null `pixel_id` or coordinates; `coverage_fraction` is in `(0, 1]`; point pixel maps have `coverage_fraction == 1.0` exactly |
| Yearly pixel values have expected schema and temporal keys | Spot-checks first, middle, and last year files: right column names, `year` column matches filename, `month` is 1–12, no null `pixel_id` |
| Damage area summaries are internally consistent | Every variable's summary parquet has the full schema; `n_pixels_with_data ≤ n_pixels`; water year math is correct for all rows sampled |
| Yearly pixel values have no year gaps | Every year from `config.yaml start_year` to `end_year` has a corresponding pixel values file |
| Pixel values are within physical bounds | Middle-year file spot-check: temperatures in plausible °C ranges, precipitation 0–3000 mm/month, water balance variables ≥ 0 |

---

### 03_prism — `03_prism/tests/testthat/test_prism_outputs.R`

| Test | What it checks |
|------|---------------|
| Pixel maps exist with valid coverage fractions | Same as TerraClimate pixel map checks; PRISM is CONUS-only (Alaska/Hawaii excluded) |
| Yearly pixel values have expected schema and key ranges | Same pattern as TerraClimate |
| Summaries are internally consistent | Same pattern as TerraClimate |
| Yearly pixel values have no year gaps | Full year coverage from config |
| Pixel values are within physical bounds | Temperature (°C), precipitation (mm), VPD (kPa) within plausible CONUS ranges |

---

### 04_worldclim — `04_worldclim/tests/testthat/test_worldclim_outputs.R`

| Test | What it checks |
|------|---------------|
| Pixel maps exist with valid coverage fractions | Same as TerraClimate; WorldClim is global |
| Yearly pixel values have expected schema and key ranges | Same pattern |
| Summaries are internally consistent | Same pattern |
| Yearly pixel values have no year gaps | Full year coverage from config |
| Pixel values are within physical bounds | tmin/tmax (°C), prec (mm/month) within plausible global ranges |

---

### 05_fia — `05_fia/tests/testthat/test_fia_outputs.R`

| Test | What it checks |
|------|---------------|
| Lookup and partitioned extraction outputs exist | `ref_species` and `ref_forest_type` parquets have required columns; the four per-state directories (`trees`, `cond`, `seedlings`, `mortality`) exist and contain parquet files |
| National summary files exist with expected schema | All 8 summary parquets exist, have required columns, are non-empty; `plot_tree_metrics` and `plot_exclusion_flags` have no duplicate `(PLT_CN, INVYR)` keys |
| Site pixel map preserves global TC pixel semantics | Every site from `all_site_locations.csv` has a pixel mapping with no nulls and `coverage_fraction == 1.0`; each `pixel_id` matches what `terra::cellFromXY()` returns on the global TC raster; snapped x/y coordinates are within half a pixel of the original site coordinates. This specifically guards against the region-limited raster bug documented in `05_fia/cleaning_log.md` Issue #001 |
| Site climate extraction has expected keys and time semantics | Schema present; months 1–12; all 6 variables present; no duplicate `(site_id, year, month, variable)` keys; water year arithmetic is correct |
| Site climate is complete and values are within physical bounds | Row count equals `n_sites × n_years × 12 × 6` — no silent missing combinations; tmmx/tmmn in plausible °C ranges, pr ≥ 0 and ≤ 3000 mm/month |

---

## Adding new tests

1. Create a test file at `<module>/tests/testthat/test_<module>_outputs.R`
2. Source the shared helpers at the top: `source(here::here("tests/testthat/helpers.R"))`
3. Use `qa_require_file()` / `qa_require_dir()` for all output existence checks so they respect strict mode
4. For any point-based pixel mapping test, validate pixel IDs against the global TC raster (not a region-limited one) — see the 05_fia pixel map test for the pattern
5. Register the module name in `scripts/run_tests.R` under `available_modules`
