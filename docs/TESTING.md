# QC and Validation

This project uses manual diagnostic scripts rather than an automated test suite. QC scripts are run on demand to verify pipeline outputs and explore raw data — they are **not required** for data reproduction.

---

## IDS

The most complete QC coverage is in the IDS pipeline, which has two scripts:

| Script | When to run | What it checks |
|--------|-------------|----------------|
| [`validate_ids.R`](../01_ids/scripts/qc/validate_ids.R) | After `03_clean_ids.R` | Field structure, missing data, cleaning actions, geometry validity, merge readiness |
| [`explore_ids_coverage.R`](../01_ids/scripts/qc/explore_ids_coverage.R) | After `01_download_ids.R` | Column availability and missingness by era, value distributions, regional temporal coverage |

See **[`01_ids/scripts/qc/README.md`](../01_ids/scripts/qc/README.md)** for full details on what each check verifies and what files are produced.

```bash
# Post-cleaning validation (console output only)
Rscript 01_ids/scripts/qc/validate_ids.R

# Raw data exploration (writes CSVs to 01_ids/data/processed/ids_exploration_raw/)
Rscript 01_ids/scripts/qc/explore_ids_coverage.R
```

---

## Climate Datasets (TerraClimate, PRISM, WorldClim)

No dedicated QC scripts. Data quality issues encountered during development are documented in each dataset's `cleaning_log.md`:

| Dataset | Issues documented |
|---------|------------------|
| [`02_terraclimate/cleaning_log.md`](../02_terraclimate/cleaning_log.md) | Scale factors, coastal NoData, degenerate geometries |
| [`01_ids/cleaning_log.md`](../01_ids/cleaning_log.md) | CRS issues, pancake features, field standardization |

The TerraClimate pipeline includes an optional exploration script used during development:

```bash
# Tests GEE extraction on a 100-feature sample before committing to full run
Rscript 02_terraclimate/scripts/explore/00_explore_terraclimate.R
```

---

## FIA

No dedicated QC script. The inspection script (`02_inspect_fia.R`) serves a validation-adjacent role — it verifies schema, generates lookup parquets, and prints summary statistics before the main extraction begins.

```bash
Rscript 05_fia/scripts/02_inspect_fia.R
```

---

## What Is Not Covered

| Gap | Notes |
|-----|-------|
| Climate pipeline outputs | No automated checks on pixel map completeness, pixel value ranges, or summary output row counts |
| PRISM and WorldClim | No exploration or validation scripts |
| FIA summaries | No validation of plot-level metric calculations |
| Cross-dataset consistency | No automated comparison between TerraClimate / PRISM / WorldClim values; the demo script [`scripts/demos/demo_04_compare_climate_datasets.R`](../scripts/demos/demo_04_compare_climate_datasets.R) does this manually for MPB locations |
