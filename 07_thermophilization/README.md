# Thermophilization Analysis

This directory consumes FIA community products and species niche products to build analysis-ready thermophilization tables.

## Current Status

The current CWM builder can use study-area species niches with a flagged global fallback. This keeps the preferred niche definition tied to the configured FIA study area while avoiding unnecessary loss of FIA-observed species whose BIEN range polygons fall outside the study-area bounding box.

## Current Workflow

1. Build species climate niches in `06_species_niches/`. The preferred primary input for this workflow is:

   ```text
   06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
   ```

   Fallback mode also uses:

   ```text
   06_species_niches/data/processed/species_climate_niches.parquet
   ```

2. Build FIA seedling community-weighted means:

   ```bash
   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
   ```

   This writes:

   ```text
   07_thermophilization/data/processed/plot_recruitment_cwm.parquet
   ```

## Script 01 Inputs

- `05_fia/data/processed/summaries/plot_seedling_species.parquet`
- `06_species_niches/data/processed/species_climate_niches_us_study_area.parquet`

The join uses FIA species codes through `species_key`, for example:

```text
SPCD 802 -> fia_spcd:802
```

## Script 01 Outputs

The output grain is:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

In FIA terms:

- `stable_plot_id` is the same plot location followed through time.
- `PLT_CN` is one FIA plot visit record.
- `INVYR` is the inventory year for that visit.
- `CONDID` is a mapped forest condition within that plot visit.
- `SUBP` is a subplot/microplot location. Script `01` aggregates subplot-level seedling species records up to the condition level before calculating CWMs.

Main climate-affinity columns:

- `cwm_temp`: recruitment community-weighted annual mean temperature niche.
- `cwm_heat`: recruitment community-weighted warmest-month temperature niche.
- `cwm_cold`: recruitment community-weighted coldest-month temperature niche.
- `cwm_temp_seasonality`: recruitment community-weighted temperature seasonality.
- `cwm_cwd`: recruitment community-weighted annual climate water deficit niche.
- `cwm_peak_cwd`: recruitment community-weighted peak monthly climate water deficit niche.
- `cwm_pr`: recruitment community-weighted annual precipitation niche.
- `cwm_dry_month_pr`: recruitment community-weighted driest-month precipitation niche.

Coverage columns such as `frac_weight_with_niche` should be used to filter or flag plot conditions where many seedlings lack species niche values. Fallback coverage columns, such as `frac_weight_with_global_fallback_niche`, identify how much of the CWM comes from global rather than study-area niche values.

## CWM Formula

For each FIA condition, the script calculates:

```text
CWM_indicator = sum(seedling_weight_i * species_indicator_i) /
                sum(seedling_weight_i)
```

where `i` indexes species in the seedling community after aggregating across subplots within the same `PLT_CN x INVYR x CONDID`.

The default `seedling_weight_i` is `seedlings_tpa`, the expanded seedlings-per-acre value from the FIA seedling species product. Sensitivity checks can use raw counts or presence/absence through the `--weight` argument.

Rows with incomplete niche coverage are retained, not silently dropped. Use:

- `frac_weight_with_niche`
- `frac_weight_with_study_area_niche`
- `frac_weight_with_global_fallback_niche`
- `frac_seedling_species_with_niche`
- `n_seedling_species_total`
- `n_seedling_species_with_niche`
- `niche_scopes_used`

to decide whether a condition is safe for modeling or should be flagged.

Known species-level gaps are documented in:

```text
06_species_niches/qa/outputs/species_niche_gap_ledger.csv
07_thermophilization/qa/outputs/plot_recruitment_cwm_missing_species.csv
```

## Smoke Tests

Limited runs write to ignored smoke folders:

```bash
Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --limit=100
```

Smoke outputs go to:

```text
07_thermophilization/data/smoke/
07_thermophilization/qa/smoke/
```

## Optional Arguments

- `--limit=N`: smoke test on the first `N` FIA condition rows.
- `--weight=seedlings_tpa`: default CWM weighting.
- `--weight=treecount_total`: raw seedling-count sensitivity check.
- `--weight=treecount_calc_total`: FIA calculated count sensitivity check.
- `--weight=presence`: species presence/absence sensitivity check.
- `--range-scope=us_study_area_with_global_fallback`: default. Use study-area niches first and global niches only for species without a study-area niche.
- `--range-scope=us_study_area`: use only study-area clipped species niches.
- `--range-scope=global`: use global BIEN range climate niches instead.
