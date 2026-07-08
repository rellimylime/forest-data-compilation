# Thermophilization Analysis Walkthrough

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Thermophilization Plan](thermophilization_plan.md) | [Species Niches](../06_species_niches/README.md) | [Thermophilization Workflow](../07_thermophilization/README.md)

## Overview

This walkthrough explains how the current repository turns FIA community data
and BIEN/TerraClimate species niches into analysis-ready thermophilization
inputs. It is intended as a technical orientation for collaborators who need to
understand what each table represents before extending the analysis.

The active thermophilization implementation currently contains two production
scripts:

| Step | Script | Main output |
|---|---|---|
| 1 | [01_build_plot_recruitment_cwm.R](../07_thermophilization/scripts/01_build_plot_recruitment_cwm.R) | `plot_recruitment_cwm.parquet` |
| 2 | [02_build_analysis_cohort.R](../07_thermophilization/scripts/02_build_analysis_cohort.R) | `plot_recruitment_analysis_cohort.parquet` |

Matched-control modeling and final inferential summaries are planned next steps.
They should not be treated as completed results until scripts and QA outputs are
committed.

## Conceptual Flow

```text
FIA seedling species records
        +
BIEN/TerraClimate species niche indicators
        |
        v
Condition-level recruitment community-weighted means
        +
FIA disturbance and treatment classification
        |
        v
Analysis cohort of disturbed and control candidates
        |
        v
Planned matched-control analysis
```

## Key FIA Grains

FIA data are hierarchical. The current workflow keeps this structure explicit.

| Grain | Meaning | Important fields |
|---|---|---|
| Stable plot | A physical plot followed through time | `stable_plot_id` |
| Plot visit | One inventory record for a plot | `PLT_CN`, `INVYR` |
| Condition | A mapped land/forest condition inside a plot visit | `PLT_CN`, `INVYR`, `CONDID` |
| Subplot or microplot | Within-plot sampling unit | `SUBP` |
| Species record | A species observed within a sampling unit | `SPCD` or P2VEG symbol |

The thermophilization input tables are currently built at:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

This condition-level grain is used because different conditions within the same
plot visit can have different forest types, disturbance histories, or treatment
histories.

## Step 1: Species Climate Niches

Species climate niches are built in `06_species_niches/`.

The core idea is to represent each resolved species by the climate across its
BIEN range map. TerraClimate is overlaid on the BIEN polygon and summarized for
the 1981-2010 baseline period.

The compact niche table contains eight indicators:

- `temp`: annual mean temperature
- `heat`: warmest-month temperature
- `cold`: coldest-month temperature
- `temp_seasonality`: temperature seasonality
- `cwd`: annual climate water deficit
- `peak_cwd`: peak monthly climate water deficit
- `pr`: annual precipitation
- `dry_month_pr`: driest-month precipitation

For details, see:

- [Species Niche Workflow](../06_species_niches/WORKFLOW.md)
- [Species Niche Methods](../06_species_niches/docs/methods_species_niches.md)
- [Species Niche QA Guide](../06_species_niches/qa/README.md)

## Step 2: Seedling Recruitment CWM

Script:

```bash
Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
```

Main inputs:

- `05_fia/data/processed/summaries/plot_seedling_species.parquet`
- `06_species_niches/data/processed/species_climate_niches_us_study_area.parquet`
- `06_species_niches/data/processed/species_climate_niches.parquet` when using global fallback mode

Main output:

```text
07_thermophilization/data/processed/plot_recruitment_cwm.parquet
```

### What The CWM Represents

A community-weighted mean (CWM) is an abundance-weighted species trait average.
For each FIA condition:

```text
CWM_indicator =
  sum(seedling_weight_species * species_niche_indicator_species) /
  sum(seedling_weight_species)
```

The default weight is `seedlings_tpa`, the FIA expanded seedlings-per-acre value.
The script also supports sensitivity runs with raw counts or presence/absence.

### Output Contents

Each row represents one FIA condition visit:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

Important columns include:

- `cwm_temp`
- `cwm_heat`
- `cwm_cold`
- `cwm_temp_seasonality`
- `cwm_cwd`
- `cwm_peak_cwd`
- `cwm_pr`
- `cwm_dry_month_pr`
- `frac_weight_with_niche`
- `frac_weight_with_study_area_niche`
- `frac_weight_with_global_fallback_niche`
- `n_seedling_species_total`
- `n_seedling_species_with_niche`

Coverage fields are retained so downstream analyses can filter or flag
conditions with incomplete niche coverage instead of silently dropping them.

## Step 3: Analysis Cohort

Script:

```bash
Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R
```

Main inputs:

- `07_thermophilization/data/processed/plot_recruitment_cwm.parquet`
- `05_fia/data/processed/summaries/plot_disturbance_classification.parquet`
- `05_fia/data/processed/summaries/plot_exclusion_flags.parquet`

Main output:

```text
07_thermophilization/data/processed/plot_recruitment_analysis_cohort.parquet
```

### Eligibility Logic

The cohort keeps FIA condition rows that:

1. Have a usable recruitment CWM.
2. Are forested conditions.
3. Are either natural-disturbance candidates or control candidates.

Whole-plot exclusion flags are retained as warning fields rather than always
removing the condition. This allows later sensitivity analyses to test whether
results change when stricter plot-level exclusions are imposed.

### Current Cohort Counts

The current QA summary reports:

| Category | Rows |
|---|---:|
| Final cohort | 410,420 |
| Controls | 349,916 |
| Disturbed candidates | 60,504 |
| Rows meeting 95% niche coverage | 371,693 |
| Rows below 95% niche coverage | 38,727 |

Disturbance-class counts:

| Disturbance class | Rows |
|---|---:|
| none/control | 349,916 |
| insect | 16,452 |
| other | 11,950 |
| disease | 11,437 |
| fire | 10,537 |
| weather | 10,128 |

These counts describe the analysis cohort only. They are not estimates of a
disturbance effect.

## Step 4: Planned Matched-Control Table

The next analysis step should build a matched-control table. The recommended
grain is one row per disturbed-control pair:

```text
disturbed_PLT_CN x disturbed_INVYR x disturbed_CONDID
control_PLT_CN x control_INVYR x control_CONDID
match_rank
```

Recommended matching constraints:

- Same or similar forest type.
- Same region or ecoregion.
- Similar inventory period.
- Similar baseline climate.
- Sufficient niche coverage.

The table should retain:

- Disturbed and control CWM values.
- `delta_cwm_* = disturbed - control`.
- Match distance and match rank.
- Forest type, region, disturbance class, and inventory year.
- Niche coverage for both sides.

## Step 5: Planned Effect Summaries

After matching, effect summaries should aggregate pair-level deltas by:

- Disturbance class
- Region or ecoregion
- Forest type group
- Time since disturbance, where disturbance year is reliable

Primary responses:

- `delta_cwm_temp`
- `delta_cwm_cwd`
- `delta_cwm_pr`

The sign convention should always be:

```text
disturbed - matched control
```

Positive `delta_cwm_temp` indicates warmer-niche recruitment on disturbed
conditions relative to matched controls. Positive `delta_cwm_cwd` indicates
recruitment weighted toward species associated with higher climate water deficit.

## QA Files To Read First

Species niche status:

- `06_species_niches/qa/outputs/species_niche_validation_decision.csv`
- `06_species_niches/qa/outputs/species_niche_validation_summary.csv`
- `06_species_niches/qa/outputs/species_taxon_resolution_summary.csv`
- `06_species_niches/qa/outputs/study_area_climate_gap_summary.csv`

Thermophilization status:

- `07_thermophilization/qa/outputs/plot_recruitment_cwm_summary.csv`
- `07_thermophilization/qa/outputs/plot_recruitment_cwm_missing_species.csv`
- `07_thermophilization/qa/outputs/analysis_cohort_attrition.csv`
- `07_thermophilization/qa/outputs/analysis_cohort_summary.csv`

## Interpretation Boundaries

The current repository supports statements about:

- Which FIA conditions have recruitment CWMs.
- Which species niche gaps affect CWM coverage.
- Which conditions are currently eligible as disturbed or control candidates.
- How many rows are retained or removed by cohort filters.

The current repository does not yet support final statements about:

- Whether disturbance increases or decreases recruitment thermophilization.
- Whether one region has a stronger effect than another.
- Whether a specific disturbance class has a statistically reliable effect.

Those claims require the planned matching/modeling scripts and associated QA
outputs.

## Schema Reference

### `plot_recruitment_cwm.parquet`

- **Path:** `07_thermophilization/data/processed/plot_recruitment_cwm.parquet`
- **Grain:** one row per FIA condition visit
- **Primary key:** `PLT_CN`, `INVYR`, `CONDID`
- **Key role:** recruitment climate-affinity response table

### `plot_recruitment_analysis_cohort.parquet`

- **Path:** `07_thermophilization/data/processed/plot_recruitment_analysis_cohort.parquet`
- **Grain:** one row per eligible FIA condition visit
- **Primary key:** `PLT_CN`, `INVYR`, `CONDID`
- **Key role:** pool of disturbed and control candidates for matching

### `plot_disturbance_classification.parquet`

- **Path:** `05_fia/data/processed/summaries/plot_disturbance_classification.parquet`
- **Grain:** one row per FIA condition visit
- **Primary key:** `PLT_CN`, `INVYR`, `CONDID`
- **Key role:** disturbance class and control eligibility

### `species_climate_niches*.parquet`

- **Path:** `06_species_niches/data/processed/`
- **Grain:** one row per resolved species/taxon
- **Key role:** species-level climate niche indicators used in CWM calculation
