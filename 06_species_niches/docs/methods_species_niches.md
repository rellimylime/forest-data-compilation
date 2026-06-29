# Species Niche Methods Notes

**Navigation:** [Module README](../README.md) | [Technical Workflow](../WORKFLOW.md) | [QA Guide](../qa/README.md) | [Thermophilization](../../07_thermophilization/README.md)

This document is the working methods note for the species climate niche table.

## Current Method

The production method uses BIEN range maps overlaid with TerraClimate. The
earlier FIA-occurrence climate-affinity approach has been retired, and a
GBIF/CHELSA point-occurrence comparison remains a possible future sensitivity
analysis.

Canonical global target:

```text
06_species_niches/data/processed/species_climate_niches.parquet
```

Recommended FIA-facing target:

```text
06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
```

The manuscript-facing version should use BIEN species range polygons overlaid with TerraClimate. The repo keeps both a global BIEN range version and a study-area-clipped version.

## Range Scope

BIEN range maps can be global or transcontinental, especially for cosmopolitan P2VEG species and introduced plants. To keep the FIA thermophilization analysis focused on the climate space relevant to U.S. plots, script 04 accepts:

```text
--range-scope=global
--range-scope=us_study_area
```

`global` preserves the full BIEN polygons. `us_study_area` clips BIEN polygons to the configured all-U.S. study-area bounding box in `config.yaml` (`params.study_area`, including Alaska and Hawaii). This is a bounding-box clip, not a political-boundary clip, so it should be described as "study-area clipped" rather than "U.S. boundary clipped."

The downstream FIA CWM product uses `us_study_area_with_global_fallback` by default. In that mode, study-area niche values are preferred, and global BIEN niche values are used only for FIA-observed species without study-area niche values. This avoids dropping FIA-observed species solely because BIEN maps their range outside the configured study-area bounding box, while preserving flags that identify how much of each CWM comes from global fallback values.

## Compact Niche Indicators

The range overlay keeps monthly climate summaries in `species_range_climate*.parquet`, but downstream CWM analyses should consume a small, interpretable set of species indicators from `species_climate_niches*.parquet`.

The current compact table uses eight indicators:

| Indicator | Interpretation |
| --- | --- |
| `tmean_annual_mean` | Species thermal niche center. |
| `tmean_warmest_month_mean` | Warm-season heat exposure across the range. |
| `tmean_coldest_month_mean` | Cold-limit exposure across the range. |
| `temp_seasonality_mean` | Annual thermal range, warmest minus coldest month. |
| `cwd_annual_sum` | Annual climate water deficit, primary drought-affinity axis. |
| `cwd_max_month_mean` | Peak monthly water stress. |
| `pr_annual_sum` | Annual precipitation supply. |
| `pr_driest_month_mean` | Dry-season precipitation limit. |

These are derived from the spatial mean climate values across each BIEN range scope. The p10, p50, and p90 spatial summaries remain in `species_range_climate*.parquet` for sensitivity checks.

Formulas:

```text
tmean_annual_mean        = mean(monthly tmean means across months 1-12)
tmean_warmest_month_mean = max(monthly tmean means across months 1-12)
tmean_coldest_month_mean = min(monthly tmean means across months 1-12)
temp_seasonality_mean    = tmean_warmest_month_mean - tmean_coldest_month_mean

cwd_annual_sum           = sum(monthly def means across months 1-12)
cwd_max_month_mean       = max(monthly def means across months 1-12)

pr_annual_sum            = sum(monthly pr means across months 1-12)
pr_driest_month_mean     = min(monthly pr means across months 1-12)
```

`def` is TerraClimate climate water deficit. In this workflow `cwd` and `def` refer to the same drought-affinity axis: water demand not met by actual evapotranspiration. Higher values mean the species' mapped range occupies climates with greater water deficit.

These formulas produce fixed species traits. They are not recalculated by FIA inventory year. The downstream thermophilization analysis asks whether the species composition observed on FIA plots has a higher or lower community-weighted value of these fixed traits.

## Grain Definitions

The word "grain" means the unit represented by one row.

| Product | Grain | Meaning |
| --- | --- | --- |
| `species_universe.parquet` | one row per source species code | Master list of species-like records seen in FIA tree/seedling/sapling data and P2VEG understory data. |
| `bien_range_availability.parquet` | one row per target species | Whether a BIEN range exists for the queried species name. |
| `species_range_polygons.gpkg` | species polygon features | Spatial BIEN range geometries, with project species keys attached. |
| `species_range_climate_us_study_area.parquet` | species x month x variable x metric | Monthly TerraClimate summaries over the range. |
| `species_climate_niches_us_study_area.parquet` | one row per species | Compact species-level niche traits used by CWM scripts. |
| `plot_recruitment_cwm.parquet` | FIA stable plot x plot visit x condition | FIA seedling community-weighted climate affinities, built downstream in `07_thermophilization`. |

FIA condition-level products should not be confused with subplot-level products. `CONDID` identifies a mapped forest condition within a FIA plot visit. `SUBP` identifies a subplot or microplot location. The thermophilization CWM currently aggregates seedling species from subplots up to the `PLT_CN x INVYR x CONDID` condition grain before calculating community-weighted means.

## Source Roles

| Source | Role |
| --- | --- |
| FIA | Defines the species universe and downstream response data, not the species range-climate source. |
| BIEN range maps | Primary range source for species climate niches. |
| TerraClimate | Climate raster source for both species range overlays and FIA plot climate histories. |
| GBIF/CHELSA | Deferred point-occurrence sensitivity check against BIEN range estimates. |
| USDA PLANTS / USFS Little maps / BONAP | Name and range sanity checks, not primary climate summaries. |

## Taxonomic Name Review

BIEN range lookup depends on scientific names, while FIA and P2VEG species
codes may use older names, synonyms, infraspecific taxa, or genus-level
records. Product fields and workflow handoffs are documented in
[../WORKFLOW.md](../WORKFLOW.md); QA and missing-data interpretation are
documented in [../qa/README.md](../qa/README.md).

The short version:

- TNRS is used as a first-pass name-resolution tool for BIEN-missing species.
- TNRS results are evidence, not automatic replacements.
- A reviewed override is only pipeline-ready when it is listed in `06_species_niches/lookups/manual_bien_name_overrides_reviewed.csv` with   `review_status = ready_for_pipeline`.
- Genus-level `sp.` / `spp.` observations are excluded from the main species-level CWM and tracked through coverage/gap QA.
- Infraspecific `var.`, `ssp.`, and `subsp.` records are legitimate FIA/NRCS taxonomic ranks, not genus-level pseudo taxa. They should be queried exactly first. They should not be counted as TNRS/BIEN rescue candidates unless a parent-species fallback is explicitly reviewed and approved, because that fallback broadens the niche assignment.
- Ambiguous high-impact names, such as old forestry names that map to multiple modern taxa, remain flagged until an ecological/taxonomic decision is made.

## QA Belongs Here, Not In The Analysis

The thermophilization scripts should consume only a vetted niche table. BIEN range lookup, polygon validity, TerraClimate overlay coverage, and niche ranking sanity checks should be run and documented in this module before the analysis is rerun.

The formal validation entry point is:

```bash
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
```

This validator checks that the species universe has unique keys, genus-only records are not targeted for species-level niches, BIEN availability has exactly one row per niche target, BIEN availability includes both available and missing ranges, range polygons match the BIEN-available species set, geometries are non-empty and valid, and downstream range-climate / compact-niche / CWM products match the current polygon species set.

Validation results are split into `error` and `warning` severity. Any failed `error` check means the workflow is structurally broken and should not proceed. A failed `warning` check means the issue must be reviewed, justified, or fixed before final modeling.

The validator also writes a decision table:

```text
06_species_niches/qa/outputs/species_niche_validation_decision.csv
```

Use `proceed_to_script_04` as the gate for beginning or resuming the long TerraClimate extraction. Use `proceed_to_modeling` as the gate for final thermophilization analysis. The gap-ledger script intentionally refuses to write final gap documentation when the range-climate or compact niche products are stale relative to the current BIEN polygons.

Coverage and variability should be reviewed from:

```text
06_species_niches/qa/outputs/species_niche_coverage_by_source.csv
06_species_niches/qa/outputs/species_niche_coverage_by_layer.csv
06_species_niches/qa/outputs/bien_missing_species_ranked_by_abundance.csv
```

These tables make clear that BIEN coverage is not identical across FIA tree codes and P2VEG plant codes, or across seedlings, trees, shrubs, forbs, and graminoids. That variability is expected, but high-abundance missing species need manual review before final modeling.

Current QA files to inspect before modeling:

```text
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/species_niche_gap_ledger.csv
06_species_niches/qa/outputs/species_niche_top_cwm_gaps.csv
06_species_niches/qa/outputs/species_range_climate_failures_us_study_area.csv
06_species_niches/qa/outputs/species_climate_niches_missing_us_study_area.csv
07_thermophilization/qa/outputs/plot_recruitment_cwm_missing_species.csv
```

Live counts and warning states are intentionally not repeated in this methods
document because they change after reruns. Use these generated files as the
authoritative current status:

```text
06_species_niches/qa/outputs/species_niche_validation_decision.csv
06_species_niches/qa/outputs/species_niche_validation_summary.csv
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/study_area_climate_gap_summary.csv
```

High-weight downstream gaps should be reviewed before final models. The gap
ledger and action queue prioritize those records using their observed
community contribution.
