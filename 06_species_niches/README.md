# Species Climate Niches

This module builds species-level climate niche values for the thermophilization analysis. In plain language: it asks what climate each species' mapped BIEN range occupies, then turns that into a small set of climate indicators that can be joined to FIA species communities.

The preferred product for the current FIA analysis is:

```text
06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
```

This is the BIEN range-map niche table clipped to the configured all-U.S. study area bounding box.

## Current Rerun Status

Scripts `01` through `03` were rerun locally on June 18, 2026 and now form the current source of truth for the species universe, BIEN availability table, and BIEN range polygon product.

Current regenerated counts:

| Product | Current count |
| --- | ---: |
| Species universe | 6,554 species-like records |
| Species targeted for BIEN niche lookup | 5,894 records |
| BIEN ranges available | 4,214 records |
| BIEN ranges missing | 1,680 records |
| Consolidated BIEN range polygons | 4,214 species |

The downstream TerraClimate and compact niche products from scripts `04` and `05` still need to be regenerated from this corrected 4,214-species polygon set. Until that is done, the older `species_range_climate*.parquet`, `species_climate_niches*.parquet`, and thermophilization CWM products should be treated as stale.

Script `04` uses cached batch outputs so reruns can continue without restarting completed batches. The batch cache now includes a hash of the ordered species set, so batches from an older species universe cannot be accidentally reused.

## Workflow

Run scripts in order:

```bash
Rscript 06_species_niches/scripts/01_build_species_universe.R
Rscript 06_species_niches/scripts/02_check_bien_ranges.R
Rscript 06_species_niches/scripts/03_download_bien_ranges.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --range-scope=us_study_area
Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --range-scope=us_study_area
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
```

Smoke tests use `--limit=N` and write to ignored smoke folders:

```text
06_species_niches/data/smoke/
06_species_niches/qa/smoke/
```

## Script Outputs And Grains

| Script | Main output | Grain | Plain-language meaning |
| --- | --- | --- | --- |
| `01_build_species_universe.R` | `species_universe.parquet` | One row per source species code | Master list of FIA/P2VEG species that could need a climate niche. Includes trees, seedlings/saplings, shrubs, forbs, grasses, and P2VEG tree layers. |
| `02_check_bien_ranges.R` | `bien_range_availability.parquet` | One row per target species | Whether BIEN reports a range map for the species name we queried. |
| `03_download_bien_ranges.R` | `species_range_polygons.gpkg` | One polygon feature set per species, consolidated | BIEN range polygons downloaded locally and standardized enough for climate extraction. |
| `qa/01_validate_species_niche_workflow.R` | `species_niche_validation_checks.csv` | One row per validation check | Structural and freshness checks across the species universe, BIEN availability, polygons, climate extraction, compact niches, and CWM consumers. |
| `04_extract_terraclimate_from_ranges.R` | `species_range_climate_us_study_area.parquet` | Species x month x variable x range metric | Monthly TerraClimate summaries over each BIEN range polygon after study-area clipping. Must be rerun after the June 18 `01`-`03` regeneration. |
| `05_build_species_climate_niches.R` | `species_climate_niches_us_study_area.parquet` | One row per species | Compact species climate indicators used as traits in downstream CWM analysis. Must be rerun after script `04`. |
| `qa/04_document_species_niche_gaps.R` | `species_niche_gap_ledger.csv` | One row per species in the universe | Human-auditable list of whether each species has a usable niche and, if not, why not. |

## What The Product Columns Mean

### `species_universe.parquet`

This table defines the species universe before BIEN is involved.

Important columns:

- `species_key`: stable project join key, such as `fia_spcd:802` or `p2veg:ACRU`.
- `source_code_system`: where the source code comes from, such as `fia_spcd` or `p2veg`.
- `source_species_code`: FIA `SPCD` or P2VEG plant code.
- `scientific_name`, `common_name`: names used for human review and BIEN lookup.
- `community_layers`: where the species appears in the FIA/P2VEG data, such as `seedling`, `tree`, `shrub`, `forb`, or `graminoid`.
- `is_pseudo_taxon`: `TRUE` for records like `Amelanchier spp.` or `Tree unknown` that are not a clean binomial species.
- `needs_niche`: `TRUE` when the record is specific enough to attempt a species-level niche.

### `bien_range_availability.parquet`

This table records whether BIEN had a range map for each targeted species.

Important columns:

- `bien_query_name`: binomial name submitted to BIEN, with spaces converted to underscores.
- `bien_range_available`: whether BIEN reported a range map.
- `range_lookup_status`: `available` or `no_range`.
- `range_match_status`: matching status used by downstream scripts.
- `needs_range_review`: `TRUE` when the species could not be matched to a BIEN range.
- `range_review_reason`: plain-language reason for review, usually that BIEN did not report a range map.

### `species_range_climate_us_study_area.parquet`

This is the range-climate overlay output.

Grain:

```text
species_key x month x variable x metric
```

Important columns:

- `month`: calendar month, 1-12.
- `variable`: TerraClimate variable, such as `tmean`, `pr`, or `def`.
- `metric`: spatial summary over the BIEN range polygon. Current compact niches use `mean`; p10/p50/p90 remain available for sensitivity checks.
- `value`: climate value for that species, month, variable, and metric.
- `climate_period`: currently `1981-2010`.
- `range_scope`: currently `us_study_area` for the recommended analysis product.

### `species_climate_niches_us_study_area.parquet`

This is the compact species trait table used by thermophilization scripts.

Grain:

```text
one row per species_key
```

Indicators:

- `tmean_annual_mean`: mean of monthly mean temperature values across the year.
- `tmean_warmest_month_mean`: warmest monthly mean temperature.
- `tmean_coldest_month_mean`: coldest monthly mean temperature.
- `temp_seasonality_mean`: `tmean_warmest_month_mean - tmean_coldest_month_mean`.
- `cwd_annual_sum`: sum of monthly climate water deficit.
- `cwd_max_month_mean`: maximum monthly climate water deficit.
- `pr_annual_sum`: sum of monthly precipitation.
- `pr_driest_month_mean`: minimum monthly precipitation.

These are species traits. They do not change by FIA plot or inventory year. They describe the realized climate envelope of the species' BIEN range during the 1981-2010 baseline climate period.

## Current Gap Summary

The current gap ledger is produced by:

```bash
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
```

Current summary from the regenerated `01` through `03` products:

| Gap stage | Species count | Meaning |
| --- | ---: | --- |
| BIEN ranges available | 4,214 | These species can proceed to TerraClimate extraction. |
| BIEN ranges missing | 1,680 | BIEN did not report a range map for the queried name. |
| Not targeted for BIEN niche lookup | 660 | Pseudo taxa, genus-only records, unknown categories, or other records that are not clean species-level niche targets. |

Detailed files:

- `06_species_niches/qa/outputs/species_niche_gap_ledger.csv`
- `06_species_niches/qa/outputs/species_niche_gap_summary.csv`
- `06_species_niches/qa/outputs/species_niche_top_cwm_gaps.csv`

Those `species_niche_gap_*` files should be regenerated after scripts `04` and `05` are rerun, because the current versions still reflect the older niche products.

## Required Validation Before Modeling

Run:

```bash
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
```

Detailed QA instructions live in [qa/README.md](qa/README.md).

Core outputs:

- `06_species_niches/qa/outputs/species_niche_validation_checks.csv`
- `06_species_niches/qa/outputs/species_niche_validation_summary.csv`
- `06_species_niches/qa/outputs/species_niche_product_manifest.csv`
- `06_species_niches/qa/outputs/species_niche_validation_decision.csv`
- `06_species_niches/qa/outputs/species_niche_coverage_by_source.csv`
- `06_species_niches/qa/outputs/species_niche_coverage_by_layer.csv`
- `06_species_niches/qa/outputs/bien_missing_species_ranked_by_abundance.csv`

Validation rules:

- Any `severity = error` and `status = fail` means the workflow is structurally broken and should not proceed.
- `severity = warning` means the product can exist, but the issue must be reviewed or resolved before final modeling.
- `proceed_to_script_04 = TRUE` means scripts `01` through `03` are consistent enough to begin or resume TerraClimate extraction.
- `proceed_to_modeling = TRUE` means no unresolved validator warnings remain.
- Current validation passes all required upstream checks but warns that the old range-climate, compact-niche, and CWM products are stale relative to the regenerated `01`-`03` products.

Current warning files to inspect:

- `bien_polygons_nonpositive_area.csv`
- `range_climate_stale_species.csv`
- `range_climate_missing_polygon_species.csv`
- `compact_niche_stale_species.csv`
- `compact_niche_missing_polygon_species.csv`

## How To Interpret Gaps

Not every gap has the same importance.

- Pseudo taxa such as `Amelanchier spp.` do not have a species-level climate niche because the taxonomic unit is broader than a species. These should be excluded from species-niche CWMs or handled with a documented genus-level sensitivity analysis.
- Name mismatches or synonym issues, such as `Quercus prinus` vs accepted alternatives, should be reviewed manually if they carry high seedling weight.
- Species without BIEN range maps can be left missing if they are rare in the response data, but high-weight species should either get a manual name fix, a different range source, or a documented exclusion.
- Study-area clipped gaps are usually not important for FIA thermophilization if they do not occur materially in the FIA seedling response.

Downstream scripts must keep coverage columns, such as `frac_weight_with_niche`, so models can filter or flag plot conditions whose community-weighted means rely on incomplete species niche coverage.
