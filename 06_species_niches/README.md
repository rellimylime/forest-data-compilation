# Species Climate Niches

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Technical Workflow](WORKFLOW.md) | [Methods](docs/methods_species_niches.md) | [QA Guide](qa/README.md) | [Data Products](../docs/DATA_PRODUCTS.md) | [Thermophilization](../07_thermophilization/README.md)

## What This Workstream Does

`06_species_niches/` builds one set of climate-niche traits for each resolved
plant taxon represented in FIA or P2VEG communities. BIEN supplies range maps,
and TerraClimate supplies the 1981-2010 monthly climate summarized across those
ranges.

The resulting traits can be joined to seedling, sapling, tree, shrub, forb, and
graminoid communities. The traits describe species, not individual FIA plots
or inventory years.

## Workflow At A Glance

```mermaid
flowchart LR
  A[FIA and P2VEG taxa] --> B[Species universe]
  B --> C[BIEN range check]
  C --> D[BIEN polygons]
  D --> E[TerraClimate overlay]
  E --> F[Compact niche traits]
  F --> G[Thermophilization CWMs]
```

## Production Scripts

| Step | Script | Main result |
|---|---|---|
| 1 | [01_build_species_universe.R](scripts/01_build_species_universe.R) | Master source-species inventory |
| 2 | [02_check_bien_ranges.R](scripts/02_check_bien_ranges.R) | BIEN range availability and name-match audit |
| 3 | [03_download_bien_ranges.R](scripts/03_download_bien_ranges.R) | Consolidated BIEN range polygons |
| 4 | [04_extract_terraclimate_from_ranges.R](scripts/04_extract_terraclimate_from_ranges.R) | Monthly climate summaries across ranges |
| 5 | [05_build_species_climate_niches.R](scripts/05_build_species_climate_niches.R) | Eight compact species climate indicators |

Detailed inputs, processing choices, rerun behavior, and QA outputs are in
[WORKFLOW.md: Script Details](WORKFLOW.md#script-details).

## Quick Start

```bash
Rscript 06_species_niches/scripts/01_build_species_universe.R
Rscript 06_species_niches/scripts/02_check_bien_ranges.R
Rscript 06_species_niches/scripts/03_download_bien_ranges.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --range-scope=us_study_area
Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --range-scope=us_study_area
```

Script `04` requires Google Earth Engine. The complete validation and gap
documentation sequence is in [WORKFLOW.md: Run Order](WORKFLOW.md#run-order).
For small climate refreshes, see
[WORKFLOW.md: Extract TerraClimate From Ranges](WORKFLOW.md#04-extract-terraclimate-from-ranges).

Smoke tests use `--limit=N` and write to ignored `data/smoke/` and `qa/smoke/`
folders.

## Main Outputs

| Product | What it contains | Details |
|---|---|---|
| `species_universe.parquet` | One row per FIA or P2VEG source species code | [Output dictionary](WORKFLOW.md#species_universeparquet) |
| `species_niche_taxon_crosswalk.parquet` | Source-code rows mapped to resolved biological taxa for species-level counting | [Output dictionary](WORKFLOW.md#species_niche_taxon_crosswalkparquet) |
| `bien_range_availability.parquet` | BIEN range availability and reviewed name-query history | [Output dictionary](WORKFLOW.md#bien_range_availabilityparquet) |
| `species_range_polygons.gpkg` | BIEN range polygon and geometry QA fields for each available source taxon | [Output dictionary](WORKFLOW.md#species_range_polygonsgpkg) |
| `species_range_climate*.parquet` | Monthly TerraClimate summaries across each mapped range | [Output dictionary](WORKFLOW.md#species_range_climateparquet) |
| `species_climate_niches*.parquet` | One row per species with eight compact climate indicators | [Output dictionary](WORKFLOW.md#species_climate_nichesparquet) |

For formulas and scientific interpretation, see
[Methods: Compact Niche Indicators](docs/methods_species_niches.md#compact-niche-indicators).

## Where To Look

| Question | Documentation |
|---|---|
| What does each script do? | [Technical Workflow](WORKFLOW.md#script-details) |
| What does one row represent, and what do the columns mean? | [Output Data Dictionary](WORKFLOW.md#output-data-dictionary) |
| How many biological taxa are missing, not just source codes? | [Taxon crosswalk](WORKFLOW.md#species_niche_taxon_crosswalkparquet) and [QA Guide](qa/README.md#minimal-outputs-to-read-first) |
| Why use BIEN ranges and these eight indicators? | [Methods Notes](docs/methods_species_niches.md) |
| What does a validation warning mean? | [QA Guide](qa/README.md) |
| How are missing BIEN names and manual overrides handled? | [Methods: Taxonomic Name Review](docs/methods_species_niches.md#taxonomic-name-review) |
| Which files should I inspect first after a run? | [QA Guide: Minimal Outputs](qa/README.md#minimal-outputs-to-read-first) |
| How are the niches used in community analysis? | [Thermophilization README](../07_thermophilization/README.md) |

## Current Status

Do not copy counts from prose when checking whether the workflow is current.
The generated QA files are the authoritative status:

- `qa/outputs/species_niche_validation_decision.csv`
- `qa/outputs/species_niche_validation_summary.csv`
- `qa/outputs/species_niche_gap_summary.csv`
- `qa/outputs/species_taxon_resolution_summary.csv`
- `qa/outputs/species_taxon_duplicate_source_codes.csv`
- `qa/outputs/study_area_climate_gap_summary.csv`
- `qa/outputs/global_fallback_species_location_summary.csv`
- `qa/outputs/tnrs_candidate_bien_range_summary.csv`
- `qa/outputs/tnrs_candidate_name_pairs_available.csv`

The validator currently distinguishes structural errors from scientific or
coverage warnings. See [QA Guide: How To Read The Result](qa/README.md#how-to-read-the-result)
before final modeling.

## Directory Layout

| Path | Contents |
|---|---|
| `scripts/` | Five production scripts |
| `data/raw/bien_ranges/` | Per-species BIEN range caches |
| `data/processed/` | Production universe, availability, polygons, range climate, and compact niche products |
| `data/smoke/` | Ignored limited-run products |
| `lookups/` | Reviewed manual BIEN name decisions |
| `qa/` | Validation scripts and QA documentation |
| `qa/outputs/` | Production validation and gap reports; see [QA output index](qa/README.md#output-index) |
| `qa/smoke/` | Ignored limited-run QA reports |
| `docs/` | Scientific methods notes |
