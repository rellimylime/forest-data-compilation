# Data Products

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page summarizes the main data products in the repository: what each product represents, where it lives, whether it is tracked in git, and which scripts create or depend on it. Module README and WORKFLOW pages linked from the tables provide the fuller row-grain and column descriptions.

> **Looking for row grains, keys, and what is actually built?** See the [Master Product Inventory](MASTER_PRODUCT_INVENTORY.md). It is regenerated from the data, so its row counts, sizes, and presence flags are measured rather than written down by hand. This page remains the guide to storage conventions, git tracking status, and which script produces what.

## Status Labels

- `Git-tracked`: reviewable in the repository as-is.
- `Local/scripted`: produced by running scripts in this repo; typically gitignored.
- `Scripted`: produced by running scripts in this repo; may be local-only if too large to commit.
- `Expected local input`: path the scripts expect to exist locally, even if the files are not tracked.
- `Server mirror`: present in the provided server tree, but not written by the current production scripts.
- `Orphaned`: present on disk and consumed by a workflow, but no script in this repository creates it. These cannot be rebuilt from a clean checkout.

## Storage Conventions

| Location pattern | What it holds |
|---|---|
| `NN_name/data/raw/` | Downloads, extracted source files, or local cache files specific to a workstream |
| `NN_name/data/processed/` | Workstream-specific intermediates such as pixel maps, yearly extracts, and partitioned parquets |
| `processed/` | Cross-workstream derived products built from upstream workstream outputs |
| `output/` | Demo figures and CSV summaries |
| `logs/` | Demo and exploratory run logs |

## IDS Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Regional raw downloads | `01_ids/data/raw/*_AllYears.gdb.zip` | Local/scripted | [01_download_ids.R](../01_ids/scripts/01_download_ids.R) | Ten regional archives covering CONUS, Alaska, and Hawaii |
| Extracted geodatabases | `01_ids/data/raw/*_AllYears.gdb/` | Local/scripted | [01_download_ids.R](../01_ids/scripts/01_download_ids.R) | Expanded `.gdb` directories used by downstream IDS scripts |
| IDS source documentation download | `01_ids/data/raw/IDS2_FlatFiles_Readme.pdf` | Local/scripted | Downloaded source document | Local source copy from the upstream IDS distribution |
| IDS source documentation repo copy | `01_ids/docs/IDS2_FlatFiles_Readme.pdf` | Git-tracked | Repo reference artifact | Reviewable copy kept with the IDS documentation |
| IDS code lookups | `01_ids/lookups/*.csv` | Git-tracked | [02_inspect_ids.R](../01_ids/scripts/02_inspect_ids.R) | Includes region, damage type, host, severity, and percent-affected lookups |
| Cleaned IDS layers | `01_ids/data/processed/ids_layers_cleaned.gpkg` | Local/scripted | [03_clean_ids.R](../01_ids/scripts/03_clean_ids.R) | Canonical scripted output with `damage_areas`, `damage_points`, and `surveyed_areas` layers |
| Exploration CSVs | `01_ids/data/processed/ids_exploration_raw/*.csv` | Local/scripted | [qa/scripts/explore_ids_coverage.R](../01_ids/qa/scripts/explore_ids_coverage.R) | Includes schema and coverage summaries such as `ids_columns_by_era.csv` |
| Additional server export | `01_ids/data/processed/ids_damage_areas_cleaned.gpkg` | Server mirror | Shared server tree | Convenience single-layer export present in the provided server snapshot; not created by the current production scripts |
| Damage-to-survey assignments | `processed/ids/damage_area_to_surveyed_area.parquet` | Local/scripted | [04_assign_surveyed_areas.R](../01_ids/scripts/04_assign_surveyed_areas.R) | Cross-workstream derived output |
| Damage-area metrics | `processed/ids/damage_area_area_metrics.parquet` | Local/scripted | [05_compute_area_metrics.R](../01_ids/scripts/05_compute_area_metrics.R) | Cross-workstream derived output used by climate summaries and demos |

## TerraClimate Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Reference raster | `02_terraclimate/data/raw/terraclimate_reference.tif` | Expected local input | [01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R) | Raster grid used to assign IDS observations to TerraClimate pixels |
| Raw NetCDF cache | `02_terraclimate/data/raw/TerraClimate_{var}_{year}.nc` | Server mirror | Shared server tree | Optional server-side mirror of TerraClimate variable-year files; the current repo scripts extract through GEE and do not write these `.nc` files |
| Pixel maps | `02_terraclimate/data/processed/pixel_maps/{damage_areas,damage_points,surveyed_areas}_pixel_map.parquet` | Local/scripted | [01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R) | One parquet per IDS layer |
| Unique-pixel cache | `02_terraclimate/data/processed/pixel_maps/_all_layers_unique_pixels.parquet` | Local/scripted | [02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R) | Deduplicated pixel list across all IDS layers |
| Yearly pixel values | `02_terraclimate/data/processed/pixel_values/terraclimate_{year}.parquet` | Local/scripted | [02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R) | One parquet per year, 1997-2024 |
| Pixel centroid lookup | `02_terraclimate/lookups/damage_areas_pixel_centroids.parquet` | Git-tracked | Repo lookup artifact | Used by the dashboard and review workflows |
| Final damage-area summaries | `processed/climate/terraclimate/damage_areas_summaries/{aet,def,pdsi,pet,pr,ro,soil,srad,swe,tmmn,tmmx,vap,vpd,vs}.parquet` | Local/scripted | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Standardized observation-level summary outputs |

## PRISM Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Reference raster | `03_prism/data/raw/prism_reference.tif` | Expected local input | [01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R) | Raster grid used to assign CONUS IDS observations to PRISM pixels |
| Pixel maps | `03_prism/data/processed/pixel_maps/{damage_areas,damage_points,surveyed_areas}_pixel_map.parquet` | Local/scripted | [01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R) | One parquet per IDS layer |
| Yearly pixel values | `03_prism/data/processed/pixel_values/prism_{year}.parquet` | Local/scripted | [02_extract_prism.R](../03_prism/scripts/02_extract_prism.R) | One parquet per year, 1997-2024 |
| Final damage-area summaries | `processed/climate/prism/damage_areas_summaries/{ppt,tdmean,tmax,tmean,tmin,vpdmax,vpdmin}.parquet` | Local/scripted | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Monthly downloads are temporary; the kept outputs are the yearly parquets and final summaries |

## WorldClim Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Raw GeoTIFF cache | `04_worldclim/data/raw/{prec,tmax,tmin}/wc2.1_cruts4.09_2.5m_<var>_<YYYY>-<MM>.tif` | Local/scripted | [01_download_worldclim.R](../04_worldclim/scripts/01_download_worldclim.R) | Local archive of monthly GeoTIFFs grouped by variable |
| Pixel maps | `04_worldclim/data/processed/pixel_maps/{damage_areas,damage_points,surveyed_areas}_pixel_map.parquet` | Local/scripted | [02_build_pixel_maps.R](../04_worldclim/scripts/02_build_pixel_maps.R) | One parquet per IDS layer |
| Yearly pixel values | `04_worldclim/data/processed/pixel_values/worldclim_{year}.parquet` | Local/scripted | [03_extract_worldclim.R](../04_worldclim/scripts/03_extract_worldclim.R) | One parquet per year, 1997-2024 |
| Final damage-area summaries | `processed/climate/worldclim/damage_areas_summaries/{prec,tmax,tmin}.parquet` | Local/scripted | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Standardized observation-level summary outputs |

## FIA Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Raw state CSV bundles | `05_fia/data/raw/{ST}/{ST}_{COND,PLOT,SEEDLING,TREE,TREE_GRM_COMPONENT}.csv` | Local/scripted | [01_download_fia.R](../05_fia/scripts/core/01_download_fia.R) | One directory per state |
| Raw reference tables | `05_fia/data/raw/REF/{REF_SPECIES,REF_FOREST_TYPE}.csv` | Local/scripted | [01_download_fia.R](../05_fia/scripts/core/01_download_fia.R) | National lookup tables downloaded once |
| Lookup parquets | `05_fia/lookups/{ref_species,ref_forest_type}.parquet` | Git-tracked | [02_inspect_fia.R](../05_fia/scripts/core/02_inspect_fia.R) | Reviewable parquet copies of the REF tables |
| Tree partitions | `05_fia/data/processed/trees/state={ST}/trees_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/core/03_extract_trees.R) | One parquet per state |
| Condition partitions | `05_fia/data/processed/cond/state={ST}/cond_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/core/03_extract_trees.R) | One parquet per state |
| Damage-agent partitions | `05_fia/data/processed/damage_agents/state={ST}/damage_agents_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/core/03_extract_trees.R) | One parquet per state |
| Harvest-flag partitions | `05_fia/data/processed/harvest_flags/state={ST}/harvest_flags_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/core/03_extract_trees.R) | One parquet per state |
| Seedling partitions | `05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet` | Local/scripted | [04_extract_seedlings_mortality.R](../05_fia/scripts/core/04_extract_seedlings_mortality.R) | One parquet per state |
| Mortality partitions | `05_fia/data/processed/mortality/state={ST}/mortality_{ST}.parquet` | Local/scripted | [04_extract_seedlings_mortality.R](../05_fia/scripts/core/04_extract_seedlings_mortality.R) | One parquet per state |
| National metric summaries | `05_fia/data/processed/summaries/{plot_tree_metrics,plot_seedling_metrics,plot_mortality_metrics}.parquet` | Scripted | [05_build_fia_summaries.R](../05_fia/scripts/core/05_build_fia_summaries.R) | Plot-level structure, regeneration, diversity, and mortality metrics |
| Condition metadata | `05_fia/data/processed/summaries/{plot_cond_fortypcd,plot_condition_metadata}.parquet` | Scripted | [05_build_fia_summaries.R](../05_fia/scripts/core/05_build_fia_summaries.R) | Compact condition table plus the full condition-level join backbone |
| Forested-condition foundation | `05_fia/data/processed/summaries/forested_condition_foundation.parquet` | Local/scripted | [02_build_forested_condition_foundation.R](../05_fia/scripts/foundations/02_build_forested_condition_foundation.R) | One row per FIA condition; forest flag, forest-normalized condition weight, disturbance slots, and condition-proportion QA |
| Life-stage composition | `05_fia/data/processed/summaries/{plot_tree_species,plot_sapling_species,plot_seedling_species}.parquet` | Local/scripted | [05_build_fia_summaries.R](../05_fia/scripts/core/05_build_fia_summaries.R) | Condition x subplot x species communities; large files may remain local |
| Disturbance and treatment products | `05_fia/data/processed/summaries/{fia_condition_disturbance_flags,plot_disturbance_history,plot_treatment_history,plot_damage_agents,plot_exclusion_flags}.parquet` | Scripted | [05_build_fia_summaries.R](../05_fia/scripts/core/05_build_fia_summaries.R) | Analysis classifications, long-form histories, damage agents, and whole-plot sensitivity flags |
| Understory structure partitions | `05_fia/data/processed/understory_structure/state={ST}/` | Local/scripted | [01_extract_understory.R](../05_fia/scripts/understory/01_extract_understory.R) | Growth habit and canopy layer cover per subplot condition, from FIA's Phase 2 vegetation protocol. See [Understory products](#understory-products) |
| Understory species cover partitions | `05_fia/data/processed/understory_veg/state={ST}/` | Local/scripted | [01_extract_understory.R](../05_fia/scripts/understory/01_extract_understory.R) | Per-species cover from the Phase 2 vegetation protocol; consumed by the species-niche workflow. See [Understory products](#understory-products) |
| Site list input | `05_fia/data/processed/site_climate/all_site_locations.csv` | Git-tracked | [01_build_site_list.R](../05_fia/scripts/site_climate/01_build_site_list.R) | Input template for the optional TerraClimate extraction at FIA sites |
| Annual GEE checkpoints | `05_fia/data/processed/site_climate/_gee_annual/sites_{year}.parquet` | Local/scripted | [02_extract_terraclimate.R](../05_fia/scripts/site_climate/02_extract_terraclimate.R) | Checkpoint files kept so interrupted runs can resume |
| Site climate outputs | `05_fia/data/processed/site_climate/{site_pixel_map.parquet,site_climate.parquet}` | Not built | [02_extract_terraclimate.R](../05_fia/scripts/site_climate/02_extract_terraclimate.R) | The FIA-wide extraction over all 408,040 stable plots. No output exists at this path. The built site-climate product is the separate standalone extraction below |
| Point climate extraction | `site_climate/data/recheck/{site_pixel_map.parquet,site_climate.parquet}` | Local/scripted | [extract_terraclimate_points.R](../site_climate/scripts/extract_terraclimate_points.R) | The site-climate product that actually exists: 33,497,856 rows of monthly TerraClimate over 6,956 points from [site_climate/input/all_site_locations.csv](../site_climate/input/all_site_locations.csv) — 4,886 ITRDB tree-ring sites and 2,070 FIA plots. See [Point climate extraction](#point-climate-extraction) |
| Site climate QA | `05_fia/qa/outputs/site_climate_*.csv` | Scripted | [03_validate_site_climate.R](../05_fia/scripts/site_climate/03_validate_site_climate.R) | Read-only validation summaries for the FIA site-climate extraction |

The condition-level tree, sapling, seedling, community, and damage products are general-purpose and include all FIA condition classes. A forest-specific workflow must join condition metadata on `PLT_CN × INVYR × CONDID` and filter `COND_STATUS_CD == 1` before its reviewed aggregation. `CONDID` is not a longitudinal condition identifier.

### Point climate extraction

There are two site-climate workflows and only one of them has an output.

| | `05_fia/scripts/site_climate/` | `site_climate/` |
|---|---|---|
| Input | 408,040 FIA stable plots | 6,956 points: 4,886 ITRDB + 2,070 FIA |
| Keyed by | `stable_plot_id` | `PLT_CN` (FIA rows) or ITRDB site code |
| Output | none in this data root | 33,497,856 rows under `data/recheck/` |

The two inputs are both named `all_site_locations.csv` and share no identifiers. The one under `site_climate/input/` is the one the built product came from.

**Joining the built product back to FIA.** Use `PLT_CN`, at the plot-visit level, after filtering `source == "FIA"`. Expect only 198 of the 2,070 FIA sites to appear in the repository's processed FIA products: the other 1,872 are periodic inventory visits from 1984–1993, and the FIA pipeline starts at inventory year 2000 (`config.yaml → processed.fia.invyr_min`). FIA coverage is five Interior West states — MT 910, UT 669, ID 224, CO 156, WY 111 — and the periodic inventories are single-year statewide snapshots that predate the annual panel design, so comparing them with modern plots needs review first.

The `recheck/` directory name is provisional. Promote this product to a stable path before any handoff.

### Understory products

`05_fia/data/processed/understory_structure/` and `understory_veg/` hold fifty state partitions each — 4,763,853 and 1,672,529 rows. They come from FIA's Phase 2 vegetation protocol, which records ground-layer plants rather than trees.

**Producer recovered 2026-07-23.** These products were previously orphaned — on disk with no script that built them, and their source tables were not even downloaded by the pipeline. [`01_extract_understory.R`](../05_fia/scripts/understory/01_extract_understory.R) now rebuilds both from the raw `P2VEG_SUBP_STRUCTURE`, `P2VEG_SUBPLOT_SPP`, and `SUBP_COND` tables (added to `processed.fia.tables_required` in `config.yaml`), with plant attributes from `REF_PLANT_DICTIONARY`. The script downloads any missing raw inputs itself, so [`01_build_species_universe.R`](../06_species_niches/scripts/01_build_species_universe.R) — which reads `understory_veg` — and the whole species-niche workflow are now reproducible from a clean checkout. The rebuild was verified byte-identical to the pre-existing products (apart from one PLANTS symbol whose dictionary entry gained a trailing space upstream).

Note the two products have different geographic coverage: structure cover is recorded in every state, but per-species cover (`understory_veg`) is collected mainly in the Interior West plus Alaska, Hawaii, and Oregon, so many eastern states have a structure partition but an empty species partition.

| Product | One row is | Rows |
|---|---|---:|
| `understory_structure` | one growth habit and canopy layer on one subplot, in one condition of one plot visit | 4,763,853 |
| `understory_veg` | one recorded plant species on one subplot, in one condition of one plot visit | 1,672,529 |

Two things to know before using them:

- The Phase 2 vegetation protocol runs on a subset of plots, so coverage is far sparser than the tree data. Absence of a row is not absence of a plant.
- Species are identified by PLANTS symbols (`VEG_SPCD`, `plant_symbol`), not FIA `SPCD`. They do not join to `ref_species` or to the species climate niches without a crosswalk.

Until a producer exists, treat these as inputs to preserve, not as products the pipeline can regenerate. They are registered as `not_reviewed` and catalog-only in [the product registry](../forest_explorer/registry/products.yaml).

## Species Niche Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Species universe | `06_species_niches/data/processed/species_universe.parquet` | Local/scripted | [01_build_species_universe.R](../06_species_niches/scripts/01_build_species_universe.R) | One row per FIA or P2VEG source species code; [column guide](../06_species_niches/WORKFLOW.md#species_universeparquet) |
| Taxon crosswalk | `06_species_niches/data/processed/species_niche_taxon_crosswalk.parquet` | Local/scripted | [09_build_species_taxon_crosswalk.R](../06_species_niches/qa/scripts/09_build_species_taxon_crosswalk.R) | Maps source codes to resolved biological taxa for species-level coverage counts; [column guide](../06_species_niches/WORKFLOW.md#species_niche_taxon_crosswalkparquet) |
| BIEN availability | `06_species_niches/data/processed/bien_range_availability.parquet` | Local/scripted | [02_check_bien_ranges.R](../06_species_niches/scripts/02_check_bien_ranges.R) | Range availability and reviewed name-query audit; [column guide](../06_species_niches/WORKFLOW.md#bien_range_availabilityparquet) |
| Per-species BIEN cache | `06_species_niches/data/raw/bien_ranges/*.gpkg` | Local/scripted | [03_download_bien_ranges.R](../06_species_niches/scripts/03_download_bien_ranges.R) | Reusable local range downloads |
| Consolidated BIEN ranges | `06_species_niches/data/processed/species_range_polygons.gpkg` | Local/scripted | [03_download_bien_ranges.R](../06_species_niches/scripts/03_download_bien_ranges.R) | One project-keyed range feature per available source taxon; [column guide](../06_species_niches/WORKFLOW.md#species_range_polygonsgpkg) |
| Range climate | `06_species_niches/data/processed/species_range_climate{_us_study_area}.parquet` | Local/scripted | [04_extract_terraclimate_from_ranges.R](../06_species_niches/scripts/04_extract_terraclimate_from_ranges.R) | Monthly TerraClimate summaries over global or study-area-clipped ranges; [column guide](../06_species_niches/WORKFLOW.md#species_range_climateparquet) |
| Compact species niches | `06_species_niches/data/processed/species_climate_niches{_us_study_area}.parquet` | Local/scripted | [05_build_species_climate_niches.R](../06_species_niches/scripts/05_build_species_climate_niches.R) | Eight species-level climate indicators; [column guide](../06_species_niches/WORKFLOW.md#species_climate_nichesparquet) |
| Validation and gap reports | `06_species_niches/qa/outputs/*.csv` | Mixed tracked/local | [Species niche QA](../06_species_niches/qa/README.md) | Structural checks, coverage summaries, missing-data categories, and review priorities |

## Thermophilization Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Condition community climate | `07_thermophilization/data/processed/plot_community_climate_{seedlings,saplings,trees}.parquet` | Local/scripted | [01_build_condition_community_climate.R](../07_thermophilization/scripts/01_build_condition_community_climate.R) | One row per FIA condition and community layer; weighted mean and weighted median species niche indicators. Contains all condition classes, so a forest workflow must filter `COND_STATUS_CD == 1`; [output guide](../07_thermophilization/README.md#plot_community_climate_layerparquet) |
| Forest plot-visit CWM | `07_thermophilization/data/processed/forest_plot_visit_cwm_{seedlings,saplings,trees}.parquet` | Local/scripted | [02_build_forest_plot_visit_cwm.R](../07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R) | The analysis response. One row per plot visit; forested conditions only, weighted by forested-area share, with `forested_plot_proportion` recording how much of the plot was forest; [output guide](../07_thermophilization/README.md#forest_plot_visit_cwm_layerparquet) |
| Plot disturbance severity | `07_thermophilization/data/processed/plot_disturbance_severity.parquet` | Local/scripted | [03_build_plot_disturbance_severity.R](../07_thermophilization/scripts/03_build_plot_disturbance_severity.R) | One row per plot visit; condition-proportion-weighted disturbance extent for fire, crown fire, insects, disease, weather, treatment, and human/harvest flags; [output guide](../07_thermophilization/README.md#plot_disturbance_severityparquet) |
| Consecutive-survey change | `07_thermophilization/data/processed/forest_visit_interval_change_{seedlings,saplings,trees}.parquet` | Local/scripted | [04_build_visit_interval_change.R](../07_thermophilization/scripts/04_build_visit_interval_change.R) | One row per remeasurement interval; previous/current values, deltas, annualized rates, and current-visit disturbance. Every interval uses FIA's official `PREV_PLT_CN` link; [output guide](../07_thermophilization/README.md#forest_visit_interval_change_layerparquet) |
| First-to-last change | `07_thermophilization/data/processed/forest_first_last_change{,_by_stage}.parquet` | Local/scripted | [05_build_first_last_change.R](../07_thermophilization/scripts/05_build_first_last_change.R) | One row per stable plot (or per plot and life stage); change from earliest to latest survey. The primary file restricts to visits where all three life stages have a usable value; [output guide](../07_thermophilization/README.md#forest_first_last_changeparquet) |
| FIA forest disturbance | `08_disturbance_linkage/data/processed/{fia_forest_disturbance_measures,fia_fire_disturbance_slot_evidence}.parquet` | Local/scripted | [02_build_forest_disturbance_measures.R](../08_disturbance_linkage/scripts/fia/02_build_forest_disturbance_measures.R) | Canonical same-slot fire code-year evidence plus forest-normalized FIA disturbance extents |
| FIA damage-agent lookup | `05_fia/lookups/fia_damage_agent_lookup.csv` | Git-tracked/scripted | [01_build_damage_agent_lookup.py](../05_fia/scripts/reference/01_build_damage_agent_lookup.py) | 966 current-v9.4 definitions with groups, thresholds, regions, and reviewed insect flags; not a historical crosswalk |
| FIA tree damage-agent evidence | `08_disturbance_linkage/data/processed/fia_tree_damage_agent_evidence/` | Local/scripted | [03_prepare_damage_agent_evidence.R](../08_disturbance_linkage/scripts/fia/03_prepare_damage_agent_evidence.R) | One tree record x exact code; all condition classes and source slots retained, with manual/definition applicability |
| FIA condition damage ingredients | `08_disturbance_linkage/data/processed/{fia_condition_damage_denominators,fia_condition_damage_agent_candidates,fia_insect_severity_candidates}/` | Local/scripted | [03_prepare_damage_agent_evidence.R](../08_disturbance_linkage/scripts/fia/03_prepare_damage_agent_evidence.R) | Condition-level record, raw-TPA, and calculated basal-area ingredients; [column formulas and provenance](../08_disturbance_linkage/FIA_DISTURBANCE_DATA_DICTIONARY.md) |
| Thermophilization QA summaries | `07_thermophilization/qa/outputs/*.csv` | Local/scripted | [Thermophilization QA scripts](../07_thermophilization/qa/) and per-product builders | Compact coverage, attrition, missing-species, disturbance-severity, validation, and repeated-survey summaries; [plain-language QA guide](../07_thermophilization/README.md#qa-csvs) |

## Archived ERA5 Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| Variable metadata | `archive/05_era5/data/metadata/{era5_variable_metadata.csv,era5_variable_metadata_review.csv}` | Local/scripted | [00_export_era5_variable_metadata.R](../archive/05_era5/scripts/00_export_era5_variable_metadata.R) | Metadata support files for the archived workflow |
| Raw monthly NetCDF cache | `archive/05_era5/data/raw/<variable>/{variable}_{year}.nc` | Local/scripted | [01_download_era5.R](../archive/05_era5/scripts/01_download_era5.R) | One directory per ERA5 variable plus `_batch_tmp/` scratch storage |
| Pixel maps | `archive/05_era5/data/processed/pixel_maps/{damage_areas,damage_points,surveyed_areas}_pixel_map.parquet` | Local/scripted | [02_build_pixel_maps.R](../archive/05_era5/scripts/02_build_pixel_maps.R) | Archived reference implementation |
| Yearly pixel values | `archive/05_era5/data/processed/pixel_values/era5_{year}.parquet` | Local/scripted | [03_extract_era5.R](../archive/05_era5/scripts/03_extract_era5.R) | The archived workflow stops here; there is no maintained `processed/climate/era5/` summary tree in the active repo |

## Demo, Review, and Presentation Outputs

| Output family | Location | Status | Produced by / used by | Notes |
|---|---|---|---|---|
| IDS + climate demos | `output/demo_01_ids_climate_<dataset>/` | Local/scripted | [scripts/demos/demo_01_ids_climate.R](../scripts/demos/demo_01_ids_climate.R) | Current script-generated output path |
| FIA forest demo | `output/demo_02_fia_forest/` | Local/scripted | [scripts/demos/demo_02_fia_forest.R](../scripts/demos/demo_02_fia_forest.R) | Figures and CSV summaries |
| FIA site-climate demo | `output/demo_03_site_climate/` | Local/scripted | [scripts/demos/demo_03_site_climate.R](../scripts/demos/demo_03_site_climate.R) | Figures and CSV summaries |
| Cross-dataset comparison demo | `output/demo_mpb_comparison/` | Local/scripted | [scripts/demos/demo_04_compare_climate_datasets.R](../scripts/demos/demo_04_compare_climate_datasets.R) | Comparison figures |
| Historical server demo outputs | `output/demo_mpb_{terraclimate,prism,worldclim}/` | Server mirror | Shared server tree | Older demo naming present in the provided server snapshot |
| Logs | `logs/*.log` | Git-tracked | Saved run logs | Includes `demo_prism.log`, `demo_terraclimate.log`, and `demo_worldclim.log` |
| Dashboard app | `docs/dashboard/` | Git-tracked | Streamlit code | Review UI for data products and architecture |

## Condition-Level Analysis Products

| Product | Location | Status | Producer | Notes |
|---|---|---|---|---|
| Official condition intervals | `09_analysis/data/processed/stable_condition_intervals.parquet` | Local/scripted | `09_analysis/scripts/01_build_condition_histories_and_cwm.R` | Official PREV links; same numeric CONDID; forest and at least 30% of plot at both endpoints |
| Interval agent mortality | `09_analysis/data/processed/interval_agent_mortality.parquet` | Local/scripted | `09_analysis/scripts/02_build_interval_mortality.R` | Verified deaths use T1 abundance and sampling-element condition proportions; P2A excluded |
| Stable-condition CWM change | `09_analysis/data/processed/stable_condition_cwm_change.parquet` | Local/scripted | `09_analysis/scripts/01_build_condition_histories_and_cwm.R` | Condition × life stage; T1/T2/delta temperature, precipitation, CWD; no plot aggregation |
| Cumulative history mortality | `09_analysis/data/processed/history_cumulative_mortality.parquet` | Local/scripted | `09_analysis/scripts/04_build_cumulative_mortality.R` | Complete first-to-last history; baseline plus intermediate lineage entries; not annualized |
| Life-stage model input | `09_analysis/data/processed/lifestage_model_data.parquet` | Local/scripted | `09_analysis/scripts/06_add_cumulative_site_cwd.sql` | Compact input with cumulative TerraClimate site CWD |
| Pooled-community model input | `09_analysis/data/processed/pooled_model_data.parquet` | Local/scripted | `09_analysis/scripts/07_build_pooled_community_cwm.sql` | All live life stages combined with individual-abundance expansions |
| Preliminary model run | `09_analysis/results/model_runs/20260822_cumulative_mortality_site_cwd_all_groups_v01/` | Local/scripted | `09_analysis/scripts/08_fit_preliminary_models_and_report.R` | Twelve models, sjPlot tables, raw plots, ggeffects plots, and one HTML report |

See the current [methods](../09_analysis/docs/METHODS.md) and complete
[product guide](../09_analysis/docs/PRODUCTS.md).

## See also

- [Reproduce](REPRODUCE.md)
- [Pipeline Map](PIPELINE_MAP.md)
- [FIA README](../05_fia/README.md)
- [Architecture](ARCHITECTURE.md)
