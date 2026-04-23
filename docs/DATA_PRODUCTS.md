# Data Products

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page summarizes the main data products in the repository, where they live, whether they are tracked in git, and which scripts create or depend on them. It also documents the minimal server-aligned directory skeleton now kept in git with `.gitkeep` placeholders where the raw or intermediate data are too large to commit.

## Status Labels

- `Git-tracked`: reviewable in the repository as-is.
- `Local/scripted`: produced by running scripts in this repo; typically gitignored.
- `Expected local input`: path the scripts expect to exist locally, even if the files are not tracked.
- `Server mirror`: present in the provided server tree, but not written by the current production scripts.

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
| Exploration CSVs | `01_ids/data/processed/ids_exploration_raw/*.csv` | Local/scripted | [scripts/qc/explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R) | Includes schema and coverage summaries such as `ids_columns_by_era.csv` |
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
| Raw state CSV bundles | `05_fia/data/raw/{ST}/{ST}_{COND,PLOT,SEEDLING,TREE,TREE_GRM_COMPONENT}.csv` | Local/scripted | [01_download_fia.R](../05_fia/scripts/01_download_fia.R) | One directory per state |
| Raw reference tables | `05_fia/data/raw/REF/{REF_SPECIES,REF_FOREST_TYPE}.csv` | Local/scripted | [01_download_fia.R](../05_fia/scripts/01_download_fia.R) | National lookup tables downloaded once |
| Lookup parquets | `05_fia/lookups/{ref_species,ref_forest_type}.parquet` | Git-tracked | [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R) | Reviewable parquet copies of the REF tables |
| Tree partitions | `05_fia/data/processed/trees/state={ST}/trees_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | One parquet per state |
| Condition partitions | `05_fia/data/processed/cond/state={ST}/cond_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | One parquet per state |
| Damage-agent partitions | `05_fia/data/processed/damage_agents/state={ST}/damage_agents_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | One parquet per state |
| Harvest-flag partitions | `05_fia/data/processed/harvest_flags/state={ST}/harvest_flags_{ST}.parquet` | Local/scripted | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | One parquet per state |
| Seedling partitions | `05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet` | Local/scripted | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | One parquet per state |
| Mortality partitions | `05_fia/data/processed/mortality/state={ST}/mortality_{ST}.parquet` | Local/scripted | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | One parquet per state |
| National summaries | `05_fia/data/processed/summaries/{plot_tree_metrics,plot_seedling_metrics,plot_mortality_metrics,plot_disturbance_history,plot_damage_agents,plot_treatment_history,plot_cond_fortypcd,plot_exclusion_flags}.parquet` | Git-tracked | [05_build_fia_summaries.R](../05_fia/scripts/05_build_fia_summaries.R) | Main reviewable FIA outputs |
| Site list input | `05_fia/data/processed/site_climate/all_site_locations.csv` | Git-tracked | Used by [06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | Input template for TerraClimate extraction at FIA sites |
| Annual GEE checkpoints | `05_fia/data/processed/site_climate/_gee_annual/sites_{year}.parquet` | Local/scripted | [06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | Checkpoint files kept so interrupted runs can resume |
| Site climate outputs | `05_fia/data/processed/site_climate/{site_pixel_map.parquet,site_climate.parquet}` | Git-tracked | [06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | Pixel map plus long-format monthly TerraClimate values |

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

## See also

- [Reproduce](REPRODUCE.md)
- [Pipeline Map](PIPELINE_MAP.md)
- [FIA README](../05_fia/README.md)
- [Architecture](ARCHITECTURE.md)
