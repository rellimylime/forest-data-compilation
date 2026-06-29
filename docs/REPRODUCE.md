# Reproduce the Pipelines

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page gives the exact active production run order for the repository. It is intentionally practical: what to run, in what order, and where to look for more detail. Archived reference workflows are listed separately at the end.

## Before You Run Anything

1. Complete [environment setup](../scripts/SETUP.md).
2. Review the [Pipeline Map](PIPELINE_MAP.md) if you want the big picture first.
3. Use the workstream-specific `README.md` files for quick orientation and the `WORKFLOW.md` files for implementation detail.
4. Use [Data Products](DATA_PRODUCTS.md) if you need to confirm where raw caches, intermediate files, and final outputs should appear.

## Path 1: IDS Foundation

The IDS pipeline is the required foundation for all IDS + climate work.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_download_ids.R](../01_ids/scripts/01_download_ids.R) | Download the 10 regional IDS geodatabases | `01_ids/data/raw/*.gdb.zip`, extracted `.gdb/` directories | [01_ids/README.md](../01_ids/README.md) |
| 2 | [02_inspect_ids.R](../01_ids/scripts/02_inspect_ids.R) | Inspect schema and generate lookup tables | `01_ids/lookups/*.csv` | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 3 | [03_clean_ids.R](../01_ids/scripts/03_clean_ids.R) | Merge and clean all IDS layers | `01_ids/data/processed/ids_layers_cleaned.gpkg` | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 4 | [04_assign_surveyed_areas.R](../01_ids/scripts/04_assign_surveyed_areas.R) | Match damage areas to surveyed areas | `processed/ids/damage_area_to_surveyed_area.parquet` | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 5 | [05_compute_area_metrics.R](../01_ids/scripts/05_compute_area_metrics.R) | Compute damage-area and survey-area metrics | `processed/ids/damage_area_area_metrics.parquet` | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |

Optional QC:

- [validate_ids.R](../01_ids/scripts/qc/validate_ids.R)
- [explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R)
- [IDS QC README](../01_ids/scripts/qc/README.md)

## Path 2: Choose a Climate Dataset

After `01_ids/` is complete, you can run one or more climate workstreams.

### TerraClimate

Use TerraClimate if you want global coverage and the broadest variable set, and you have Google Earth Engine configured.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R) | Map IDS features to TerraClimate pixels | `02_terraclimate/data/processed/pixel_maps/*.parquet` | [02_terraclimate/README.md](../02_terraclimate/README.md) |
| 2 | [02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R) | Extract monthly TerraClimate values via GEE | `02_terraclimate/data/processed/pixel_values/terraclimate_{year}.parquet` | [02_terraclimate/WORKFLOW.md](../02_terraclimate/WORKFLOW.md) |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | `processed/climate/terraclimate/damage_areas_summaries/*.parquet` | [ARCHITECTURE.md](ARCHITECTURE.md) |

Optional exploratory step:

- [00_explore_terraclimate.R](../02_terraclimate/scripts/explore/00_explore_terraclimate.R)

### PRISM

Use PRISM if you want higher-resolution CONUS climate without using GEE.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R) | Map CONUS IDS features to PRISM pixels | `03_prism/data/processed/pixel_maps/*.parquet` | [03_prism/README.md](../03_prism/README.md) |
| 2 | [02_extract_prism.R](../03_prism/scripts/02_extract_prism.R) | Download, extract, and discard PRISM monthly grids | `03_prism/data/processed/pixel_values/prism_{year}.parquet` | [03_prism/WORKFLOW.md](../03_prism/WORKFLOW.md) |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | `processed/climate/prism/damage_areas_summaries/*.parquet` | [ARCHITECTURE.md](ARCHITECTURE.md) |

### WorldClim

Use WorldClim if you want global coverage without GEE and you are comfortable keeping local GeoTIFFs.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_download_worldclim.R](../04_worldclim/scripts/01_download_worldclim.R) | Download decade-based GeoTIFF archives | `04_worldclim/data/raw/{prec,tmax,tmin}/` | [04_worldclim/README.md](../04_worldclim/README.md) |
| 2 | [02_build_pixel_maps.R](../04_worldclim/scripts/02_build_pixel_maps.R) | Map IDS features to WorldClim pixels | `04_worldclim/data/processed/pixel_maps/*.parquet` | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| 3 | [03_extract_worldclim.R](../04_worldclim/scripts/03_extract_worldclim.R) | Extract monthly values from local GeoTIFFs | `04_worldclim/data/processed/pixel_values/worldclim_{year}.parquet` | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| 4 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | `processed/climate/worldclim/damage_areas_summaries/*.parquet` | [ARCHITECTURE.md](ARCHITECTURE.md) |

## Path 3: FIA Workstream

The FIA workstream is independent of the IDS + climate workstream, except for the optional site-climate extraction which also uses TerraClimate and GEE.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_download_fia.R](../05_fia/scripts/01_download_fia.R) | Download FIA CSV tables by state and the REF tables | `05_fia/data/raw/{ST}/`, `05_fia/data/raw/REF/` | [05_fia/README.md](../05_fia/README.md) |
| 2 | [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R) | Inspect schema and build lookup parquets | `05_fia/lookups/*.parquet` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 3 | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | Extract tree, condition, damage-agent, and harvest-flag tables | `05_fia/data/processed/{trees,cond,damage_agents,harvest_flags}/state={ST}/` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 4 | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | Extract seedling and mortality summaries by state | `05_fia/data/processed/{seedlings,mortality}/state={ST}/` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 5 | [05_build_fia_summaries.R](../05_fia/scripts/05_build_fia_summaries.R) | Build national plot-level summary parquets | `05_fia/data/processed/summaries/*.parquet` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| Optional 1 | [01_build_site_list.R](../05_fia/scripts/site_climate/01_build_site_list.R) | Build the FIA site list for optional climate extraction | `05_fia/data/processed/site_climate/all_site_locations.csv` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md#optional-site-climate-extension) |
| Optional 2 | [02_extract_terraclimate.R](../05_fia/scripts/site_climate/02_extract_terraclimate.R) | Extract TerraClimate for FIA site locations | `05_fia/data/processed/site_climate/` | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md#optional-site-climate-extension) |

Notes:

- The site-climate extension is optional and requires Google Earth Engine.
- The main FIA summary outputs, plus `all_site_locations.csv`, `site_pixel_map.parquet`, and `site_climate.parquet`, are reviewable in git.

## Path 4: Species Climate Niches

Run this path after the FIA species-composition products exist.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_build_species_universe.R](../06_species_niches/scripts/01_build_species_universe.R) | Combine FIA and P2VEG source taxa | `species_universe.parquet` | [Workflow](../06_species_niches/WORKFLOW.md#01-build-species-universe) |
| 2 | [02_check_bien_ranges.R](../06_species_niches/scripts/02_check_bien_ranges.R) | Check BIEN availability and apply reviewed name overrides | `bien_range_availability.parquet` | [Workflow](../06_species_niches/WORKFLOW.md#02-check-bien-ranges) |
| 3 | [03_download_bien_ranges.R](../06_species_niches/scripts/03_download_bien_ranges.R) | Cache and consolidate BIEN polygons | `species_range_polygons.gpkg` | [Workflow](../06_species_niches/WORKFLOW.md#03-download-bien-ranges) |
| QA gate | [01_validate_species_niche_workflow.R](../06_species_niches/qa/01_validate_species_niche_workflow.R) | Validate scripts 01-03 before the long extraction | Validation decision and checks | [QA Guide](../06_species_niches/qa/README.md) |
| 4 | [04_extract_terraclimate_from_ranges.R](../06_species_niches/scripts/04_extract_terraclimate_from_ranges.R) | Extract 1981-2010 range climatologies through GEE | `species_range_climate_us_study_area.parquet` | [Workflow](../06_species_niches/WORKFLOW.md#04-extract-terraclimate-from-ranges) |
| 5 | [05_build_species_climate_niches.R](../06_species_niches/scripts/05_build_species_climate_niches.R) | Build eight compact species indicators | `species_climate_niches_us_study_area.parquet` | [Workflow](../06_species_niches/WORKFLOW.md#05-build-species-climate-niches) |

Script `04` requires Google Earth Engine. Run the validation and gap scripts
listed in [the complete run order](../06_species_niches/WORKFLOW.md#run-order)
before final thermophilization modeling.

## Archived Reference: ERA5

`archive/05_era5/` is kept as a documented reference workflow, not as part of the active production checklist above.

| Step | Script | What it does | Main outputs | Details |
|---|---|---|---|---|
| 1 | [01_download_era5.R](../archive/05_era5/scripts/01_download_era5.R) | Download monthly ERA5 NetCDF files through the CDS API | `archive/05_era5/data/raw/<variable>/{variable}_{year}.nc` | [archive/05_era5/README.md](../archive/05_era5/README.md) |
| 2 | [02_build_pixel_maps.R](../archive/05_era5/scripts/02_build_pixel_maps.R) | Map IDS features to ERA5 pixels | `archive/05_era5/data/processed/pixel_maps/*.parquet` | [archive/05_era5/WORKFLOW.md](../archive/05_era5/WORKFLOW.md) |
| 3 | [03_extract_era5.R](../archive/05_era5/scripts/03_extract_era5.R) | Extract monthly ERA5 pixel values from local NetCDFs | `archive/05_era5/data/processed/pixel_values/era5_{year}.parquet` | [archive/05_era5/WORKFLOW.md](../archive/05_era5/WORKFLOW.md) |

Notes:

- No current repo-level `processed/climate/era5/` summary builder is maintained.
- Treat ERA5 as archived reference material unless you are reviving that workflow on purpose.

## Optional Follow-Up

| Category | Location | Purpose |
|---|---|---|
| Optional QC | [docs/TESTING.md](TESTING.md) | QC coverage and gaps across workstreams |
| Demo | [scripts/demos/](../scripts/demos/) | Example analyses using completed outputs |
| Dashboard | [docs/dashboard/](dashboard/) | Browse outputs after the pipelines are run |

## See also

- [Docs Hub](README.md)
- [Pipeline Map](PIPELINE_MAP.md)
- [Data Products](DATA_PRODUCTS.md)
- [Architecture](ARCHITECTURE.md)
