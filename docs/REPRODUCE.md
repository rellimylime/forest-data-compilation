# Reproduce the Pipelines

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page gives the exact production run order for the repository. It is intentionally practical: what to run, in what order, and where to look for more detail.

## Before You Run Anything

1. Complete [environment setup](../scripts/SETUP.md).
2. Review the [Pipeline Map](PIPELINE_MAP.md) if you want the big picture first.
3. Use the workstream-specific `README.md` files for quick orientation and the `WORKFLOW.md` files for implementation detail.

## Path 1: IDS Foundation

The IDS pipeline is the required foundation for all IDS + climate work.

| Step | Script | What it does | Details |
|---|---|---|---|
| 1 | [01_download_ids.R](../01_ids/scripts/01_download_ids.R) | Download the 10 regional IDS geodatabases | [01_ids/README.md](../01_ids/README.md) |
| 2 | [02_inspect_ids.R](../01_ids/scripts/02_inspect_ids.R) | Inspect schema and generate lookup tables | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 3 | [03_clean_ids.R](../01_ids/scripts/03_clean_ids.R) | Merge and clean all IDS layers | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 4 | [04_assign_surveyed_areas.R](../01_ids/scripts/04_assign_surveyed_areas.R) | Match damage areas to surveyed areas | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| 5 | [05_compute_area_metrics.R](../01_ids/scripts/05_compute_area_metrics.R) | Compute damage-area and survey-area metrics | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |

Optional QC:

- [validate_ids.R](../01_ids/scripts/qc/validate_ids.R)
- [explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R)
- [IDS QC README](../01_ids/scripts/qc/README.md)

## Path 2: Choose a Climate Dataset

After `01_ids/` is complete, you can run one or more climate workstreams.

### TerraClimate

Use TerraClimate if you want global coverage and the broadest variable set, and you have Google Earth Engine configured.

| Step | Script | What it does | Details |
|---|---|---|---|
| 1 | [01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R) | Map IDS features to TerraClimate pixels | [02_terraclimate/README.md](../02_terraclimate/README.md) |
| 2 | [02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R) | Extract monthly TerraClimate values via GEE | [02_terraclimate/WORKFLOW.md](../02_terraclimate/WORKFLOW.md) |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | [ARCHITECTURE.md](ARCHITECTURE.md) |

Optional exploratory step:

- [00_explore_terraclimate.R](../02_terraclimate/scripts/explore/00_explore_terraclimate.R)

### PRISM

Use PRISM if you want higher-resolution CONUS climate without using GEE.

| Step | Script | What it does | Details |
|---|---|---|---|
| 1 | [01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R) | Map CONUS IDS features to PRISM pixels | [03_prism/README.md](../03_prism/README.md) |
| 2 | [02_extract_prism.R](../03_prism/scripts/02_extract_prism.R) | Download, extract, and discard PRISM monthly grids | [03_prism/WORKFLOW.md](../03_prism/WORKFLOW.md) |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | [ARCHITECTURE.md](ARCHITECTURE.md) |

### WorldClim

Use WorldClim if you want global coverage without GEE and you are comfortable keeping local GeoTIFFs.

| Step | Script | What it does | Details |
|---|---|---|---|
| 1 | [01_download_worldclim.R](../04_worldclim/scripts/01_download_worldclim.R) | Download decade-based GeoTIFF archives | [04_worldclim/README.md](../04_worldclim/README.md) |
| 2 | [02_build_pixel_maps.R](../04_worldclim/scripts/02_build_pixel_maps.R) | Map IDS features to WorldClim pixels | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| 3 | [03_extract_worldclim.R](../04_worldclim/scripts/03_extract_worldclim.R) | Extract monthly values from local GeoTIFFs | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| 4 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build observation-level climate summaries | [ARCHITECTURE.md](ARCHITECTURE.md) |

## Path 3: FIA Workstream

The FIA workstream is independent of the IDS + climate workstream, except for the optional site-climate extraction which also uses TerraClimate and GEE.

| Step | Script | What it does | Details |
|---|---|---|---|
| 1 | [01_download_fia.R](../05_fia/scripts/01_download_fia.R) | Download FIA CSV tables by state and the REF tables | [05_fia/README.md](../05_fia/README.md) |
| 2 | [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R) | Inspect schema and build lookup parquets | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 3 | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | Extract tree, condition, damage-agent, and harvest-flag tables | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 4 | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | Extract seedling and mortality summaries by state | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 5 | [05_build_fia_summaries.R](../05_fia/scripts/05_build_fia_summaries.R) | Build national plot-level summary parquets | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| 6 | [06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | Optional TerraClimate extraction for FIA site locations | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |

Notes:

- Step 6 is optional and requires Google Earth Engine.
- The main FIA summary outputs are tracked in git; see [Data Products](DATA_PRODUCTS.md).

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
