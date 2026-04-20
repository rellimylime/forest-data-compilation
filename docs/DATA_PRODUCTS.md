# Data Products

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This page summarizes the main outputs in the repository, where they live, whether they are tracked in git, and which scripts produce them.

## How to Read This Page

- `git-tracked`: present in the repository for review without rerunning the full pipeline.
- `gitignored/local`: produced locally by running scripts in this repo.
- `Details`: the best follow-up page for schema, interpretation, or technical behavior.

## IDS Outputs

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| Cleaned IDS layers | `01_ids/data/processed/ids_layers_cleaned.gpkg` | Local | [03_clean_ids.R](../01_ids/scripts/03_clean_ids.R) | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| Damage area to surveyed area matches | `processed/ids/damage_area_to_surveyed_area.parquet` | Local | [04_assign_surveyed_areas.R](../01_ids/scripts/04_assign_surveyed_areas.R) | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| Damage area metrics | `processed/ids/damage_area_area_metrics.parquet` | Local | [05_compute_area_metrics.R](../01_ids/scripts/05_compute_area_metrics.R) | [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md) |
| IDS lookup tables | `01_ids/lookups/` | Git-tracked | [02_inspect_ids.R](../01_ids/scripts/02_inspect_ids.R) | [01_ids/README.md](../01_ids/README.md) |

## Climate Outputs

### Shared pattern

All climate workstreams produce:

1. `pixel_maps/` linking IDS features to raster pixels.
2. `pixel_values/` with yearly climate values by pixel.
3. `processed/climate/<dataset>/damage_areas_summaries/` with final observation-level summary parquets.

The shared summary producer is [scripts/build_climate_summaries.R](../scripts/build_climate_summaries.R).

### TerraClimate

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| Pixel maps | `02_terraclimate/data/processed/pixel_maps/` | Local | [01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R) | [02_terraclimate/WORKFLOW.md](../02_terraclimate/WORKFLOW.md) |
| Yearly pixel values | `02_terraclimate/data/processed/pixel_values/terraclimate_{year}.parquet` | Local | [02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R) | [02_terraclimate/WORKFLOW.md](../02_terraclimate/WORKFLOW.md) |
| Observation summaries | `processed/climate/terraclimate/damage_areas_summaries/` | Local | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Pixel centroid lookup | `02_terraclimate/lookups/damage_areas_pixel_centroids.parquet` | Git-tracked | Derived lookup | [02_terraclimate/README.md](../02_terraclimate/README.md) |

### PRISM

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| Pixel maps | `03_prism/data/processed/pixel_maps/` | Local | [01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R) | [03_prism/WORKFLOW.md](../03_prism/WORKFLOW.md) |
| Yearly pixel values | `03_prism/data/processed/pixel_values/prism_{year}.parquet` | Local | [02_extract_prism.R](../03_prism/scripts/02_extract_prism.R) | [03_prism/WORKFLOW.md](../03_prism/WORKFLOW.md) |
| Observation summaries | `processed/climate/prism/damage_areas_summaries/` | Local | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | [ARCHITECTURE.md](ARCHITECTURE.md) |

### WorldClim

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| Raw GeoTIFF archive cache | `04_worldclim/data/raw/` | Local | [01_download_worldclim.R](../04_worldclim/scripts/01_download_worldclim.R) | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| Pixel maps | `04_worldclim/data/processed/pixel_maps/` | Local | [02_build_pixel_maps.R](../04_worldclim/scripts/02_build_pixel_maps.R) | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| Yearly pixel values | `04_worldclim/data/processed/pixel_values/worldclim_{year}.parquet` | Local | [03_extract_worldclim.R](../04_worldclim/scripts/03_extract_worldclim.R) | [04_worldclim/WORKFLOW.md](../04_worldclim/WORKFLOW.md) |
| Observation summaries | `processed/climate/worldclim/damage_areas_summaries/` | Local | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | [ARCHITECTURE.md](ARCHITECTURE.md) |

## FIA Outputs

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| FIA lookup parquets | `05_fia/lookups/` | Git-tracked | [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R) | [05_fia/README.md](../05_fia/README.md) |
| Per-state trees | `05_fia/data/processed/trees/` | Local | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| Per-state conditions | `05_fia/data/processed/cond/` | Local | [03_extract_trees.R](../05_fia/scripts/03_extract_trees.R) | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| Per-state seedlings | `05_fia/data/processed/seedlings/` | Local | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| Per-state mortality | `05_fia/data/processed/mortality/` | Local | [04_extract_seedlings_mortality.R](../05_fia/scripts/04_extract_seedlings_mortality.R) | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |
| National plot summaries | `05_fia/data/processed/summaries/` | Git-tracked | [05_build_fia_summaries.R](../05_fia/scripts/05_build_fia_summaries.R) | [05_fia/README.md](../05_fia/README.md) |
| FIA site climate | `05_fia/data/processed/site_climate/` | Git-tracked | [06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) |

## Docs and Exploration Outputs

| Output family | Location | Tracking | Produced by | Details |
|---|---|---|---|---|
| Demo figures and CSVs | `output/` | Mixed | [scripts/demos/](../scripts/demos/) | [Repo Home](../README.md) |
| Dashboard app | `docs/dashboard/` | Git-tracked | Dashboard code | [docs/dashboard/](dashboard/) |
| Pipeline HTML companion | `docs/pipeline_diagram.html` | Git-tracked | Static HTML summary | [Pipeline Map](PIPELINE_MAP.md) |

## See also

- [Reproduce](REPRODUCE.md)
- [Pipeline Map](PIPELINE_MAP.md)
- [FIA README](../05_fia/README.md)
- [Architecture](ARCHITECTURE.md)
