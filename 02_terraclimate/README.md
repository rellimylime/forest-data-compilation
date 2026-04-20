# TerraClimate - Pixel-Level Climate Extraction

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](../docs/REPRODUCE.md) | [Pipeline Map](../docs/PIPELINE_MAP.md) | [Data Products](../docs/DATA_PRODUCTS.md) | [Technical Workflow](WORKFLOW.md) | [Scripts](scripts/)

## What this workstream does

`02_terraclimate/` maps IDS observations to TerraClimate pixels and extracts monthly climate values for all unique pixels through Google Earth Engine. Final observation-level summaries are built with the shared script [`scripts/build_climate_summaries.R`](../scripts/build_climate_summaries.R).

## When to use it

Use TerraClimate if you want:

- global coverage
- the broadest climate variable set in this repo
- a GEE-based workflow rather than local raster downloads

## Quick facts

| Item | Value |
|---|---|
| Coverage | Global |
| Resolution | About 4 km |
| Temporal coverage | 1958-present, with IDS extraction for 1997-2024 |
| Variables | 14 |
| Requires GEE | Yes |

## Workflow At a Glance

```mermaid
flowchart LR
  A[IDS foundation] --> B[Build pixel maps]
  B --> C[Extract TerraClimate via GEE]
  C --> D[Shared climate summaries]
```

## Production Scripts

| Step | Script | Role |
|---|---|---|
| 1 | [01_build_pixel_maps.R](scripts/01_build_pixel_maps.R) | Map IDS features to TerraClimate pixels |
| 2 | [02_extract_terraclimate.R](scripts/02_extract_terraclimate.R) | Extract monthly TerraClimate values via GEE |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build final observation-level summaries |

Optional exploratory script:

- [00_explore_terraclimate.R](scripts/explore/00_explore_terraclimate.R)

## Quick Start

Prerequisite: complete the IDS foundation steps in [01_ids/README.md](../01_ids/README.md).

```bash
Rscript 02_terraclimate/scripts/01_build_pixel_maps.R
Rscript 02_terraclimate/scripts/02_extract_terraclimate.R
Rscript scripts/build_climate_summaries.R terraclimate
```

## Key Outputs

| Output | Location | Notes |
|---|---|---|
| Pixel maps | `02_terraclimate/data/processed/pixel_maps/` | One parquet per IDS layer |
| Yearly pixel values | `02_terraclimate/data/processed/pixel_values/terraclimate_{year}.parquet` | Wide-format yearly files |
| Observation summaries | `processed/climate/terraclimate/damage_areas_summaries/` | One parquet per variable |
| Pixel centroid lookup | `02_terraclimate/lookups/damage_areas_pixel_centroids.parquet` | Tracked in git for dashboard use |

## Variables

Temperature, precipitation, evapotranspiration, drought, radiation, vapor pressure, wind, soil moisture, snow water equivalent, and runoff. See [WORKFLOW.md](WORKFLOW.md) and `data_dictionary.csv` for the full variable list and units.

## Related Docs

| If you want... | Go to... |
|---|---|
| shared climate architecture | [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) |
| the exact run order across datasets | [docs/REPRODUCE.md](../docs/REPRODUCE.md) |
| per-script technical detail and troubleshooting | [WORKFLOW.md](WORKFLOW.md) |
| output inventory | [docs/DATA_PRODUCTS.md](../docs/DATA_PRODUCTS.md) |

## See also

- [Docs Hub](../docs/README.md)
- [Shared Architecture](../docs/ARCHITECTURE.md)
- [PRISM README](../03_prism/README.md)
- [WorldClim README](../04_worldclim/README.md)
