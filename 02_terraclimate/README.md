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
| 1 | [01_build_pixel_maps.R](scripts/01_build_pixel_maps.R) | Map IDS features to TerraClimate pixels from `terraclimate_reference.tif` |
| 2 | [02_extract_terraclimate.R](scripts/02_extract_terraclimate.R) | Extract monthly TerraClimate values via GEE and write yearly parquet files |
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
| Reference raster | `02_terraclimate/data/raw/terraclimate_reference.tif` | Expected local input for pixel-map construction |
| Optional raw cache | `02_terraclimate/data/raw/TerraClimate_{var}_{year}.nc` | Present in the provided server tree, but not written by the current repo scripts |
| Pixel maps | `02_terraclimate/data/processed/pixel_maps/` | Includes `damage_areas`, `damage_points`, and `surveyed_areas` pixel maps |
| Unique-pixel cache | `02_terraclimate/data/processed/pixel_maps/_all_layers_unique_pixels.parquet` | Deduplicated pixel list across all IDS layers |
| Yearly pixel values | `02_terraclimate/data/processed/pixel_values/terraclimate_{year}.parquet` | Wide-format yearly files |
| Observation summaries | `processed/climate/terraclimate/damage_areas_summaries/` | One parquet per TerraClimate variable |
| Pixel centroid lookup | `02_terraclimate/lookups/damage_areas_pixel_centroids.parquet` | Tracked in git for dashboard and review use |

## Final Summary Files

The shared summary builder writes one parquet per variable under `processed/climate/terraclimate/damage_areas_summaries/`:

- `aet.parquet`
- `def.parquet`
- `pdsi.parquet`
- `pet.parquet`
- `pr.parquet`
- `ro.parquet`
- `soil.parquet`
- `srad.parquet`
- `swe.parquet`
- `tmmn.parquet`
- `tmmx.parquet`
- `vap.parquet`
- `vpd.parquet`
- `vs.parquet`

## Directory Layout

| Path | What belongs here |
|---|---|
| `02_terraclimate/data/raw/` | Reference raster plus any optional server-side raw TerraClimate cache files |
| `02_terraclimate/data/processed/pixel_maps/` | IDS feature-to-pixel crosswalks and the all-layer unique-pixel cache |
| `02_terraclimate/data/processed/pixel_values/` | Yearly TerraClimate parquet extracts |
| `02_terraclimate/lookups/` | Small reviewable lookup artifacts |
| `processed/climate/terraclimate/damage_areas_summaries/` | Cross-workstream observation-level summaries used by demos and downstream analysis |

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
