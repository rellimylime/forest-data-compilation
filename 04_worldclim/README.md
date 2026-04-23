# WorldClim - Global Monthly Climate Extraction

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](../docs/REPRODUCE.md) | [Pipeline Map](../docs/PIPELINE_MAP.md) | [Data Products](../docs/DATA_PRODUCTS.md) | [Technical Workflow](WORKFLOW.md) | [Scripts](scripts/)

## What this workstream does

`04_worldclim/` downloads WorldClim monthly weather GeoTIFFs, maps IDS observations to WorldClim pixels, and extracts monthly climate values from local files. Final observation-level summaries are built with the shared script [`scripts/build_climate_summaries.R`](../scripts/build_climate_summaries.R).

## When to use it

Use WorldClim if you want:

- global coverage without using Google Earth Engine
- a local-raster workflow
- a lightweight climate product focused on temperature and precipitation

## Quick facts

| Item | Value |
|---|---|
| Coverage | Global land areas |
| Resolution | About 4.5 km |
| Temporal coverage | 1950-2024, with IDS extraction for 1997-2024 |
| Variables | 3 |
| Requires GEE | No |

## Workflow At a Glance

```mermaid
flowchart LR
  A[IDS foundation] --> B[Download WorldClim]
  B --> C[Build pixel maps]
  C --> D[Extract from GeoTIFFs]
  D --> E[Shared climate summaries]
```

## Production Scripts

| Step | Script | Role |
|---|---|---|
| 1 | [01_download_worldclim.R](scripts/01_download_worldclim.R) | Download decade-based GeoTIFF archives and populate the raw cache |
| 2 | [02_build_pixel_maps.R](scripts/02_build_pixel_maps.R) | Map IDS features to WorldClim pixels |
| 3 | [03_extract_worldclim.R](scripts/03_extract_worldclim.R) | Extract monthly values from local GeoTIFFs and write yearly parquet files |
| 4 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build final observation-level summaries |

## Quick Start

Prerequisite: complete the IDS foundation steps in [01_ids/README.md](../01_ids/README.md).

```bash
Rscript 04_worldclim/scripts/01_download_worldclim.R
Rscript 04_worldclim/scripts/02_build_pixel_maps.R
Rscript 04_worldclim/scripts/03_extract_worldclim.R
Rscript scripts/build_climate_summaries.R worldclim
```

## Key Outputs

| Output | Location | Notes |
|---|---|---|
| Raw GeoTIFF cache | `04_worldclim/data/raw/{prec,tmax,tmin}/` | Monthly files named `wc2.1_cruts4.09_2.5m_<var>_<YYYY>-<MM>.tif` |
| Pixel maps | `04_worldclim/data/processed/pixel_maps/` | Includes `damage_areas`, `damage_points`, and `surveyed_areas` pixel maps |
| Yearly pixel values | `04_worldclim/data/processed/pixel_values/worldclim_{year}.parquet` | Wide-format yearly files |
| Observation summaries | `processed/climate/worldclim/damage_areas_summaries/` | One parquet per WorldClim variable |

## Final Summary Files

The shared summary builder writes one parquet per variable under `processed/climate/worldclim/damage_areas_summaries/`:

- `prec.parquet`
- `tmax.parquet`
- `tmin.parquet`

## Directory Layout

| Path | What belongs here |
|---|---|
| `04_worldclim/data/raw/prec/` | Monthly precipitation GeoTIFFs |
| `04_worldclim/data/raw/tmax/` | Monthly maximum-temperature GeoTIFFs |
| `04_worldclim/data/raw/tmin/` | Monthly minimum-temperature GeoTIFFs |
| `04_worldclim/data/processed/pixel_maps/` | IDS feature-to-pixel crosswalks |
| `04_worldclim/data/processed/pixel_values/` | Yearly WorldClim parquet extracts |
| `processed/climate/worldclim/damage_areas_summaries/` | Cross-workstream observation-level summaries used by demos and downstream analysis |

## Variables

WorldClim contributes three monthly variables in physical units: `tmin`, `tmax`, and `prec`.

## Related Docs

| If you want... | Go to... |
|---|---|
| shared climate architecture | [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) |
| per-script technical detail and troubleshooting | [WORKFLOW.md](WORKFLOW.md) |
| reproduction order | [docs/REPRODUCE.md](../docs/REPRODUCE.md) |
| output inventory | [docs/DATA_PRODUCTS.md](../docs/DATA_PRODUCTS.md) |

## See also

- [Docs Hub](../docs/README.md)
- [Shared Architecture](../docs/ARCHITECTURE.md)
- [TerraClimate README](../02_terraclimate/README.md)
- [PRISM README](../03_prism/README.md)
