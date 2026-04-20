# PRISM - 800m Pixel-Level Climate Extraction

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](../docs/REPRODUCE.md) | [Pipeline Map](../docs/PIPELINE_MAP.md) | [Data Products](../docs/DATA_PRODUCTS.md) | [Technical Workflow](WORKFLOW.md) | [Scripts](scripts/)

## What this workstream does

`03_prism/` maps IDS observations to PRISM pixels and extracts monthly climate values by downloading data from the PRISM web service. Final observation-level summaries are built with the shared script [`scripts/build_climate_summaries.R`](../scripts/build_climate_summaries.R).

## When to use it

Use PRISM if you want:

- the highest spatial resolution climate product in this repo
- a workflow that does not require Google Earth Engine
- CONUS-focused climate extraction for IDS analyses

## Quick facts

| Item | Value |
|---|---|
| Coverage | CONUS only |
| Resolution | About 800 m |
| Temporal coverage | 1981-present, with IDS extraction for 1997-2024 |
| Variables | 7 |
| Requires GEE | No |

## Workflow At a Glance

```mermaid
flowchart LR
  A[IDS foundation] --> B[Build pixel maps]
  B --> C[Extract PRISM values]
  C --> D[Shared climate summaries]
```

## Production Scripts

| Step | Script | Role |
|---|---|---|
| 1 | [01_build_pixel_maps.R](scripts/01_build_pixel_maps.R) | Map CONUS IDS features to PRISM pixels |
| 2 | [02_extract_prism.R](scripts/02_extract_prism.R) | Download, extract, and discard monthly PRISM rasters |
| 3 | [build_climate_summaries.R](../scripts/build_climate_summaries.R) | Build final observation-level summaries |

## Quick Start

Prerequisite: complete the IDS foundation steps in [01_ids/README.md](../01_ids/README.md).

```bash
Rscript 03_prism/scripts/01_build_pixel_maps.R
Rscript 03_prism/scripts/02_extract_prism.R
Rscript scripts/build_climate_summaries.R prism
```

## Key Outputs

| Output | Location | Notes |
|---|---|---|
| Pixel maps | `03_prism/data/processed/pixel_maps/` | One parquet per IDS layer |
| Yearly pixel values | `03_prism/data/processed/pixel_values/prism_{year}.parquet` | Wide-format yearly files |
| Observation summaries | `processed/climate/prism/damage_areas_summaries/` | One parquet per variable |

## Coverage Notes

PRISM covers the contiguous United States only. Alaska and Hawaii observations are excluded from this workstream; use TerraClimate or WorldClim for those regions.

## Related Docs

| If you want... | Go to... |
|---|---|
| shared climate architecture | [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) |
| detailed PRISM behavior and troubleshooting | [WORKFLOW.md](WORKFLOW.md) |
| reproduction order | [docs/REPRODUCE.md](../docs/REPRODUCE.md) |
| output inventory | [docs/DATA_PRODUCTS.md](../docs/DATA_PRODUCTS.md) |

## See also

- [Docs Hub](../docs/README.md)
- [Shared Architecture](../docs/ARCHITECTURE.md)
- [TerraClimate README](../02_terraclimate/README.md)
- [WorldClim README](../04_worldclim/README.md)
