# Documentation Hub

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This is the main navigation page for the repository documentation. Use it to move between the high-level overview, exact reproduction steps, technical workflow references, and the code that implements each workstream. Treat it as the index, not another full explanation layer.

## Start Here

| If you want to... | Go to... |
|---|---|
| Understand the repo at a glance | [Repo Home](../README.md) |
| See the whole pipeline visually | [Pipeline Map](PIPELINE_MAP.md) |
| Reproduce the production pipelines | [Reproduce](REPRODUCE.md) |
| Find outputs and file locations | [Data Products](DATA_PRODUCTS.md) |
| Understand shared climate architecture | [Architecture](ARCHITECTURE.md) |
| Review QC and validation coverage | [Testing and QC](TESTING.md) |
| Set up the environment | [Setup](../scripts/SETUP.md) |
| Use the main visual guide while working locally | [Dashboard entrypoint](dashboard/app.py) |

## Reviewer Paths

### I want the high-level overview

- [Repo Home](../README.md)
- [Pipeline Map](PIPELINE_MAP.md)
- [Data Products](DATA_PRODUCTS.md)

### I want the exact run order

- [Reproduce](REPRODUCE.md)
- [IDS README](../01_ids/README.md)
- [FIA README](../05_fia/README.md)

### I want to inspect the FIA pipeline

- [FIA overview](../05_fia/README.md)
- [FIA technical workflow](../05_fia/WORKFLOW.md)
- [FIA scripts](../05_fia/scripts/)

### I want to inspect climate extraction

- [Shared architecture](ARCHITECTURE.md)
- [Dashboard entrypoint](dashboard/app.py)
- [TerraClimate overview](../02_terraclimate/README.md)
- [PRISM overview](../03_prism/README.md)
- [WorldClim overview](../04_worldclim/README.md)
- [Shared climate summary script](../scripts/build_climate_summaries.R)

### I want outputs and file locations

- [Data Products](DATA_PRODUCTS.md)
- [Dashboard](dashboard/)

### I want QC / validation

- [Testing and QC](TESTING.md)
- [IDS QC README](../01_ids/scripts/qc/README.md)
- [TerraClimate cleaning log](../02_terraclimate/cleaning_log.md)

## Workstream Guide

| Directory | What it covers | Overview | Technical workflow | Scripts |
|---|---|---|---|---|
| `01_ids/` | IDS download, cleaning, spatial joins, area metrics | [README](../01_ids/README.md) | [WORKFLOW](../01_ids/WORKFLOW.md) | [scripts/](../01_ids/scripts/) |
| `02_terraclimate/` | GEE-based TerraClimate extraction for IDS locations | [README](../02_terraclimate/README.md) | [WORKFLOW](../02_terraclimate/WORKFLOW.md) | [scripts/](../02_terraclimate/scripts/) |
| `03_prism/` | PRISM web-service extraction for CONUS IDS locations | [README](../03_prism/README.md) | [WORKFLOW](../03_prism/WORKFLOW.md) | [scripts/](../03_prism/scripts/) |
| `04_worldclim/` | WorldClim local GeoTIFF extraction for IDS locations | [README](../04_worldclim/README.md) | [WORKFLOW](../04_worldclim/WORKFLOW.md) | [scripts/](../04_worldclim/scripts/) |
| `05_fia/` | FIA plot summaries, disturbance, treatment, and site climate | [README](../05_fia/README.md) | [WORKFLOW](../05_fia/WORKFLOW.md) | [scripts/](../05_fia/scripts/) |

## Core Documentation

| Page | Purpose |
|---|---|
| [Reproduce](REPRODUCE.md) | Exact production run order, grouped by workstream |
| [Pipeline Map](PIPELINE_MAP.md) | GitHub-renderable diagram plus links to the dashboard and HTML companion |
| [Data Products](DATA_PRODUCTS.md) | Main outputs, what is git-tracked, and which scripts produce each output family |
| [Architecture](ARCHITECTURE.md) | Shared climate extraction concepts and data model |
| [Testing and QC](TESTING.md) | Optional diagnostics, validation steps, and known QC gaps |

## Where the Code Lives

| Category | Location | Notes |
|---|---|---|
| Production | [01_ids/scripts/](../01_ids/scripts/) | IDS production and QC scripts |
| Production | [02_terraclimate/scripts/](../02_terraclimate/scripts/) | TerraClimate production plus exploratory script |
| Production | [03_prism/scripts/](../03_prism/scripts/) | PRISM production scripts |
| Production | [04_worldclim/scripts/](../04_worldclim/scripts/) | WorldClim production scripts |
| Production | [05_fia/scripts/](../05_fia/scripts/) | FIA production scripts |
| Shared | [scripts/utils/](../scripts/utils/) | Reusable helpers across workstreams |
| Shared | [scripts/build_climate_summaries.R](../scripts/build_climate_summaries.R) | Shared climate summary builder |
| Demo | [scripts/demos/](../scripts/demos/) | Example analyses using finished outputs |
| Dashboard | [docs/dashboard/](dashboard/) | Streamlit app and pages |

## Visuals

- [Dashboard entrypoint](dashboard/app.py) for the main guided visual experience when working locally.
- [Pipeline Map](PIPELINE_MAP.md) for the GitHub-friendly Markdown summary.
- [pipeline_diagram.html](pipeline_diagram.html) for a short static HTML companion.

## See also

- [Repo Home](../README.md)
- [Reproduce](REPRODUCE.md)
- [Data Products](DATA_PRODUCTS.md)
- [Setup](../scripts/SETUP.md)
