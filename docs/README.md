# Documentation Hub

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Shared Scripts](../scripts/README.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This is the main navigation page for the repository documentation. Use it to move between the high-level overview, exact reproduction steps, technical workflow references, and the code that implements each workstream. The companion [Data Products](DATA_PRODUCTS.md) page also documents the minimal server-aligned directory skeleton that is now tracked with `.gitkeep` placeholders where needed.

## Start Here

| If you want to... | Go to... |
|---|---|
| Understand the repo at a glance | [Repo Home](../README.md) |
| See the whole pipeline visually | [Pipeline Map](PIPELINE_MAP.md) |
| Reproduce the active production pipelines | [Reproduce](REPRODUCE.md) |
| Find outputs and file locations | [Data Products](DATA_PRODUCTS.md) |
| Understand shared climate architecture | [Architecture](ARCHITECTURE.md) |
| Review QC and validation coverage | [Testing and QC](TESTING.md) |
| Set up the environment | [Setup](../scripts/SETUP.md) |
| Understand root-level helper scripts | [Shared Scripts](../scripts/README.md) |
| Understand FIA plot design visually | [FIA visual explainer](fia-explorer.html) |
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

- [FIA visual explainer](fia-explorer.html)
- [FIA overview](../05_fia/README.md)
- [FIA technical workflow](../05_fia/WORKFLOW.md)
- [FIA scripts](../05_fia/scripts/)

### I want to inspect species niches and thermophilization

- [Species niche overview](../06_species_niches/README.md)
- [Species niche technical workflow](../06_species_niches/WORKFLOW.md)
- [Species niche methods](../06_species_niches/docs/methods_species_niches.md)
- [Species niche QA guide](../06_species_niches/qa/README.md)
- [Thermophilization overview](../07_thermophilization/README.md)

### I want to inspect climate extraction

- [Shared architecture](ARCHITECTURE.md)
- [TerraClimate overview](../02_terraclimate/README.md)
- [PRISM overview](../03_prism/README.md)
- [WorldClim overview](../04_worldclim/README.md)
- [Shared scripts overview](../scripts/README.md)

### I want to inspect the archived ERA5 reference

- [Archived ERA5 README](../archive/05_era5/README.md)
- [Archived ERA5 workflow](../archive/05_era5/WORKFLOW.md)
- [Archived ERA5 scripts](../archive/05_era5/scripts/)

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
| `06_species_niches/` | BIEN range-map and TerraClimate species niches | [README](../06_species_niches/README.md) | [WORKFLOW](../06_species_niches/WORKFLOW.md) | [scripts/](../06_species_niches/scripts/) |
| `07_thermophilization/` | Plot-year community climate-affinity metrics, repeated-survey change rates, and disturbance proportions | [README](../07_thermophilization/README.md) | [Output guide](../07_thermophilization/README.md#output-reference) | [scripts/](../07_thermophilization/scripts/) |
| `archive/05_era5/` | Archived ERA5 extraction reference and directory layout | [README](../archive/05_era5/README.md) | [WORKFLOW](../archive/05_era5/WORKFLOW.md) | [scripts/](../archive/05_era5/scripts/) |

## Core Documentation

| Page | Purpose |
|---|---|
| [Reproduce](REPRODUCE.md) | Exact production run order, grouped by workstream |
| [Pipeline Map](PIPELINE_MAP.md) | GitHub-renderable diagram plus links to the dashboard and HTML companion |
| [Data Products](DATA_PRODUCTS.md) | Main outputs, tracked review files, local-only artifacts, and server-aligned directory paths |
| [Architecture](ARCHITECTURE.md) | Shared climate extraction concepts and data model |
| [Testing and QC](TESTING.md) | Optional diagnostics, validation steps, and known QC gaps |
| [FIA visual explainer](fia-explorer.html) | Static visual guide to FIA plot design, sampling grain, and FIADB tables |

## Where the Code Lives

| Category | Location | Notes |
|---|---|---|
| Production | [01_ids/scripts/](../01_ids/scripts/) | IDS production and QC scripts |
| Production | [02_terraclimate/scripts/](../02_terraclimate/scripts/) | TerraClimate production plus exploratory script |
| Production | [03_prism/scripts/](../03_prism/scripts/) | PRISM production scripts |
| Production | [04_worldclim/scripts/](../04_worldclim/scripts/) | WorldClim production scripts |
| Production | [05_fia/scripts/](../05_fia/scripts/) | FIA production scripts |
| Production | [06_species_niches/scripts/](../06_species_niches/scripts/) | Species climate-niche production scripts |
| Production | [07_thermophilization/scripts/](../07_thermophilization/scripts/) | Community climate-affinity, disturbance severity, and repeated-survey change scripts |
| Shared | [scripts/](../scripts/README.md) | Setup helper, shared climate summary builder, reusable utilities, demos, and test runner |
| Archived | [archive/05_era5/scripts/](../archive/05_era5/scripts/) | ERA5 reference implementation retained outside the active run path |
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
