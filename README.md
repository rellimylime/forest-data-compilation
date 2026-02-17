# Forest Data Compilation

Compiled and cleaned datasets for forest disturbance analysis: USDA Forest Service aerial detection surveys (IDS), climate data (TerraClimate, PRISM, WorldClim), and related environmental variables.

**Author:** Emily Miller
**Institution:** UC Santa Barbara, Bren School of Environmental Science & Management
**Last Updated:** 2026-02-13

---

## Overview

This repository provides a reproducible pipeline for compiling forest disturbance observations with climate and environmental data across the United States. The primary goal is to create analysis-ready datasets linking **where and when** forest damage occurred (from aerial surveys) with **climate conditions** at those locations.

### Key Outputs

| Dataset | Description | Records | Status |
|---------|-------------|---------|--------|
| IDS Damage Areas | Forest insect/disease damage polygons (1997-2024) | 4,475,827 | ✅ Complete |
| TerraClimate | Climate variables at IDS observation centroids | 4,475,817 | ✅ Complete |
| PRISM | High-resolution US climate normals | — | 🔲 Planned |
| WorldClim | Global bioclimatic variables | — | 🔲 Planned |
| **Merged Dataset** | IDS + all climate variables | — | 🔲 In Progress |

---

## Directory and File Organization

### Top-Level Directories

| Directory | Purpose | Tracked in Git |
|-----------|---------|----------------|
| `01_ids/` | IDS (Insect & Disease Survey) data processing | ✅ Scripts & docs only |
| `02_terraclimate/` | TerraClimate climate data extraction | ✅ Scripts & docs only |
| `03_prism/` | PRISM climate data (planned) | ✅ Scripts & docs only |
| `04_worldclim/` | WorldClim climate data (planned) | ✅ Scripts & docs only |
| `05_era5/` | ERA5 climate data (planned) | ✅ Scripts & docs only |
| `scripts/` | Shared utilities and cross-dataset processing | ✅ All files |
| `docs/` | Project-wide documentation (architecture, guides) | ✅ All files |
| `processed/` | Cross-dataset derived outputs | ❌ Data only (gitignored) |
| `local/` | User-specific configuration files | ❌ All files (gitignored) |
| `renv/` | R package dependency lockfiles | ✅ Lock files only |

### Key Configuration Files

| File | Purpose |
|------|---------|
| `config.yaml` | Central configuration: data sources, variables, processing parameters |
| `local/user_config.yaml` | User-specific settings (GEE project ID, local paths) — **gitignored** |
| `.Renviron` | R environment variables (Python path for reticulate) — **gitignored** |
| `renv.lock` | R package dependency versions (reproducibility) |

### Documentation Files

| File | Location | Purpose |
|------|----------|---------|
| `README.md` | Root | This file — project overview and quick start |
| `ARCHITECTURE.md` | `docs/` | **Shared architecture documentation** (pixel decomposition pattern, time conventions, workflow steps) |
| `SETUP.md` | `scripts/` | Detailed installation and environment setup instructions |
| `README.txt` | Each dataset dir | Dataset-specific overview, source, citation, quick-start usage examples |
| `WORKFLOW.md` | Each dataset dir | Technical reference for dataset-specific scripts and decisions |
| `cleaning_log.md` | Each dataset dir | Data quality issues encountered and cleaning decisions made |
| `data_dictionary.csv` | Each dataset dir | Field definitions, units, value ranges |

### Standard Dataset Directory Structure

Each numbered dataset directory (`01_ids/`, `02_terraclimate/`, etc.) follows this consistent structure:

```
NN_datasetname/
├── README.txt                # Dataset overview, usage examples, citations
├── WORKFLOW.md               # Script documentation and decisions → links to docs/ARCHITECTURE.md
├── cleaning_log.md           # Data quality issues and cleaning decisions
├── data_dictionary.csv       # Field metadata
├── docs/                     # Original documentation from data provider (PDFs, etc.)
├── lookups/                  # Code-to-description lookup tables (CSV files)
├── scripts/
│   ├── 00_explore_*.R        # Optional: exploratory analysis (diagnostic only)
│   ├── 01_*.R                # Required: core processing scripts
│   ├── 02_*.R                # Required: core processing scripts
│   └── ...
└── data/
    ├── raw/                  # Original downloaded data (gitignored)
    └── processed/            # Cleaned outputs (gitignored)
```

### Shared Scripts Directory

`scripts/` contains utilities and cross-dataset processing scripts used by **all** climate datasets:

| File | Type | Purpose |
|------|------|---------|
| `00_setup.R` | Setup | Load packages, initialize GEE, check environment |
| `reshape_pixel_values.R` | Processing | *(Optional)* Convert wide-format yearly files → long-format parquet (generic for all datasets) |
| `build_climate_summaries.R` | Processing | Compute area-weighted observation-level summaries (reads source files directly; generic for all datasets) |
| **utils/climate_extract.R** | Utility | Pixel map building, GEE extraction framework |
| **utils/gee_utils.R** | Utility | GEE initialization, sf↔ee conversions |
| **utils/time_utils.R** | Utility | Calendar ↔ water year conversions |
| **utils/load_config.R** | Utility | config.yaml loader |
| **utils/metadata_utils.R** | Utility | Metadata tracking helpers |
| **utils/cds_utils.R** | Utility | Climate Data Store (CDS) API utilities for ERA5 |

**Usage:**
```r
# Build observation-level summaries (reads directly from yearly source files)
Rscript scripts/build_climate_summaries.R terraclimate

# Optional: reshape to long-format pixel values for custom analysis
Rscript scripts/reshape_pixel_values.R terraclimate
```

### Processed Outputs Directory

`processed/` contains cross-dataset derived files not specific to any one input dataset:

```
processed/
├── ids/                                      # IDS-specific derived outputs
│   ├── damage_area_to_surveyed_area.parquet  # Spatial assignment: damage → survey area
│   └── damage_area_area_metrics.parquet      # Area metrics in EPSG:5070
└── climate/                                  # Standardized climate outputs
    ├── terraclimate/
    │   └── damage_areas_summaries/           # Per-variable parquet files (open_dataset())
    │       ├── tmmx.parquet                  # weighted_mean, value_min, value_max per obs
    │       ├── tmmn.parquet
    │       └── ...                           # One file per climate variable
    ├── prism/                                # (Same structure, planned)
    ├── worldclim/                            # (Same structure, planned)
    └── era5/                                 # (Same structure, planned)
```

---

## Data Sources

### 1. Insect and Disease Detection Survey (IDS)
- **Source:** [USDA Forest Service Forest Health Protection](https://www.fs.usda.gov/foresthealth/)
- **Description:** Annual aerial and ground survey data detecting forest insect and disease damage across all USFS regions
- **Coverage:** Continental US, Alaska, Hawaii (1997-2024)
- **Format:** Geodatabase (.gdb) → cleaned to GeoPackage (.gpkg)
- **Features:** 4.5M damage area polygons with host species, damage agent, severity, and extent

### 2. TerraClimate
- **Source:** [Climatology Lab](https://www.climatologylab.org/terraclimate.html)
- **Citation:** Abatzoglou et al. (2018), Scientific Data
- **Description:** High-resolution (~4km) global climate and water balance data
- **Coverage:** Global, monthly (1958-present)
- **Variables:** 14 climate variables (temperature, precipitation, ET, drought indices, etc.)
- **Access Method:** Pixel-level extraction via Google Earth Engine (no per-observation rasters)

### 3. PRISM *(Planned)*
- **Source:** [PRISM Climate Group](https://prism.oregonstate.edu/)
- **Description:** High-resolution climate data for the contiguous United States
- **Coverage:** CONUS only, monthly/daily (1895-present)

### 4. WorldClim *(Planned)*
- **Source:** [WorldClim](https://www.worldclim.org/)
- **Description:** Global climate and bioclimatic variables
- **Coverage:** Global, climatological normals

---

## Repository Structure

```
forest-data-compilation/
├── README.md                      # Project overview (this file)
├── config.yaml                    # Central configuration
├── .gitignore
├── renv.lock                      # R package dependencies
│
├── docs/                          # Project-wide documentation
│   └── ARCHITECTURE.md            # Shared pixel decomposition architecture
│
├── scripts/                       # Shared utilities (all climate datasets)
│   ├── 00_setup.R                 # Environment setup
│   ├── reshape_pixel_values.R     # Wide → long format conversion
│   ├── build_climate_summaries.R  # Observation-level weighted means
│   ├── SETUP.md                   # Installation guide
│   └── utils/                     # Utility modules
│
├── processed/                     # Cross-dataset outputs (gitignored)
│   ├── ids/                       # IDS-specific products
│   └── climate/                   # Standardized climate by dataset
│
├── local/                         # User-specific config (gitignored)
│   └── user_config.yaml           # GEE project ID, paths
│
├── 01_ids/                        # Insect & Disease Survey
├── 02_terraclimate/               # TerraClimate (✅ complete)
├── 03_prism/                      # PRISM (planned)
├── 04_worldclim/                  # WorldClim (planned)
└── 05_era5/                       # ERA5 (planned)
```

See **[Directory and File Organization](#directory-and-file-organization)** above for detailed descriptions.

---

## Setup

### Prerequisites

- **R** (≥ 4.3.0)
- **Python** (≥ 3.9) with `earthengine-api` package
- **Google Earth Engine** account with authenticated project

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/forest-data-compilation.git
   cd forest-data-compilation
   ```

2. **Restore R environment:**
   ```r
   # In R console
   renv::restore()
   ```

3. **Configure Google Earth Engine:**
   
   Create `local/user_config.yaml`:
   ```yaml
   gee_project: "your-gee-project-id"
   ```
   
   Authenticate GEE (one-time):
   ```bash
   earthengine authenticate
   ```

4. **Set Python path** (if needed):
   
   Add to `.Renviron`:
   ```
   RETICULATE_PYTHON=/path/to/your/python
   ```

5. **Run setup script:**
   ```r
   source("scripts/00_setup.R")
   ```

---

## Workflow

### Quick Start

```r
# === STEP 1: IDS Foundation (Required) ===
source("01_ids/scripts/01_download_ids.R")    # Download raw geodatabases (~1.6GB)
source("01_ids/scripts/02_inspect_ids.R")     # Generate data dictionary & lookups
source("01_ids/scripts/03_clean_ids.R")       # Clean and merge 10 regions
source("01_ids/scripts/04_verify_ids.R")      # Validate cleaned output

# === STEP 2: IDS Spatial Products (Required) ===
source("01_ids/scripts/06_assign_surveyed_areas.R")  # Spatial join: damage → survey areas
source("01_ids/scripts/07_compute_area_metrics.R")   # Compute area metrics (EPSG:5070)

# === STEP 3: Climate Extraction (Per Dataset) ===
# TerraClimate example:
source("02_terraclimate/scripts/01_build_pixel_maps.R")      # Required: polygon → pixel mapping
source("02_terraclimate/scripts/02_extract_terraclimate.R")  # Required: GEE extraction

# === STEP 4: Build Observation Summaries (Generic, Per Dataset) ===
Rscript scripts/build_climate_summaries.R terraclimate   # Area-weighted summaries

# Repeat Steps 3-4 for additional datasets:
# Rscript scripts/build_climate_summaries.R prism
# (Same pattern for worldclim, era5)

# === OPTIONAL: Reshape pixel values to long format (for custom analysis) ===
# Rscript scripts/reshape_pixel_values.R terraclimate
```

### Optional Exploratory Scripts

These scripts generate diagnostic outputs but are **not required** for the core workflow:

```r
# IDS temporal coverage analysis (optional)
source("01_ids/scripts/05_explore_ids_coverage.R")

# TerraClimate extraction testing (optional)
source("02_terraclimate/scripts/00_explore_terraclimate.R")
```

### Detailed Workflow Documentation

**Architecture Overview:**
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — **Shared pixel decomposition pattern** used by all climate datasets

**Dataset-Specific Technical Details:**
- [`01_ids/WORKFLOW.md`](01_ids/WORKFLOW.md) — IDS processing scripts and decisions
- [`02_terraclimate/WORKFLOW.md`](02_terraclimate/WORKFLOW.md) — TerraClimate GEE extraction
- Each `WORKFLOW.md` includes:
  - Script descriptions and dependencies
  - Input/output specifications
  - Decision log with rationale
  - Troubleshooting guide

---

## Data Outputs

### IDS Outputs

| File | Location | Description |
|------|----------|-------------|
| `ids_layers_cleaned.gpkg` | `01_ids/data/processed/` | Cleaned IDS layers (damage areas, damage points, surveyed areas) |
| `damage_area_to_surveyed_area.parquet` | `processed/ids/` | Spatial assignment: each damage area to its best-matching surveyed area |
| `damage_area_area_metrics.parquet` | `processed/ids/` | Area metrics: damage_area_m2, survey_area_m2, damage_frac_of_survey (EPSG:5070) |

### Climate Outputs (per dataset)

| File | Location | Description |
|------|----------|-------------|
| `*_pixel_map.parquet` | `XX_dataset/data/processed/pixel_maps/` | Pixel map: observation to raster pixel with coverage_fraction |
| `*_{year}.parquet` | `XX_dataset/data/processed/pixel_values/` | Wide-format pixel values per year |
| `damage_areas_summaries/` | `processed/climate/<dataset>/` | Per-variable parquet files with area-weighted summaries (read with `open_dataset()`) |

### Data Access

Raw and processed data files are **not tracked in git** due to size. To obtain:

1. **Run the pipeline** using scripts in this repository
2. **Contact the author** for pre-processed files
3. **Download from source** (see Data Sources above)

---

## Key Documentation

| Document | Location | Purpose |
|----------|----------|---------|
| `ARCHITECTURE.md` | `docs/` | **Shared pixel decomposition architecture** (workflow, time conventions, schemas) |
| `SETUP.md` | `scripts/` | Detailed installation and environment setup instructions |
| `config.yaml` | Root | Central configuration for all datasets (URLs, variables, parameters) |
| `README.txt` | Each dataset dir | Dataset overview, source, citation, usage examples |
| `WORKFLOW.md` | Each dataset dir | Dataset-specific script documentation and decisions |
| `cleaning_log.md` | Each dataset dir | Data quality issues and cleaning decisions |
| `data_dictionary.csv` | Each dataset dir | Field definitions, units, value ranges |
| `lookups/*.csv` | Dataset dirs | Code-to-description lookup tables |

---

## Architecture: Pixel Decomposition

Climate data is linked to IDS observations through a **pixel decomposition** approach rather than clipping rasters per observation. This pattern is shared identically across all climate datasets (TerraClimate, PRISM, WorldClim, ERA5).

**Key Concepts:**
- Each IDS observation maps to the climate pixels it overlaps
- Climate values extracted once per unique pixel (not per observation)
- **coverage_fraction** = area(observation ∩ pixel) / area(pixel) — used as weight for area-weighted means
- Both **calendar year** and **water year** retained (Oct-Sep water year)
- IDS keeps original `SURVEY_YEAR` (not forced to water year)

**For complete architecture documentation, see:**
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)

This document covers:
- Pixel decomposition workflow (4 standard steps)
- Time conventions (calendar vs water year)
- Weighted mean calculations
- Shared utility functions
- Data format schemas
- Implementation checklist for new datasets

---

## Known Issues & Limitations

### IDS Data
- **Methodology break (~2015):** Legacy (trees per acre) vs. DMSM (percent canopy affected) measures are not directly comparable
- **Pancake features (14.7%):** Multiple observations share same geometry; don't sum ACRES naively
- **Survey effort variation:** More records in recent years reflects increased survey capacity, not necessarily more damage

### TerraClimate
- **Scale factors:** Raw values are integers; must apply scale factors for physical units
- **Annual means:** Flux variables (precipitation, ET) may need x12 for annual totals
- **10 excluded observations:** Invalid geometries couldn't produce centroids

### General
- **Large file sizes:** Raw IDS data ~1.6 GB, cleaned ~3.8 GB; not suitable for git
- **GEE dependency:** TerraClimate extraction requires Google Earth Engine access
- **Alaska/Hawaii CRS:** Original data in regional Albers projections; transformed to WGS84

---

## Configuration

### config.yaml

Central configuration file containing:
- Dataset source URLs and paths
- Variable definitions and scale factors
- Processing parameters
- Output specifications

```yaml
# Example structure
raw:
  ids:
    source: "https://www.fs.usda.gov/foresthealth/..."
    local_dir: "01_ids/data/raw"
    files:
      region1:
        url: "..."
        filename: "CONUS_Region1_AllYears.gdb.zip"
  terraclimate:
    gee_asset: "IDAHO_EPSCOR/TERRACLIMATE"
    variables:
      tmmx:
        description: "Maximum temperature"
        units: "°C"
        scale: 0.1
      # ...

params:
  crs: "EPSG:4326"
  time_range:
    start_year: 1997
    end_year: 2024
```

### local/user_config.yaml

User-specific settings (gitignored):
```yaml
gee_project: "your-gee-project-id"
# Add other local overrides as needed
```

---

## Contributing

This repository is primarily for personal research use. If you find errors or have suggestions:

1. Open an issue describing the problem
2. For code changes, submit a pull request with clear description

---

## Citation

If you use this compiled dataset, please cite:

**This repository:**
```
Miller, E. (2025). Forest Data Compilation: Integrated forest disturbance and 
climate datasets for the United States. UC Santa Barbara.
https://github.com/yourusername/forest-data-compilation
```

**Original data sources:**

IDS:
```
USDA Forest Service. Forest Health Protection Insect and Disease Detection 
Survey Data. https://www.fs.usda.gov/foresthealth/
```

TerraClimate:
```
Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018). 
TerraClimate, a high-resolution global dataset of monthly climate and climatic 
water balance from 1958-2015. Scientific Data 5:170191.
https://doi.org/10.1038/sdata.2017.191
```

---

## License

Data sources retain their original licenses. See individual dataset README files for specific terms.

Code in this repository is available under [MIT License](LICENSE).

---

## Contact

**Emily Miller**  
Master of Environmental Data Science (MEDS), Class of 2026  
Bren School of Environmental Science & Management  
UC Santa Barbara  

For questions about this repository, please open an issue or contact via UCSB email.
