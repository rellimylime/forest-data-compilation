# Forest Data Compilation

Compiled and cleaned datasets for forest disturbance analysis: USDA Forest Service aerial detection surveys (IDS), climate data (TerraClimate, PRISM, WorldClim), and related environmental variables.

**Author:** Emily Miller  
**Institution:** UC Santa Barbara, Bren School of Environmental Science & Management  
**Last Updated:** 2025-01-31

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
- **Access Method:** Point extraction via Google Earth Engine at IDS observation centroids

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
├── README.md                 # This file
├── config.yaml               # Central configuration (paths, variables, parameters)
├── METADATA_MASTER.csv       # Cross-dataset metadata tracking
├── .gitignore
├── renv.lock                 # R package dependencies
│
├── scripts/                  # Shared utility scripts
│   ├── 00_setup.R           # Environment setup and package loading
│   └── utils/
│       ├── load_config.R    # Configuration file loader
│       ├── gee_utils.R      # Google Earth Engine functions
│       └── metadata_utils.R # Metadata management helpers
│
├── local/                    # User-specific config (gitignored)
│   └── user_config.yaml     # GEE project ID, local paths
│
├── 01_ids/                   # Insect & Disease Survey data
├── 02_terraclimate/          # TerraClimate extractions
├── 03_prism/                 # PRISM climate data (planned)
├── 04_worldclim/             # WorldClim data (planned)
│
├── merged_data/              # Final merged outputs
└── templates/                # Documentation templates
```

### Dataset Directory Structure

Each dataset folder (`01_ids/`, `02_terraclimate/`, etc.) follows a consistent structure:

```
XX_datasetname/
├── README.txt            # Dataset overview, source, citation
├── data_dictionary.csv   # Field definitions, units, value ranges
├── cleaning_log.md       # Data quality issues and decisions
├── WORKFLOW.md           # Processing steps and script documentation
├── docs/                 # Original documentation from data provider
├── lookups/              # Code lookup tables (if applicable)
├── scripts/              # Dataset-specific processing scripts
└── data/
    ├── raw/              # Original downloaded data (gitignored)
    └── processed/        # Cleaned outputs (gitignored)
```

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
# 1. Download and clean IDS data
source("01_ids/scripts/01_download_ids.R")  # Download raw geodatabases
source("01_ids/scripts/02_inspect_ids.R")   # Generate data dictionary & lookups
source("01_ids/scripts/03_clean_ids.R")     # Clean and merge regions
source("01_ids/scripts/04_verify_ids.R")    # Validate output

# 2. Extract TerraClimate at IDS locations
source("02_terraclimate/scripts/01_extract_terraclimate.R")  # ~25 min for 4.5M points

# 3. Process and merge (coming soon)
# source("02_terraclimate/scripts/02_process_terraclimate.R")
# source("02_terraclimate/scripts/03_merge_ids_terraclimate.R")
```

### Detailed Workflow Documentation

Each dataset has its own `WORKFLOW.md` with:
- Script descriptions and dependencies
- Input/output specifications
- Decision log with rationale
- Troubleshooting guide

See:
- [`01_ids/WORKFLOW.md`](01_ids/WORKFLOW.md)
- [`02_terraclimate/WORKFLOW.md`](02_terraclimate/WORKFLOW.md)

---

## Data Outputs

### Current Outputs

| File | Location | Size | Description |
|------|----------|------|-------------|
| `ids_damage_areas_cleaned.gpkg` | `01_ids/data/processed/` | 3.8 GB | Cleaned IDS polygons (4.5M features) |
| `tc_r*_*.csv` (251 files) | `02_terraclimate/data/raw/` | ~500 MB | TerraClimate extractions by region-year |

### Planned Outputs

| File | Location | Description |
|------|----------|-------------|
| `terraclimate_scaled.csv` | `02_terraclimate/data/processed/` | Scaled climate values, single file |
| `ids_terraclimate_merged.csv` | `merged_data/` | IDS + TerraClimate joined dataset |

### Data Access

Raw and processed data files are **not tracked in git** due to size. To obtain:

1. **Run the pipeline** using scripts in this repository
2. **Contact the author** for pre-processed files
3. **Download from source** (see Data Sources above)

---

## Key Documentation

| Document | Purpose |
|----------|---------|
| `config.yaml` | Central configuration for all datasets |
| `*/README.txt` | Dataset overview, source, citation |
| `*/data_dictionary.csv` | Field definitions and metadata |
| `*/cleaning_log.md` | Data quality issues and cleaning decisions |
| `*/WORKFLOW.md` | Processing steps and script documentation |
| `*/lookups/*.csv` | Code-to-description lookup tables |

---

## Known Issues & Limitations

### IDS Data
- **Methodology break (~2015):** Legacy (trees per acre) vs. DMSM (percent canopy affected) measures are not directly comparable
- **Pancake features (14.7%):** Multiple observations share same geometry; don't sum ACRES naively
- **Survey effort variation:** More records in recent years reflects increased survey capacity, not necessarily more damage

### TerraClimate
- **Scale factors:** Raw values are integers; must apply scale factors for physical units
- **Annual means:** Flux variables (precipitation, ET) may need ×12 for annual totals
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