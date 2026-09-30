## Setup Instructions

### Prerequisites
- R 4.x
- Conda (Anaconda or Miniconda)
- Google Earth Engine account

### 1. Clone and restore R packages
```bash
git clone <repo-url>
cd forest-data-compilation
```

In R:
```r
renv::restore()
```

### 2. Create Python environment
```bash
conda create -n rgee python=3.10
conda activate rgee
pip install earthengine-api
```

### 3. Configure Python path
Find your Python path:
```bash
which python # while conda env is active
```

Create `.Renviron` in the project root:
```
RETICULATE_PYTHON=/path/to/conda/envs/rgee/bin/python
```

### 4. Authenticate with GEE
```bash
earthengine authenticate
```

### 5. Run setup script
Restart R, then:
```r
source("scripts/00_setup.R")
```

### 6. Install dashboard dependencies (optional)
The unified dashboard requires Python packages. Install from the repo root:
```bash
pip install -r requirements.txt
```

Launch the dashboard:
```bash
streamlit run docs/dashboard/app.py
```

The dashboard covers all datasets and active analysis products. Its Find data and Build a dataset pages search committed snapshots, so browsing variables, seeing compatible joins, and generating an export query do not require access to the data directories. See [`docs/dashboard/README.md`](../docs/dashboard/README.md) for usage and troubleshooting.

The generated SQL expects DuckDB when you choose to execute it. DuckDB is not required merely to browse or generate a query.
