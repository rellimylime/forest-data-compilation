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