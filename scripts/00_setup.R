# ==============================================================================
# 00_setup.R
# One-time project setup - run this first on a new machine
# ==============================================================================

cat("
================================================================================
FOREST DATA COMPILATION - PROJECT SETUP
================================================================================

This script will:
1. Restore R packages from renv.lock
2. Guide you through Google Earth Engine Python setup
3. Create local configuration files

REQUIREMENTS:
- R 4.x
- Conda (Anaconda or Miniconda)
- Google Earth Engine account (https://earthengine.google.com/)

================================================================================
")

# --- Step 1: Restore R environment --------------------------------------------
cat("\n[1/5] Checking R packages...\n")

if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

renv::restore(prompt = FALSE)
cat("✓ R packages restored\n")

# --- Step 2: Check for conda environment --------------------------------------
cat("\n[2/5] Checking Python environment...\n")

library(reticulate)

# Check if RETICULATE_PYTHON is set
python_path <- Sys.getenv("RETICULATE_PYTHON", unset = "")

if (nzchar(python_path) && file.exists(python_path)) {
  cat("✓ Python path found:", python_path, "\n")
} else {
  cat("
ERROR: Python environment not configured.

Please complete these steps before running this script:

1. Create a conda environment with earthengine-api:
 
   conda create -n rgee python=3.10
   conda activate rgee
   pip install earthengine-api

2. Find your Python path:
 
   which python  # on Mac/Linux
   where python  # on Windows

3. Create a .Renviron file in this project directory with:
 
   RETICULATE_PYTHON=/path/to/your/conda/envs/rgee/bin/python

4. Restart R and run this script again.

")
  stop("Python environment not configured. See instructions above.")
}

# --- Step 3: Test Earth Engine import -----------------------------------------
cat("\n[3/5] Testing Earth Engine import...\n")

ee <- tryCatch({
  import("ee")
}, error = function(e) {
  stop("Failed to import earthengine-api. Is it installed in your conda env?\n",
       "Run: pip install earthengine-api")
})

cat("✓ Earth Engine module loaded\n")

# --- Step 4: Authenticate and get project ID ----------------------------------
cat("\n[4/5] Configuring Google Earth Engine...\n")

# Check if already authenticated
auth_needed <- tryCatch({
  ee$Initialize()
  FALSE
}, error = function(e) {
  TRUE
})

if (auth_needed) {
  cat("\nYou need to authenticate with Google Earth Engine.\n")
  cat("Run this in your terminal:\n\n")
  cat(" earthengine authenticate\n\n")
  cat("Then re-run this script.\n")
  stop("GEE authentication required.")
}

# Prompt for project ID
cat("\nEnter your GEE project ID (e.g., 'my-gee-project'): ")
gee_project <- readline()

if (nchar(gee_project) == 0) {
  stop("Project ID is required.")
}

# Test with project
ee$Initialize(project = gee_project)

test_result <- tryCatch({
  ee$String("Setup successful!")$getInfo()
}, error = function(e) {
  stop("Connection failed. Check your project ID: ", e$message)
})

cat("✓", test_result, "\n")

# --- Step 5: Save local config ------------------------------------------------
cat("\n[5/5] Saving configuration...\n")

local_config <- list(
  gee_project = gee_project,
  setup_date = as.character(Sys.Date())
)

dir.create("local", showWarnings = FALSE)
yaml::write_yaml(local_config, "local/user_config.yaml")

cat("✓ Configuration saved to local/user_config.yaml\n")

cat("
================================================================================
SETUP COMPLETE

Your configuration has been saved. The following files are gitignored:
- .Renviron (Python path)
- local/user_config.yaml (GEE project ID)

To test your setup:
 source(here::here('scripts/utils/gee_utils.R'))
 ee <- init_gee()
 ee$String('Hello from GEE!')$getInfo()

================================================================================
")