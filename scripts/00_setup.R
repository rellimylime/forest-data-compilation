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
2. Install Google Earth Engine Python dependencies (if needed)
3. Authenticate with Google Earth Engine

REQUIREMENTS:
- R 4.x
- Google Earth Engine account (https://earthengine.google.com/)

================================================================================
")

# --- Step 1: Restore R environment --------------------------------------------
cat("\n[1/4] Checking R packages...\n")

if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv")
}

renv::restore(prompt = FALSE)
cat("✓ R packages restored\n")

# --- Step 2: Load rgee --------------------------------------------------------
cat("\n[2/4] Loading rgee...\n")
library(rgee)

# --- Step 3: Check/install Earth Engine Python env ----------------------------
cat("\n[3/4] Checking Earth Engine Python environment...\n")

# Check if EE is already set up
tryCatch({
  ee_check()
  cat("✓ Earth Engine Python environment OK\n")
}, error = function(e) {
  cat("Installing Earth Engine Python environment...\n")
  cat("This may take a few minutes on first run.\n\n")
  ee_install()
})

# --- Step 4: Authenticate -----------------------------------------------------
cat("\n[4/4] Authenticating with Google Earth Engine...\n\n")

# Prompt for email
user_email <- readline(prompt = "Enter your GEE-registered email: ")

if (nchar(user_email) == 0) {
  stop("Email is required for GEE authentication.")
}

# Initialize (will prompt for browser auth if needed)
ee_Initialize(user = user_email, drive = TRUE)

# --- Test connection ----------------------------------------------------------
cat("\nTesting Earth Engine connection...\n")
test_result <- tryCatch({
  ee$String("Setup successful!")$getInfo()
}, error = function(e) {
  stop("Earth Engine connection failed: ", e$message)
})

cat("✓", test_result, "\n")

# --- Save user email locally (not in git) -------------------------------------
# Create a local config that's gitignored
local_config <- list(
  gee_user = user_email,
  setup_date = Sys.Date()
)

dir.create("local", showWarnings = FALSE)
yaml::write_yaml(local_config, "local/user_config.yaml")

cat("\n
================================================================================
SETUP COMPLETE

Your GEE email has been saved to local/user_config.yaml (gitignored).
Future scripts will read from this file.

To test your setup:
  source('scripts/test_gee_connection.R')

================================================================================
\n")