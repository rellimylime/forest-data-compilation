# ==============================================================================
# 05_era5/scripts/01_download_era5.R
# Download ERA5 reanalysis data via CDS API
#
# Prerequisites:
# 1. Register at https://cds.climate.copernicus.eu/
# 2. Create ~/.cdsapirc with your credentials:
#    url: https://cds.climate.copernicus.eu/api
#    key: <uid>:<api-key>
# 3. Install Python cdsapi: pip install cdsapi
# ==============================================================================

library(here)
library(yaml)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/cds_utils.R"))

# Load config
config <- load_config()
era5_config <- config$raw$era5
time_config <- config$params$time_range

# Paths
output_dir <- here(era5_config$local_dir)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("ERA5 Daily Reanalysis Downloader (CDS API)\n")
cat("==========================================\n\n")

cat(sprintf("Source: %s\n", era5_config$source))
cat(sprintf("Resolution: %s\n", era5_config$spatial_resolution))
cat(sprintf("Temporal: %s\n", era5_config$temporal_resolution))
cat(sprintf("Coverage: %s\n\n", era5_config$coverage))

# ------------------------------------------------------------------------------
# Check CDS API setup
# ------------------------------------------------------------------------------

cat("Checking CDS API configuration...\n")

cdsapirc_path <- file.path(Sys.getenv("HOME"), ".cdsapirc")
if (!file.exists(cdsapirc_path)) {
  stop(sprintf("
CDS API credentials not found at %s

To set up:
1. Register at https://cds.climate.copernicus.eu/
2. Get your API key from your user profile
3. Create %s with:
   url: https://cds.climate.copernicus.eu/api
   key: <your-uid>:<your-api-key>
", cdsapirc_path, cdsapirc_path))
}

cat("  Found ~/.cdsapirc\n")

# Initialize CDS client
cat("  Initializing CDS API client...\n")
client <- init_cds()
cat("  CDS API initialized successfully\n\n")

# ------------------------------------------------------------------------------
# Download ERA5 data
# ------------------------------------------------------------------------------

years <- time_config$start_year:time_config$end_year
variables <- names(era5_config$variables)
area <- era5_config$cds_area  # [north, west, south, east]

cat(sprintf("Variables to download: %d\n", length(variables)))
cat(sprintf("  %s\n", paste(variables, collapse = ", ")))
cat(sprintf("\nYears: %d-%d (%d years)\n", min(years), max(years), length(years)))
cat(sprintf("Area: [N=%.0f, W=%.0f, S=%.0f, E=%.0f]\n\n", area[1], area[2], area[3], area[4]))

total_files <- length(variables) * length(years)
cat(sprintf("Total files to download: %d\n", total_files))
cat("Note: Each file is ~1-2 GB. Ensure sufficient disk space.\n")
cat("      Downloads may take several hours.\n\n")

# Download each variable for each year
for (year in years) {
  cat(sprintf("\nYear %d:\n", year))

  for (var_name in variables) {
    var_config <- era5_config$variables[[var_name]]
    era5_name <- var_config$era5_name

    var_dir <- file.path(output_dir, var_name)
    dir.create(var_dir, recursive = TRUE, showWarnings = FALSE)

    output_path <- file.path(var_dir, sprintf("%s_%d.nc", var_name, year))

    download_era5_variable(
      client = client,
      variable_name = var_name,
      era5_name = era5_name,
      year = year,
      area = area,
      output_path = output_path
    )
  }
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n==========================================\n")
cat("Download Summary:\n")

for (var_name in variables) {
  var_dir <- file.path(output_dir, var_name)
  nc_files <- list.files(var_dir, pattern = "\\.nc$")
  cat(sprintf("  %s: %d files\n", var_name, length(nc_files)))
}

cat(sprintf("\nOutput directory: %s\n", output_dir))
cat("\nNext: Run 02_build_pixel_maps.R, then 03_extract_era5.R\n")
