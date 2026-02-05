# ==============================================================================
# 03_prism/scripts/02_extract_prism.R
# Extract PRISM 800m pixel values from GEE for all unique pixels
# ==============================================================================

library(here)
library(yaml)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
prism_config <- config$raw$prism
time_config <- config$params$time_range

# Paths
pixel_map_dir <- here(prism_config$output_dir, "pixel_maps")
output_dir <- here(prism_config$output_dir, "pixel_values")

cat("PRISM Extraction (GEE 800m)\n")
cat("================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Load pixel maps and get unique pixels
# ------------------------------------------------------------------------------

cat("Step 1: Loading pixel maps...\n")

all_pixels <- list()

for (layer in c("damage_areas", "damage_points", "surveyed_areas")) {
  pm_file <- file.path(pixel_map_dir, paste0(layer, "_pixel_map.parquet"))

  if (file.exists(pm_file)) {
    pm <- read_parquet(pm_file)
    all_pixels[[layer]] <- get_unique_pixels(pm)
    cat(sprintf("  %s: %d unique pixels\n", layer, nrow(all_pixels[[layer]])))
  } else {
    cat(sprintf("  %s: pixel map not found, skipping\n", layer))
  }
}

pixel_coords <- bind_rows(all_pixels) %>%
  distinct(pixel_id, x, y)

cat(sprintf("\nTotal unique pixels across all layers: %d\n", nrow(pixel_coords)))
cat("  (This is ~25x more than TerraClimate due to higher resolution)\n")

# ------------------------------------------------------------------------------
# Step 2: Initialize GEE
# ------------------------------------------------------------------------------

cat("\nStep 2: Initializing Google Earth Engine...\n")
ee <- init_gee()
cat("  GEE initialized successfully\n")

# ------------------------------------------------------------------------------
# Step 3: Extract climate values from GEE
# ------------------------------------------------------------------------------

cat("\nStep 3: Extracting climate values from GEE...\n")

# Get variable names (PRISM values are already in correct units, no scaling needed)
variables <- names(prism_config$variables)

# PRISM data availability: 1981-present, but we only need IDS years
years <- time_config$start_year:time_config$end_year

cat(sprintf("  Variables: %s\n", paste(variables, collapse = ", ")))
cat(sprintf("  Years: %d-%d\n", min(years), max(years)))
cat(sprintf("  Resolution: 800m\n"))
cat(sprintf("  Temporal resolution: monthly\n\n"))

cat("  Note: PRISM extraction may be slow due to high pixel count.\n")
cat("  Consider running overnight or in batches.\n\n")

extract_climate_from_gee(
  pixel_coords = pixel_coords,
  gee_asset = prism_config$gee_asset,
  variables = variables,
  years = years,
  ee = ee,
  scale = prism_config$gee_scale,
  batch_size = 5000,  # Smaller batches for GEE stability
  output_dir = output_dir,
  output_prefix = prism_config$output_prefix,
  scale_factors = NULL,  # PRISM values are already scaled
  monthly = TRUE
)

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n================================\n")
cat("Extraction complete!\n\n")

output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
cat(sprintf("Output files: %d parquet files\n", length(output_files)))
cat(sprintf("Output directory: %s\n", output_dir))

if (length(output_files) > 0) {
  sample_file <- output_files[1]
  sample_data <- read_parquet(sample_file)
  cat(sprintf("\nSample from %s:\n", basename(sample_file)))
  cat(sprintf("  Rows: %d\n", nrow(sample_data)))
  cat(sprintf("  Columns: %s\n", paste(names(sample_data), collapse = ", ")))
}
