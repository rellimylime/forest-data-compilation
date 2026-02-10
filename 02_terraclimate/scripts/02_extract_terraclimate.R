# ==============================================================================
# 02_terraclimate/scripts/02_extract_terraclimate.R
# Extract TerraClimate pixel values from GEE for all unique pixels
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
tc_config <- config$raw$terraclimate
time_config <- config$params$time_range

# Paths
pixel_map_dir <- here(tc_config$output_dir, "pixel_maps")
output_dir <- here(tc_config$output_dir, "pixel_values")

cat("TerraClimate Extraction (GEE)\n")
cat("================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Load pixel maps and get unique pixels
# ------------------------------------------------------------------------------

cat("Step 1: Loading pixel maps...\n")

# Combine unique pixels from all layers
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

# Combine and deduplicate
pixel_coords <- bind_rows(all_pixels) %>%
  distinct(pixel_id, x, y)

cat(sprintf("\nTotal unique pixels across all layers: %d\n", nrow(pixel_coords)))

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

# Get variable names and scale factors
variables <- names(tc_config$variables)
scale_factors <- sapply(tc_config$variables, function(v) v$scale)

years <- time_config$start_year:time_config$end_year

cat(sprintf("  Variables: %s\n", paste(variables, collapse = ", ")))
cat(sprintf("  Years: %d-%d (%d years)\n", min(years), max(years), length(years)))
cat(sprintf("  Temporal resolution: monthly (stacked extraction)\n\n"))

t_start <- Sys.time()

extract_climate_from_gee(
  pixel_coords = pixel_coords,
  gee_asset = tc_config$gee_asset,
  variables = variables,
  years = years,
  ee = ee,
  scale = tc_config$gee_scale,
  batch_size = 2500,
  output_dir = output_dir,
  output_prefix = tc_config$output_prefix,
  scale_factors = scale_factors,
  monthly = TRUE
)

t_end <- Sys.time()

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n================================\n")
cat(sprintf("Extraction complete! Total time: %.1f hours\n\n",
            as.numeric(difftime(t_end, t_start, units = "hours"))))

output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
cat(sprintf("Output files: %d parquet files\n", length(output_files)))
cat(sprintf("Output directory: %s\n", output_dir))

# Show sample
if (length(output_files) > 0) {
  sample_file <- output_files[1]
  sample_data <- read_parquet(sample_file)
  cat(sprintf("\nSample from %s:\n", basename(sample_file)))
  cat(sprintf("  Rows: %d\n", nrow(sample_data)))
  cat(sprintf("  Columns: %s\n", paste(names(sample_data), collapse = ", ")))
}

cat("\nTo join with IDS observations, use join_to_observations() from climate_extract.R\n")
