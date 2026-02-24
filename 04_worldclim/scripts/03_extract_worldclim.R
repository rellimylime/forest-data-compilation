# ==============================================================================
# 04_worldclim/scripts/03_extract_worldclim.R
# Extract WorldClim pixel values from local GeoTIFF files
#
# WorldClim v2.1 (CRU TS 4.09) stores one GeoTIFF per month, named:
#   wc2.1_cruts4.09_2.5m_{var}_{YYYY}-{MM}.tif
# Each file is a single-band global raster at ~4.5km resolution.
# ==============================================================================

library(here)
library(yaml)
library(terra)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
wc_config  <- config$raw$worldclim
time_config <- config$params$time_range

# Paths
raw_dir        <- here(wc_config$local_dir)
pixel_map_dir  <- here(wc_config$output_dir, "pixel_maps")
output_dir     <- here(wc_config$output_dir, "pixel_values")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("WorldClim Extraction (Local GeoTIFF)\n")
cat("====================================\n\n")

# WorldClim coverage: 1950-2024; IDS coverage: 1997-2024; overlap: 1997-2024
wc_start_year  <- 1950
wc_end_year    <- 2024
ids_start_year <- time_config$start_year
ids_end_year   <- time_config$end_year

years <- max(ids_start_year, wc_start_year):min(ids_end_year, wc_end_year)
cat(sprintf("WorldClim coverage: %d-%d\n", wc_start_year, wc_end_year))
cat(sprintf("IDS coverage      : %d-%d\n", ids_start_year, ids_end_year))
cat(sprintf("Extracting years  : %d-%d\n\n", min(years), max(years)))

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
  }
}

pixel_coords  <- bind_rows(all_pixels) %>% distinct(pixel_id, x, y)
coords_matrix <- as.matrix(pixel_coords[, c("x", "y")])
cat(sprintf("\nTotal unique pixels: %d\n", nrow(pixel_coords)))

# ------------------------------------------------------------------------------
# Step 2: Extract values year by year
# ------------------------------------------------------------------------------

cat("\nStep 2: Extracting climate values...\n")

variables <- names(wc_config$variables)

for (year in years) {
  output_file <- file.path(
    output_dir, sprintf("%s_%d.parquet", wc_config$output_prefix, year))

  if (file.exists(output_file)) {
    cat(sprintf("  %d: exists, skipping\n", year))
    next
  }

  cat(sprintf("  %d: ", year))

  year_results <- list()

  for (month in 1:12) {
    month_data <- data.frame(
      pixel_id = pixel_coords$pixel_id,
      x        = pixel_coords$x,
      y        = pixel_coords$y,
      year     = year,
      month    = month
    )

    for (var in variables) {
      tif_file <- file.path(raw_dir, var, sprintf(
        "wc2.1_cruts4.09_2.5m_%s_%04d-%02d.tif", var, year, month))

      if (!file.exists(tif_file)) {
        cat(sprintf("missing %s/%04d-%02d ", var, year, month))
        month_data[[var]] <- NA_real_
        next
      }

      r <- rast(tif_file)
      vals <- terra::extract(r, coords_matrix)
      month_data[[var]] <- vals[, 1]
    }

    year_results[[month]] <- month_data
    cat(".")
  }

  year_data <- bind_rows(year_results)
  write_parquet(year_data, output_file)
  cat(sprintf(" saved %d rows\n", nrow(year_data)))
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n====================================\n")
cat("Extraction complete!\n\n")

output_files <- list.files(output_dir, pattern = "\\.parquet$",
                           full.names = TRUE)
cat(sprintf("Output files: %d parquet files\n", length(output_files)))
cat(sprintf("Output directory: %s\n", output_dir))

if (length(output_files) > 0) {
  sample_data <- read_parquet(output_files[1])
  cat(sprintf("\nSample from %s:\n", basename(output_files[1])))
  cat(sprintf("  Rows   : %d\n", nrow(sample_data)))
  cat(sprintf("  Columns: %s\n", paste(names(sample_data), collapse = ", ")))
}
