# ==============================================================================
# 04_worldclim/scripts/03_extract_worldclim.R
# Extract WorldClim pixel values from local GeoTIFF files
# ==============================================================================

library(here)
library(yaml)
library(terra)
library(dplyr)
library(arrow)
library(purrr)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
wc_config <- config$raw$worldclim
time_config <- config$params$time_range

# Paths
raw_dir <- here(wc_config$local_dir)
pixel_map_dir <- here(wc_config$output_dir, "pixel_maps")
output_dir <- here(wc_config$output_dir, "pixel_values")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("WorldClim Extraction (Local GeoTIFF)\n")
cat("====================================\n\n")

# WorldClim coverage: 1960-2021
# IDS coverage: 1997-2024
# Overlap: 1997-2021
wc_end_year <- 2021
ids_start_year <- time_config$start_year
ids_end_year <- time_config$end_year

years <- ids_start_year:min(ids_end_year, wc_end_year)
cat(sprintf("WorldClim coverage: 1960-%d\n", wc_end_year))
cat(sprintf("IDS coverage: %d-%d\n", ids_start_year, ids_end_year))
cat(sprintf("Extracting years: %d-%d\n\n", min(years), max(years)))

if (ids_end_year > wc_end_year) {
  cat(sprintf("Note: Years %d-%d have no WorldClim data\n\n", wc_end_year + 1, ids_end_year))
}

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

pixel_coords <- bind_rows(all_pixels) %>%
  distinct(pixel_id, x, y)

coords_matrix <- as.matrix(pixel_coords[, c("x", "y")])
cat(sprintf("\nTotal unique pixels: %d\n", nrow(pixel_coords)))

# ------------------------------------------------------------------------------
# Step 2: Build decade-to-file mapping
# ------------------------------------------------------------------------------

cat("\nStep 2: Building file mapping...\n")

variables <- names(wc_config$variables)

# WorldClim file naming: wc2.1_2.5m_{var}_{decade_start}_{decade_end}.tif
# Each file has 12 bands per year in that decade

get_decade_for_year <- function(year) {
  decades <- wc_config$decades
  for (decade in decades) {
    years_range <- as.numeric(strsplit(decade, "-")[[1]])
    if (year >= years_range[1] && year <= years_range[2]) {
      return(decade)
    }
  }
  return(NULL)
}

get_band_index <- function(year, month, decade) {
  years_range <- as.numeric(strsplit(decade, "-")[[1]])
  year_offset <- year - years_range[1]
  band_idx <- year_offset * 12 + month
  return(band_idx)
}

# ------------------------------------------------------------------------------
# Step 3: Extract values year by year
# ------------------------------------------------------------------------------

cat("\nStep 3: Extracting climate values...\n")

for (year in years) {
  output_file <- file.path(output_dir, sprintf("%s_%d.parquet", wc_config$output_prefix, year))

  if (file.exists(output_file)) {
    cat(sprintf("  %d: exists, skipping\n", year))
    next
  }

  cat(sprintf("  %d: ", year))

  decade <- get_decade_for_year(year)
  if (is.null(decade)) {
    cat("no decade mapping, skipping\n")
    next
  }

  year_results <- list()

  for (month in 1:12) {
    month_data <- data.frame(
      pixel_id = pixel_coords$pixel_id,
      x = pixel_coords$x,
      y = pixel_coords$y,
      year = year,
      month = month
    )

    for (var in variables) {
      # Find the file for this variable and decade
      var_dir <- file.path(raw_dir, var)
      decade_clean <- gsub("-", "_", decade)
      tif_pattern <- sprintf("wc2.1_2.5m_%s_%s.*\\.tif$", var, decade_clean)
      tif_files <- list.files(var_dir, pattern = tif_pattern, full.names = TRUE)

      if (length(tif_files) == 0) {
        cat(sprintf("missing %s/%s ", var, decade))
        month_data[[var]] <- NA
        next
      }

      r <- rast(tif_files[1])
      band_idx <- get_band_index(year, month, decade)

      if (band_idx > nlyr(r)) {
        month_data[[var]] <- NA
        next
      }

      values <- extract(r[[band_idx]], coords_matrix)
      month_data[[var]] <- values[, 1]
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
