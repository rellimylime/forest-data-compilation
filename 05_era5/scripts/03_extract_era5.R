# ==============================================================================
# 05_era5/scripts/03_extract_era5.R
# Extract ERA5 daily pixel values from local NetCDF files
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
era5_config <- config$raw$era5
time_config <- config$params$time_range

# Paths
raw_dir <- here(era5_config$local_dir)
pixel_map_dir <- here(era5_config$output_dir, "pixel_maps")
output_dir <- here(era5_config$output_dir, "pixel_values")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("ERA5 Extraction (Local NetCDF, Daily)\n")
cat("=====================================\n\n")

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
# Step 2: Prepare extraction parameters
# ------------------------------------------------------------------------------

cat("\nStep 2: Preparing extraction...\n")

variables <- names(era5_config$variables)
years <- time_config$start_year:time_config$end_year

# Build scale factors and kelvin conversion lists
scale_factors <- list()
kelvin_vars <- character()

for (var_name in variables) {
  var_config <- era5_config$variables[[var_name]]
  scale_factors[[var_name]] <- var_config$scale

  if (isTRUE(var_config$convert_kelvin)) {
    kelvin_vars <- c(kelvin_vars, var_name)
  }
}

cat(sprintf("  Variables: %d\n", length(variables)))
cat(sprintf("  Years: %d-%d\n", min(years), max(years)))
cat(sprintf("  Temporal resolution: daily (~365 time steps/year)\n"))
cat(sprintf("  Kelvin -> Celsius conversion: %s\n", paste(kelvin_vars, collapse = ", ")))

# ------------------------------------------------------------------------------
# Step 3: Extract values year by year
# ------------------------------------------------------------------------------

cat("\nStep 3: Extracting climate values...\n")
cat("  Note: Daily extraction produces large files. Ensure sufficient disk space.\n\n")

for (year in years) {
  output_file <- file.path(output_dir, sprintf("%s_%d.parquet", era5_config$output_prefix, year))

  if (file.exists(output_file)) {
    cat(sprintf("  %d: exists, skipping\n", year))
    next
  }

  cat(sprintf("  %d: ", year))

  # Determine dates for this year
  start_date <- as.Date(sprintf("%d-01-01", year))
  end_date <- as.Date(sprintf("%d-12-31", year))
  dates <- seq(start_date, end_date, by = "day")
  n_days <- length(dates)

  # Initialize list to hold daily data
  daily_results <- vector("list", n_days)

  for (day_idx in seq_len(n_days)) {
    current_date <- dates[day_idx]
    day_data <- data.frame(
      pixel_id = pixel_coords$pixel_id,
      x = pixel_coords$x,
      y = pixel_coords$y,
      year = year,
      month = as.integer(format(current_date, "%m")),
      day = as.integer(format(current_date, "%d"))
    )

    for (var_name in variables) {
      nc_file <- file.path(raw_dir, var_name, sprintf("%s_%d.nc", var_name, year))

      if (!file.exists(nc_file)) {
        day_data[[var_name]] <- NA
        next
      }

      # Load raster (lazy loading)
      r <- rast(nc_file)

      # ERA5 NetCDFs have one band per day
      if (day_idx > nlyr(r)) {
        day_data[[var_name]] <- NA
        next
      }

      values <- extract(r[[day_idx]], coords_matrix)
      raw_values <- values[, 1]

      # Apply scale factor
      if (var_name %in% names(scale_factors)) {
        raw_values <- raw_values * scale_factors[[var_name]]
      }

      # Convert Kelvin to Celsius
      if (var_name %in% kelvin_vars) {
        raw_values <- raw_values - 273.15
      }

      day_data[[var_name]] <- raw_values
    }

    daily_results[[day_idx]] <- day_data

    # Progress indicator
    if (day_idx %% 30 == 0) cat(".")
  }

  year_data <- bind_rows(daily_results)
  write_parquet(year_data, output_file)
  cat(sprintf(" saved %d rows\n", nrow(year_data)))
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n=====================================\n")
cat("Extraction complete!\n\n")

output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
cat(sprintf("Output files: %d parquet files\n", length(output_files)))
cat(sprintf("Output directory: %s\n", output_dir))

if (length(output_files) > 0) {
  sample_file <- output_files[1]
  sample_data <- read_parquet(sample_file)
  cat(sprintf("\nSample from %s:\n", basename(sample_file)))
  cat(sprintf("  Rows: %d (pixels x days)\n", nrow(sample_data)))
  cat(sprintf("  Columns: %s\n", paste(names(sample_data), collapse = ", ")))

  # Show date range
  cat(sprintf("  Date range: %d-%02d-%02d to %d-%02d-%02d\n",
              min(sample_data$year), min(sample_data$month), min(sample_data$day),
              max(sample_data$year), max(sample_data$month), max(sample_data$day)))
}
