# ==============================================================================
# 05_era5/scripts/03_extract_era5.R
# Extract ERA5 monthly pixel values from local NetCDF files
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

cat("ERA5 Extraction (Local NetCDF, Monthly)\n")
cat("========================================\n\n")

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

pixel_coords <- bind_rows(all_pixels) |>
  distinct(pixel_id, x, y)

coords_matrix <- as.matrix(pixel_coords[, c("x", "y")])
n_pixels <- nrow(pixel_coords)
cat(sprintf("\nTotal unique pixels: %d\n", n_pixels))

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
cat(sprintf("  Temporal resolution: monthly (12 time steps/year)\n"))
cat(sprintf("  Kelvin -> Celsius: %s\n", paste(kelvin_vars, collapse = ", ")))

# ------------------------------------------------------------------------------
# Step 3: Extract values year by year
# ------------------------------------------------------------------------------

cat("\nStep 3: Extracting climate values...\n\n")

for (year in years) {
  output_file <- file.path(
    output_dir, sprintf("%s_%d.parquet", era5_config$output_prefix, year)
  )

  if (file.exists(output_file)) {
    existing_cols <- open_dataset(output_file)$schema$names
    missing_vars <- setdiff(variables, existing_cols)
    if (length(missing_vars) == 0) {
      cat(sprintf("  %d: exists, skipping\n", year))
      next
    } else {
      cat(sprintf("  %d: missing columns [%s], re-extracting\n",
                  year, paste(missing_vars, collapse = ", ")))
    }
  }

  cat(sprintf("  %d:", year))

  # Build skeleton: n_pixels * 12 rows, months cycle slowly
  year_data <- data.frame(
    pixel_id = rep(pixel_coords$pixel_id, times = 12),
    x        = rep(pixel_coords$x,        times = 12),
    y        = rep(pixel_coords$y,        times = 12),
    year     = year,
    month    = rep(1:12, each = n_pixels)
  )

  for (var_name in variables) {
    nc_file <- file.path(raw_dir, var_name, sprintf("%s_%d.nc", var_name, year))

    if (!file.exists(nc_file)) {
      year_data[[var_name]] <- NA_real_
      next
    }

    r <- rast(nc_file)

    if (nlyr(r) < 12) {
      # Unexpected band count — fill NA and warn
      year_data[[var_name]] <- NA_real_
      cat(sprintf(" [%s:%dL]", var_name, nlyr(r)))
      next
    }

    # Extract all 12 months at once: returns n_pixels × 13 data.frame (ID + 12 bands)
    extracted     <- extract(r[[1:12]], coords_matrix)
    values_matrix <- as.matrix(extracted[, -1, drop = FALSE])  # n_pixels × 12

    # as.vector() is column-major: all pixels for month 1, then month 2, ...
    # This matches year_data row order: pixels repeat (times=12), month cycles (each=n_pixels)
    values_vec <- as.vector(values_matrix)

    # Apply scale factor and unit conversions
    if (var_name %in% names(scale_factors)) {
      values_vec <- values_vec * scale_factors[[var_name]]
    }
    if (var_name %in% kelvin_vars) {
      values_vec <- values_vec - 273.15
    }

    year_data[[var_name]] <- values_vec
    cat(".")
  }

  write_parquet(year_data, output_file)
  cat(sprintf(" saved %d rows\n", nrow(year_data)))
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n========================================\n")
cat("Extraction complete!\n\n")

output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
cat(sprintf("Output files: %d parquet files\n", length(output_files)))
cat(sprintf("Output directory: %s\n", output_dir))

if (length(output_files) > 0) {
  sample_file <- output_files[1]
  sample_data <- read_parquet(sample_file)
  cat(sprintf("\nSample from %s:\n", basename(sample_file)))
  cat(sprintf("  Rows: %d (pixels x months)\n", nrow(sample_data)))
  cat(sprintf("  Columns: %s\n", paste(names(sample_data), collapse = ", ")))
  cat(sprintf("  Months: %s\n", paste(sort(unique(sample_data$month)), collapse = ", ")))
}
