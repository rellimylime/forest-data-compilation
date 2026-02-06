# ==============================================================================
# scripts/03_reshape_pixel_values.R
# Reshape per-dataset wide pixel values into standardized long format
#
# Reads the existing yearly parquet files (pixel_id, x, y, year, month, var1, var2, ...)
# and writes long-format output keyed by:
#   pixel_id, calendar_year, calendar_month, variable, value
# with water_year and water_year_month appended.
#
# Filters to only pixels present in pixel maps (drops any orphan pixels).
#
# Output: processed/climate/<dataset>/pixel_values.parquet
#   (single file per dataset; partitioning optional via write_dataset)
#
# Reusable for: terraclimate, prism, worldclim, era5
# ==============================================================================

library(here)
library(yaml)
library(dplyr)
library(tidyr)
library(arrow)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/time_utils.R"))

config <- load_config()
time_config <- config$params$time_range

# ==============================================================================
# Configuration: which dataset to process
# Set via command-line arg or edit here
# ==============================================================================

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1) args[1] else "terraclimate"

# Dataset-specific settings
dataset_configs <- list(
  terraclimate = list(
    source_dir = here("02_terraclimate/data/processed/pixel_values"),
    source_prefix = "terraclimate",
    pixel_map_dir = here("02_terraclimate/data/processed/pixel_maps"),
    variables = names(config$raw$terraclimate$variables),
    time_cols = c("year", "month")
  ),
  prism = list(
    source_dir = here("03_prism/data/processed/pixel_values"),
    source_prefix = "prism",
    pixel_map_dir = here("03_prism/data/processed/pixel_maps"),
    variables = names(config$raw$prism$variables),
    time_cols = c("year", "month")
  ),
  worldclim = list(
    source_dir = here("04_worldclim/data/processed/pixel_values"),
    source_prefix = "worldclim",
    pixel_map_dir = here("04_worldclim/data/processed/pixel_maps"),
    variables = names(config$raw$worldclim$variables),
    time_cols = c("year", "month")
  ),
  era5 = list(
    source_dir = here("05_era5/data/processed/pixel_values"),
    source_prefix = "era5",
    pixel_map_dir = here("05_era5/data/processed/pixel_maps"),
    variables = names(config$raw$era5$variables),
    time_cols = c("year", "month", "day")
  )
)

if (!dataset %in% names(dataset_configs)) {
  stop("Unknown dataset: ", dataset,
       ". Choose from: ", paste(names(dataset_configs), collapse = ", "))
}

ds <- dataset_configs[[dataset]]
output_dir <- here("processed/climate", dataset)
output_file <- file.path(output_dir, "pixel_values.parquet")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat(sprintf("Reshape Pixel Values: %s\n", toupper(dataset)))
cat("========================================\n\n")

if (file.exists(output_file)) {
  cat("Output already exists:", output_file, "\n")
  cat("Delete to rerun.\n")
  quit(save = "no")
}

# ------------------------------------------------------------------------------
# Step 1: Get set of valid pixel_ids from pixel maps
# ------------------------------------------------------------------------------

cat("Step 1: Loading pixel maps to identify valid pixels...\n")

pm_files <- list.files(ds$pixel_map_dir, pattern = "\\.parquet$", full.names = TRUE)
if (length(pm_files) == 0) {
  stop("No pixel map files found in ", ds$pixel_map_dir)
}

valid_pixels <- unique(unlist(lapply(pm_files, function(f) {
  read_parquet(f)$pixel_id
})))

cat(sprintf("  Valid pixels from pixel maps: %d\n", length(valid_pixels)))

# ------------------------------------------------------------------------------
# Step 2: Load and reshape yearly files
# ------------------------------------------------------------------------------

cat("\nStep 2: Loading and reshaping yearly pixel value files...\n")

source_files <- list.files(ds$source_dir,
                           pattern = paste0("^", ds$source_prefix, "_\\d{4}\\.parquet$"),
                           full.names = TRUE)

if (length(source_files) == 0) {
  stop("No pixel value files found in ", ds$source_dir)
}

cat(sprintf("  Found %d yearly files\n", length(source_files)))

all_long <- list()

for (f in source_files) {
  yr_data <- read_parquet(f)

  # Filter to valid pixels
  yr_data <- yr_data[yr_data$pixel_id %in% valid_pixels, ]

  if (nrow(yr_data) == 0) next

  # Identify variable columns (everything except pixel_id, x, y, time cols)
  non_var_cols <- c("pixel_id", "x", "y", ds$time_cols)
  var_cols <- setdiff(names(yr_data), non_var_cols)
  var_cols <- intersect(var_cols, ds$variables)  # Only configured variables

  if (length(var_cols) == 0) next

  # Pivot to long format
  long <- yr_data %>%
    pivot_longer(
      cols = all_of(var_cols),
      names_to = "variable",
      values_to = "value"
    )

  # Standardize column names
  if ("year" %in% names(long)) {
    long <- long %>% rename(calendar_year = year, calendar_month = month)
  }

  all_long[[basename(f)]] <- long
  cat(".")
}

cat("\n")

if (length(all_long) == 0) {
  stop("No data after filtering. Check pixel maps and source files.")
}

pixel_values_long <- bind_rows(all_long)

cat(sprintf("  Combined: %d rows\n", nrow(pixel_values_long)))

# ------------------------------------------------------------------------------
# Step 3: Add water year columns
# ------------------------------------------------------------------------------

cat("\nStep 3: Adding water year columns...\n")

pixel_values_long <- add_water_year(
  pixel_values_long %>% rename(year = calendar_year, month = calendar_month)
) %>%
  rename(calendar_year = year, calendar_month = month)

cat("  water_year and water_year_month added\n")

# ------------------------------------------------------------------------------
# Step 4: Select final columns and save
# ------------------------------------------------------------------------------

cat("\nStep 4: Saving output...\n")

# Final column order
keep_cols <- c("pixel_id", "calendar_year", "calendar_month",
               "water_year", "water_year_month", "variable", "value")

# Add day column for ERA5
if ("day" %in% names(pixel_values_long)) {
  keep_cols <- c("pixel_id", "calendar_year", "calendar_month", "day",
                 "water_year", "water_year_month", "variable", "value")
}

pixel_values_long <- pixel_values_long %>%
  select(all_of(keep_cols))

write_parquet(pixel_values_long, output_file)

cat(sprintf("  Saved %d rows to %s\n", nrow(pixel_values_long), output_file))
cat(sprintf("  Columns: %s\n", paste(names(pixel_values_long), collapse = ", ")))
cat(sprintf("  Variables: %s\n", paste(unique(pixel_values_long$variable), collapse = ", ")))
cat(sprintf("  Year range: %d-%d\n",
            min(pixel_values_long$calendar_year),
            max(pixel_values_long$calendar_year)))

cat("\nDone.\n")
