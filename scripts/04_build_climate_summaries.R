# ==============================================================================
# scripts/04_build_climate_summaries.R
# Compute observation-level climate summaries (area-weighted means)
#
# Joins pixel maps to long-format pixel values, then computes:
#   weighted_mean = sum(value * coverage_fraction) / sum(coverage_fraction)
# per DAMAGE_AREA_ID (or OBSERVATION_ID) per variable per time step.
#
# Output: processed/climate/<dataset>/damage_areas_summaries_long.parquet
#   Long format with calendar_year/month AND water_year/month retained.
#   Diagnostics: n_pixels, sum_coverage_fraction
#
# Reusable for: terraclimate, prism, worldclim, era5
# ==============================================================================

library(here)
library(yaml)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/time_utils.R"))

config <- load_config()

# ==============================================================================
# Configuration: which dataset to process
# ==============================================================================

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1) args[1] else "terraclimate"

# Dataset-specific pixel map paths
pixel_map_paths <- list(
  terraclimate = here("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
  prism = here("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
  worldclim = here("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
  era5 = here("05_era5/data/processed/pixel_maps/damage_areas_pixel_map.parquet")
)

if (!dataset %in% names(pixel_map_paths)) {
  stop("Unknown dataset: ", dataset,
       ". Choose from: ", paste(names(pixel_map_paths), collapse = ", "))
}

pixel_map_file <- pixel_map_paths[[dataset]]
pixel_values_file <- here("processed/climate", dataset, "pixel_values.parquet")
output_dir <- here("processed/climate", dataset)
output_file <- file.path(output_dir, "damage_areas_summaries_long.parquet")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat(sprintf("Climate Summaries for Damage Areas: %s\n", toupper(dataset)))
cat("========================================\n\n")

if (file.exists(output_file)) {
  cat("Output already exists:", output_file, "\n")
  cat("Delete to rerun.\n")
  quit(save = "no")
}

# ------------------------------------------------------------------------------
# Step 1: Load pixel map
# ------------------------------------------------------------------------------

cat("Step 1: Loading pixel map...\n")

if (!file.exists(pixel_map_file)) {
  stop("Pixel map not found: ", pixel_map_file,
       "\nRun the build_pixel_maps script for ", dataset, " first.")
}

pixel_map <- read_parquet(pixel_map_file)

# Determine ID columns present
has_obs_id <- "OBSERVATION_ID" %in% names(pixel_map)
has_da_id <- "DAMAGE_AREA_ID" %in% names(pixel_map)

if (has_da_id) {
  group_id <- "DAMAGE_AREA_ID"
} else if (has_obs_id) {
  group_id <- "OBSERVATION_ID"
} else {
  stop("Pixel map must contain DAMAGE_AREA_ID or OBSERVATION_ID")
}

cat(sprintf("  Pixel map: %d rows, grouping by %s\n", nrow(pixel_map), group_id))
cat(sprintf("  Unique %s: %d\n", group_id, length(unique(pixel_map[[group_id]]))))
cat(sprintf("  Unique pixels: %d\n", length(unique(pixel_map$pixel_id))))

# Keep only needed columns from pixel map
pm_cols <- c(group_id, "pixel_id", "coverage_fraction")
if (has_obs_id && group_id == "DAMAGE_AREA_ID") {
  pm_cols <- c("OBSERVATION_ID", pm_cols)
}
pixel_map <- pixel_map[, intersect(pm_cols, names(pixel_map))]

# ------------------------------------------------------------------------------
# Step 2: Load pixel values (long format)
# ------------------------------------------------------------------------------

cat("\nStep 2: Loading pixel values...\n")

if (!file.exists(pixel_values_file)) {
  stop("Pixel values not found: ", pixel_values_file,
       "\nRun scripts/03_reshape_pixel_values.R for ", dataset, " first.")
}

pixel_values <- read_parquet(pixel_values_file)
cat(sprintf("  Pixel values: %d rows, %d variables\n",
            nrow(pixel_values), length(unique(pixel_values$variable))))

# Identify time columns in pixel values
time_cols <- intersect(
  c("calendar_year", "calendar_month", "day", "water_year", "water_year_month"),
  names(pixel_values)
)

cat(sprintf("  Time columns: %s\n", paste(time_cols, collapse = ", ")))

# ------------------------------------------------------------------------------
# Step 3: Join and compute weighted means (chunked by variable)
# ------------------------------------------------------------------------------

cat("\nStep 3: Computing area-weighted means...\n")

variables <- unique(pixel_values$variable)
cat(sprintf("  Variables to process: %s\n", paste(variables, collapse = ", ")))

# Process one variable at a time to manage memory
results <- list()

for (var in variables) {
  cat(sprintf("  %s: ", var))

  var_values <- pixel_values[pixel_values$variable == var, ]

  # Join pixel map to pixel values
  joined <- pixel_map %>%
    inner_join(var_values, by = "pixel_id")

  if (nrow(joined) == 0) {
    cat("no matches, skipping\n")
    next
  }

  # Group by observation ID + time + variable
  group_cols <- c(group_id, time_cols, "variable")
  if ("OBSERVATION_ID" %in% names(joined) && group_id != "OBSERVATION_ID") {
    group_cols <- c("OBSERVATION_ID", group_cols)
  }

  # Compute weighted mean
  summary <- joined %>%
    group_by(across(all_of(group_cols))) %>%
    summarize(
      weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                      sum(coverage_fraction[!is.na(value)]),
      n_pixels = n(),
      n_pixels_with_data = sum(!is.na(value)),
      sum_coverage_fraction = sum(coverage_fraction),
      .groups = "drop"
    )

  results[[var]] <- summary
  cat(sprintf("%d summaries\n", nrow(summary)))
}

# Combine all variables
summaries <- bind_rows(results)

cat(sprintf("\n  Total summary rows: %d\n", nrow(summaries)))

# ------------------------------------------------------------------------------
# Step 4: Save output
# ------------------------------------------------------------------------------

cat("\nStep 4: Saving output...\n")

write_parquet(summaries, output_file)

cat(sprintf("  Saved %d rows to %s\n", nrow(summaries), output_file))
cat(sprintf("  Columns: %s\n", paste(names(summaries), collapse = ", ")))

# Summary stats
cat(sprintf("\n  Unique %s: %d\n", group_id, length(unique(summaries[[group_id]]))))
cat(sprintf("  Variables: %s\n", paste(unique(summaries$variable), collapse = ", ")))
cat(sprintf("  Year range: %d-%d\n",
            min(summaries$calendar_year), max(summaries$calendar_year)))
cat(sprintf("  Median n_pixels per summary: %.0f\n", median(summaries$n_pixels)))

cat("\nDone.\n")
