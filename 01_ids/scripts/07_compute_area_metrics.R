# ==============================================================================
# 01_ids/scripts/07_compute_area_metrics.R
# Compute area metrics for damage areas and their surveyed areas
#
# All areas computed in EPSG:5070 (Conus Albers Equal Area).
# Requires: 06_assign_surveyed_areas.R output
#
# Output: processed/ids/damage_area_area_metrics.parquet
#   DAMAGE_AREA_ID, damage_area_m2, SURVEYED_AREA_ID, survey_area_m2,
#   damage_frac_of_survey
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))

config <- load_config()
ids_config <- config$processed$ids

# Paths
ids_path <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
assignment_file <- here("processed/ids/damage_area_to_surveyed_area.parquet")
output_dir <- here("processed/ids")
output_file <- file.path(output_dir, "damage_area_area_metrics.parquet")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("Damage Area / Surveyed Area Metrics\n")
cat("====================================\n\n")

if (file.exists(output_file)) {
  cat("Output already exists:", output_file, "\n")
  cat("Delete to rerun.\n")
  quit(save = "no")
}

if (!file.exists(assignment_file)) {
  stop("Run 06_assign_surveyed_areas.R first. Missing: ", assignment_file)
}

# ------------------------------------------------------------------------------
# Step 1: Load data
# ------------------------------------------------------------------------------

cat("Step 1: Loading data...\n")

assignments <- read_parquet(assignment_file)
cat(sprintf("  Assignments: %d rows\n", nrow(assignments)))

# Load damage areas (unique geometries only)
damage_areas <- st_read(ids_path, layer = "damage_areas", quiet = TRUE)

damage_unique <- damage_areas %>%
  group_by(DAMAGE_AREA_ID) %>%
  slice(1) %>%
  ungroup() %>%
  select(DAMAGE_AREA_ID)

cat(sprintf("  Unique damage geometries: %d\n", nrow(damage_unique)))
rm(damage_areas)
gc()

# Load surveyed areas
surveyed_areas <- st_read(ids_path, layer = "surveyed_areas", quiet = TRUE)
cat(sprintf("  Surveyed areas: %d features\n", nrow(surveyed_areas)))

# ------------------------------------------------------------------------------
# Step 2: Compute areas in EPSG:5070
# ------------------------------------------------------------------------------

cat("\nStep 2: Computing areas in EPSG:5070...\n")

damage_unique <- st_transform(damage_unique, 5070)
surveyed_areas <- st_transform(surveyed_areas, 5070)

# Damage area in m2
damage_unique$damage_area_m2 <- as.numeric(st_area(damage_unique))

damage_areas_df <- damage_unique %>%
  st_drop_geometry() %>%
  select(DAMAGE_AREA_ID, damage_area_m2)

cat(sprintf("  Damage areas computed: median = %.0f m2, range = [%.0f, %.0f]\n",
            median(damage_areas_df$damage_area_m2),
            min(damage_areas_df$damage_area_m2),
            max(damage_areas_df$damage_area_m2)))

# Survey area in m2
surveyed_areas$survey_area_m2 <- as.numeric(st_area(surveyed_areas))

survey_areas_df <- surveyed_areas %>%
  st_drop_geometry() %>%
  select(SURVEYED_AREA_ID = SURVEY_FEATURE_ID, survey_area_m2)

cat(sprintf("  Survey areas computed: median = %.0f m2, range = [%.0f, %.0f]\n",
            median(survey_areas_df$survey_area_m2),
            min(survey_areas_df$survey_area_m2),
            max(survey_areas_df$survey_area_m2)))

rm(damage_unique, surveyed_areas)
gc()

# ------------------------------------------------------------------------------
# Step 3: Join and compute fraction
# ------------------------------------------------------------------------------

cat("\nStep 3: Computing damage fraction of survey...\n")

metrics <- assignments %>%
  select(DAMAGE_AREA_ID, SURVEYED_AREA_ID) %>%
  left_join(damage_areas_df, by = "DAMAGE_AREA_ID") %>%
  left_join(survey_areas_df, by = "SURVEYED_AREA_ID")

# Compute fraction (only where survey area is available and > 0)
metrics$damage_frac_of_survey <- ifelse(
  !is.na(metrics$survey_area_m2) & metrics$survey_area_m2 > 0,
  metrics$damage_area_m2 / metrics$survey_area_m2,
  NA_real_
)

cat(sprintf("  Rows with valid fraction: %d / %d\n",
            sum(!is.na(metrics$damage_frac_of_survey)), nrow(metrics)))

frac_valid <- metrics$damage_frac_of_survey[!is.na(metrics$damage_frac_of_survey)]
if (length(frac_valid) > 0) {
  cat(sprintf("  Fraction stats: median = %.4f, mean = %.4f, max = %.4f\n",
              median(frac_valid), mean(frac_valid), max(frac_valid)))
}

# ------------------------------------------------------------------------------
# Step 4: Save output
# ------------------------------------------------------------------------------

cat("\nStep 4: Saving output...\n")

write_parquet(metrics, output_file)

cat(sprintf("  Saved %d rows to %s\n", nrow(metrics), output_file))
cat(sprintf("  Columns: %s\n", paste(names(metrics), collapse = ", ")))

cat("\nDone.\n")
