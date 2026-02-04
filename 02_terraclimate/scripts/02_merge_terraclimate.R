# ==============================================================================
# 02_merge_terraclimate.R
# Merge all TerraClimate CSVs and join with IDS data
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(readr)
library(glue)
library(purrr)

source(here("scripts/utils/load_config.R"))

# ==============================================================================
# SETUP
# ==============================================================================

cat("=== MERGE TERRACLIMATE WITH IDS ===\n\n")

config <- load_config()

tc_dir <- here("02_terraclimate/data/raw")
ids_path <- here("01_ids/data/processed/ids_layers_cleaned.gpkg")
output_dir <- here("02_terraclimate/data/processed")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

output_file <- file.path(output_dir, "ids_terraclimate_merged.gpkg")

# ==============================================================================
# LOAD AND MERGE TERRACLIMATE CSVs
# ==============================================================================

cat("[1] Loading TerraClimate CSVs...\n")

tc_files <- list.files(tc_dir, pattern = "^tc_damage_areas_r\\d+_\\d{4}\\.csv$", full.names = TRUE)

if (length(tc_files) == 0) {
  stop("No TerraClimate CSV files found. Run 01_extract_terraclimate.R first.")
}

cat(glue("  Found {length(tc_files)} files\n"))

tc_data <- map_dfr(tc_files, read_csv, col_types = cols(.default = "d", OBSERVATION_ID = "c"))

cat(glue("  Total rows: {format(nrow(tc_data), big.mark=',')}\n"))
cat(glue("  Columns: {ncol(tc_data)}\n\n"))

# ==============================================================================
# APPLY SCALE FACTORS
# ==============================================================================

cat("[2] Applying scale factors...\n")

tc_vars <- config$raw$terraclimate$variables

for (var_name in names(tc_vars)) {
  if (var_name %in% names(tc_data)) {
    scale <- tc_vars[[var_name]]$scale
    tc_data[[var_name]] <- tc_data[[var_name]] * scale
  }
}

cat("  Done\n\n")

# ==============================================================================
# LOAD IDS DATA
# ==============================================================================

cat("[3] Loading IDS data...\n")

ids_data <- st_read(ids_path, layer = "damage_areas", quiet = TRUE)

cat(glue("  IDS rows: {format(nrow(ids_data), big.mark=',')}\n"))
cat(glue("  IDS columns: {ncol(ids_data)}\n\n"))

# ==============================================================================
# CLEAN TERRACLIMATE DATA
# ==============================================================================

cat("[4] Cleaning TerraClimate data...\n")

n_na_ids <- sum(is.na(tc_data$OBSERVATION_ID))
n_dupes <- nrow(tc_data) - n_distinct(tc_data$OBSERVATION_ID, na.rm = TRUE)

cat(glue("  Rows with NA OBSERVATION_ID: {n_na_ids}\n"))
cat(glue("  Duplicate OBSERVATION_IDs: {n_dupes}\n"))

tc_data_clean <- tc_data %>%
  filter(!is.na(OBSERVATION_ID)) %>%
  distinct(OBSERVATION_ID, .keep_all = TRUE)

cat(glue("  Rows after cleaning: {format(nrow(tc_data_clean), big.mark=',')}\n\n"))

# ==============================================================================
# MERGE
# ==============================================================================

cat("[5] Merging IDS with TerraClimate...\n")

merged <- ids_data %>%
  left_join(tc_data_clean %>% select(-REGION_ID, -SURVEY_YEAR), 
            by = "OBSERVATION_ID")

cat(glue("  Merged rows: {format(nrow(merged), big.mark=',')}\n"))
cat(glue("  Merged columns: {ncol(merged)}\n\n"))

# ==============================================================================
# REPORT MISSING CLIMATE DATA
# ==============================================================================

cat("[6] Checking missing climate data...\n")

missing_climate <- merged %>% 
  st_drop_geometry() %>%
  filter(is.na(tmmx))

n_missing <- nrow(missing_climate)
cat(glue("  Observations without climate data: {format(n_missing, big.mark=',')} ({round(100*n_missing/nrow(merged), 3)}%)\n"))

if (n_missing > 0) {
  cat("\n  Missing by region:\n")
  print(table(missing_climate$REGION_ID))
}

cat("\n")

# ==============================================================================
# SAVE
# ==============================================================================

cat("[7] Saving merged data...\n")

st_write(merged, output_file, delete_dsn = TRUE, quiet = TRUE)

file_size_mb <- file.size(output_file) / 1024^2
cat(glue("  Saved: {output_file}\n"))
cat(glue("  File size: {round(file_size_mb, 1)} MB\n\n"))

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("================================================================================\n")
cat("MERGE COMPLETE\n")
cat("================================================================================\n\n")
cat(glue("Output: {output_file}\n"))
cat(glue("Total observations: {format(nrow(merged), big.mark=',')}\n"))
cat(glue("Missing climate data: {format(n_missing, big.mark=',')} ({round(100*n_missing/nrow(merged), 3)}%)\n"))
cat(glue("Climate variables: {length(names(tc_vars))}\n"))
cat("================================================================================\n")
