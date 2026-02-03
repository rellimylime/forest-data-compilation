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
ids_path <- here("01_ids/data/processed/ids_damage_areas_cleaned.gpkg")
output_dir <- here("02_terraclimate/data/processed")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

output_file <- file.path(output_dir, "ids_terraclimate_merged.gpkg")

# ==============================================================================
# LOAD AND MERGE TERRACLIMATE CSVs
# ==============================================================================

cat("[1] Loading TerraClimate CSVs...\n")

tc_files <- list.files(tc_dir, pattern = "^tc_r\\d+_\\d{4}\\.csv$", full.names = TRUE)

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

cat("  ✓ Scale factors applied\n\n")

# ==============================================================================
# LOAD IDS DATA
# ==============================================================================

cat("[3] Loading IDS data...\n")

ids_data <- st_read(ids_path, quiet = TRUE)

cat(glue("  IDS rows: {format(nrow(ids_data), big.mark=',')}\n"))
cat(glue("  IDS columns: {ncol(ids_data)}\n\n"))

# ==============================================================================
# MERGE
# ==============================================================================

cat("[4] Merging IDS with TerraClimate...\n")

merged <- ids_data %>%
  left_join(tc_data, by = "OBSERVATION_ID")

cat(glue("  Merged rows: {format(nrow(merged), big.mark=',')}\n"))
cat(glue("  Merged columns: {ncol(merged)}\n"))

n_missing <- sum(is.na(merged$tmmx))
cat(glue("  Observations without climate data: {format(n_missing, big.mark=',')}\n\n"))

# ==============================================================================
# SAVE
# ==============================================================================

cat("[5] Saving merged data...\n")

st_write(merged, output_file, delete_dsn = TRUE, quiet = TRUE)

file_size_mb <- file.size(output_file) / 1024^2
cat(glue("  ✓ Saved: {output_file}\n"))
cat(glue("  File size: {round(file_size_mb, 1)} MB\n\n"))

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("================================================================================\n")
cat("MERGE COMPLETE\n")
cat("================================================================================\n\n")
cat(glue("Output: {output_file}\n"))
cat(glue("Total observations: {format(nrow(merged), big.mark=',')}\n"))
cat(glue("Climate variables: {length(names(tc_vars))}\n"))
cat("================================================================================\n")
