# ==============================================================================
# 03_clean_ids.R
# Clean and merge IDS data from all regions
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(purrr)
library(glue)

source(here("scripts/utils/metadata_utils.R"))

# ==============================================================================
# CONFIG
# ==============================================================================

raw_dir <- here("01_ids/data/raw")
out_path <- here("01_ids/data/processed/ids_damage_areas_cleaned.gpkg")

# Fields to keep (codes only - use lookup tables for names)
keep_fields <- c(
  # Identifiers
  "OBSERVATION_ID",
  "DAMAGE_AREA_ID",
  
  # Spatial/temporal
  "SURVEY_YEAR",
  "REGION_ID",
  
  # What was damaged (codes only - see lookup tables)
  "HOST_CODE",
  "DCA_CODE",
  "DAMAGE_TYPE_CODE",
  
  # Extent
  "ACRES",
  "AREA_TYPE",
  "OBSERVATION_COUNT",
  
  # Intensity - DMSM (2015+)
  "PERCENT_AFFECTED_CODE",
  "PERCENT_MID",
  
  # Intensity - Legacy (pre-2015)
  "LEGACY_TPA",
  "LEGACY_NO_TREES",
  "LEGACY_SEVERITY_CODE"
)

# ==============================================================================
# FUNCTIONS
# ==============================================================================

clean_region <- function(gdb_path, keep_fields, target_crs = 4326) {
  
  region_name <- basename(gdb_path)
  cat(glue("Processing {region_name}...\n"))
  
  # Get layer name
  layer_name <- get_layer_name(gdb_path, "DAMAGE_AREAS_FLAT")
  
  # Read layer
  layer <- st_read(gdb_path, layer = layer_name, quiet = TRUE)
  cat(glue("  Read {nrow(layer)} features\n"))
  
  # Transform to common CRS
  original_crs <- st_crs(layer)$input
  layer <- st_transform(layer, target_crs)
  cat(glue("  Transformed from {original_crs} to EPSG:{target_crs}\n"))
  
  # Select fields (keep geometry automatically)
  layer <- layer |>
    select(all_of(keep_fields))
  
  # Clean OBSERVATION_COUNT (standardize case)
  layer <- layer |>
    mutate(OBSERVATION_COUNT = toupper(OBSERVATION_COUNT))
  
  # Recode PERCENT_AFFECTED_CODE -1 to NA
  layer <- layer |>
    mutate(PERCENT_AFFECTED_CODE = if_else(PERCENT_AFFECTED_CODE == -1, 
                                           NA_integer_, 
                                           PERCENT_AFFECTED_CODE))
  
  # Add source file for traceability
  layer <- layer |>
    mutate(SOURCE_FILE = region_name)
  
  cat(glue("  Cleaned: {nrow(layer)} features, {ncol(layer)} fields\n\n"))
  
  return(layer)
}

# ==============================================================================
# PROCESS ALL REGIONS
# ==============================================================================

cat("=== CLEANING IDS DATA ===\n\n")

# Find all gdbs
gdb_dirs <- list.dirs(raw_dir, recursive = FALSE, full.names = TRUE)
gdb_dirs <- gdb_dirs[grepl("\\.gdb$", gdb_dirs)]
cat(glue("Found {length(gdb_dirs)} geodatabases\n\n"))

# Process each region
all_regions <- map(gdb_dirs, ~clean_region(.x, keep_fields))

# ==============================================================================
# MERGE ALL REGIONS
# ==============================================================================

cat("=== MERGING REGIONS ===\n")

ids_merged <- bind_rows(all_regions)

cat(glue("Total features: {nrow(ids_merged)}\n"))
cat(glue("Total fields: {ncol(ids_merged)}\n\n"))

# Quick check
cat("Features by region:\n")
print(table(ids_merged$SOURCE_FILE))

cat("\nFeatures by year (sample):\n")
print(table(ids_merged$SURVEY_YEAR))

cat("\nOBSERVATION_COUNT:\n")
print(table(ids_merged$OBSERVATION_COUNT))

cat("\nPERCENT_AFFECTED_CODE (should have NAs now):\n")
print(table(ids_merged$PERCENT_AFFECTED_CODE, useNA = "ifany"))

# ==============================================================================
# SAVE
# ==============================================================================

cat(glue("\n=== SAVING TO {out_path} ===\n"))

# Create output directory if needed
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

# Save as geopackage
st_write(ids_merged, out_path, delete_dsn = TRUE, quiet = TRUE)

cat(glue("Saved {nrow(ids_merged)} features\n"))
cat(glue("File size: {round(file.size(out_path) / 1e9, 2)} GB\n"))

cat("\n=== DONE ===\n")