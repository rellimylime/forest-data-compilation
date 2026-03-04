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
out_path <- here("01_ids/data/processed/ids_layers_cleaned.gpkg")

# Fields to keep (codes only - use lookup tables for names)
damage_fields <- c(
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

survey_fields <- c(
  "SURVEY_YEAR",
  "REGION_ID",
  "ACRES",
  "AREA_TYPE"
)

layer_specs <- list(
  damage_areas = list(prefix = "DAMAGE_AREAS_FLAT", keep_fields = damage_fields),
  damage_points = list(prefix = "DAMAGE_POINTS_FLAT", keep_fields = damage_fields),
  surveyed_areas = list(prefix = "SURVEYED_AREAS_FLAT", keep_fields = survey_fields)
)

# ==============================================================================
# FUNCTIONS
# ==============================================================================

clean_region_layer <- function(gdb_path, layer_prefix, keep_fields, target_crs = 4326) {
  
  region_name <- basename(gdb_path)
  cat(glue("Processing {region_name} - {layer_prefix}...\n"))
  
  # Get layer name
  layer_name <- get_layer_name(gdb_path, layer_prefix)
  
  # Read layer
  layer <- st_read(gdb_path, layer = layer_name, quiet = TRUE)
  cat(glue("  Read {nrow(layer)} features\n"))
  
  # Transform to common CRS
  original_crs <- st_crs(layer)$input
  layer <- st_transform(layer, target_crs)
  cat(glue("  Transformed from {original_crs} to EPSG:{target_crs}\n"))
  
  # Select fields (keep geometry automatically)
  layer <- layer |>
    select(any_of(keep_fields))
  
  # Clean OBSERVATION_COUNT (standardize case)
  if ("OBSERVATION_COUNT" %in% names(layer)) {
    layer <- layer |>
      mutate(OBSERVATION_COUNT = toupper(OBSERVATION_COUNT))
  }
  
  # Recode PERCENT_AFFECTED_CODE -1 to NA
  if ("PERCENT_AFFECTED_CODE" %in% names(layer)) {
    layer <- layer |>
      mutate(PERCENT_AFFECTED_CODE = if_else(PERCENT_AFFECTED_CODE == -1, 
                                             NA_integer_, 
                                             PERCENT_AFFECTED_CODE))
  }
  
  # Add source file for traceability
  layer <- layer |>
    mutate(SOURCE_FILE = region_name)

  # Add stable ID for surveyed areas (per region)
  if (layer_prefix == "SURVEYED_AREAS_FLAT") {
    layer <- layer |>
      mutate(SURVEY_FEATURE_ID = paste0(region_name, "_", row_number()))
  }
  
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

# Process each layer type
ids_layers <- imap(layer_specs, function(spec, layer_name) {
  cat(glue("\n=== PROCESSING {layer_name} ===\n"))
  all_regions <- map(gdb_dirs, ~clean_region_layer(.x, spec$prefix, spec$keep_fields))
  
  ids_merged <- bind_rows(all_regions)
  
  cat(glue("Total features: {nrow(ids_merged)}\n"))
  cat(glue("Total fields: {ncol(ids_merged)}\n\n"))
  
  # Quick check
  cat("Features by region:\n")
  print(table(ids_merged$SOURCE_FILE))
  
  if ("SURVEY_YEAR" %in% names(ids_merged)) {
    cat("\nFeatures by year (sample):\n")
    print(table(ids_merged$SURVEY_YEAR))
  }
  
  if ("OBSERVATION_COUNT" %in% names(ids_merged)) {
    cat("\nOBSERVATION_COUNT:\n")
    print(table(ids_merged$OBSERVATION_COUNT))
  }
  
  if ("PERCENT_AFFECTED_CODE" %in% names(ids_merged)) {
    cat("\nPERCENT_AFFECTED_CODE (should have NAs now):\n")
    print(table(ids_merged$PERCENT_AFFECTED_CODE, useNA = "ifany"))
  }
  
  ids_merged
})

# ==============================================================================
# SAVE
# ==============================================================================

cat(glue("\n=== SAVING TO {out_path} ===\n"))

# Create output directory if needed
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

# Save as geopackage with one layer per IDS type
first_layer <- TRUE
imap(ids_layers, function(layer_data, layer_name) {
  st_write(
    layer_data,
    out_path,
    layer = layer_name,
    delete_dsn = first_layer,
    append = !first_layer,
    quiet = TRUE
  )
  cat(glue("Saved {layer_name}: {nrow(layer_data)} features\n"))
  first_layer <<- FALSE
})

cat(glue("File size: {round(file.size(out_path) / 1e9, 2)} GB\n"))

cat("\n=== DONE ===\n")
