# ==============================================================================
# 02_inspect_ids.R
# Inspect downloaded IDS geodatabases
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(purrr)
library(glue)
library(readr)

source(here("scripts/utils/metadata_utils.R"))

# ==============================================================================
# 1. FIND ALL DOWNLOADED GDBs
# ==============================================================================

raw_dir <- here("01_ids/data/raw")
gdb_dirs <- list.dirs(raw_dir, recursive = FALSE, full.names = TRUE)
gdb_dirs <- gdb_dirs[grepl("\\.gdb$", gdb_dirs)]

cat(glue("Found {length(gdb_dirs)} geodatabases:\n\n"))
for (gdb in gdb_dirs) {
  cat(" ", basename(gdb), "\n")
}

# ==============================================================================
# 2. LIST LAYERS IN EACH GDB
# ==============================================================================

cat("\n\n=== LAYERS BY REGION ===\n")

layer_info <- map_df(gdb_dirs, function(gdb) {
  info <- st_layers(gdb)
  tibble(
    region = basename(gdb),
    layer = info$name,
    geom_type = sapply(info$geomtype, `[`, 1),
    n_features = info$features,
    n_fields = info$fields
  )
})

# Show summary by layer type (strip region suffix)
layer_info |>
  mutate(layer_type = gsub("_AllYears.*$", "", layer)) |>
  group_by(layer_type) |>
  summarise(
    n_regions = n(),
    total_features = sum(n_features, na.rm = TRUE),
    .groups = "drop"
  ) |>
  print()

# ==============================================================================
# 3. CHECK FIELD CONSISTENCY ACROSS REGIONS
# ==============================================================================

cat("\n\n=== CHECKING FIELD CONSISTENCY (DAMAGE_AREAS) ===\n")

# Get field names from each region - find the DAMAGE_AREAS layer dynamically
fields_by_region <- map(gdb_dirs, function(gdb) {
  target_layer <- get_layer_name(gdb, "DAMAGE_AREAS_FLAT")
  
  layer <- st_read(gdb, layer = target_layer, 
                   query = glue("SELECT * FROM \"{target_layer}\" LIMIT 1"),
                   quiet = TRUE)
  names(st_drop_geometry(layer))
})
names(fields_by_region) <- basename(gdb_dirs)

# Check if all regions have same fields
all_fields <- unique(unlist(fields_by_region))
field_presence <- map_df(fields_by_region, function(fields) {
  tibble(field = all_fields, present = all_fields %in% fields)
}, .id = "region")

# Find inconsistencies
field_summary <- field_presence |>
  group_by(field) |>
  summarise(n_regions = sum(present), .groups = "drop") |>
  arrange(n_regions)

inconsistent <- field_summary |> filter(n_regions < length(gdb_dirs))

if (nrow(inconsistent) > 0) {
  cat("\nFields NOT present in all regions:\n")
  print(inconsistent)
} else {
  cat("\nAll regions have identical fields.\n")
}

cat(glue("\nTotal fields in DAMAGE_AREAS_FLAT: {length(all_fields)}\n"))

# ==============================================================================
# 4. INSPECT ONE REGION IN DETAIL
# ==============================================================================

cat("\n\n=== DETAILED INSPECTION: REGION 5 (California) ===\n")

r5_path <- here("01_ids/data/raw/CONUS_Region5_AllYears.gdb")

if (file.exists(r5_path)) {
  
  # Summarize the gdb
  cat("\nGDB Summary:\n")
  gdb_summary <- summarize_gdb(r5_path)
  print(gdb_summary)
  
  # Get actual layer names
  r5_damage_areas <- get_layer_name(r5_path, "DAMAGE_AREAS_FLAT")
  r5_damage_points <- get_layer_name(r5_path, "DAMAGE_POINTS_FLAT")
  r5_surveyed <- get_layer_name(r5_path, "SURVEYED_AREAS_FLAT")
  
  # Extract metadata from DAMAGE_AREAS
  cat(glue("\n\nExtracting metadata from {r5_damage_areas}...\n"))
  metadata_areas <- extract_gdb_metadata(r5_path, r5_damage_areas, sample_size = 5000)
  
  cat(glue("\nSampled {attr(metadata_areas, 'n_features_sampled')} of {attr(metadata_areas, 'n_features_total')} features\n"))
  cat(glue("Geometry type: {attr(metadata_areas, 'geometry_type')}\n"))
  cat(glue("CRS: {attr(metadata_areas, 'crs')}\n"))
  
  # Show metadata
  cat("\n\nField summary:\n")
  metadata_areas |>
    select(field_name, r_class, n_unique, pct_missing, range_or_levels) |>
    print(n = 50)
  
  # Check SURVEY_YEAR range
  cat("\n\nSURVEY_YEAR distribution:\n")
  year_query <- glue("SELECT SURVEY_YEAR FROM \"{r5_damage_areas}\"")
  layer_years <- st_read(r5_path, layer = r5_damage_areas,
                         query = year_query, quiet = TRUE) |> 
    st_drop_geometry()
  
  cat("Range:", range(layer_years$SURVEY_YEAR), "\n")
  print(table(layer_years$SURVEY_YEAR))
  
} else {
  cat("Region 5 not found - adjust path to an available region.\n")
}

# ==============================================================================
# 5. GENERATE DATA DICTIONARY
# ==============================================================================

cat("\n\n=== GENERATING DATA DICTIONARY ===\n")

if (exists("metadata_areas")) {
  
  # Create data dictionary with placeholders for descriptions
  data_dict <- metadata_areas |>
    select(
      field_name,
      r_class,
      n_unique,
      pct_missing,
      range_or_levels,
      sample_values
    ) |>
    mutate(
      description = "",
      units = "",
      notes = ""
    ) |>
    select(field_name, description, r_class, units, range_or_levels, 
           pct_missing, n_unique, sample_values, notes)
  
  # Save
  dict_path <- here("01_ids/data_dictionary.csv")
  write_csv(data_dict, dict_path)
  cat(glue("Saved data dictionary to: {dict_path}\n"))
  cat("Fill in 'description' column using docs/IDS2_FlatFiles_Readme.pdf\n")
}

# ==============================================================================
# 6. QUICK LOOK AT OTHER LAYERS
# ==============================================================================

cat("\n\n=== OTHER LAYERS ===\n")

if (file.exists(r5_path)) {
  
  # DAMAGE_POINTS_FLAT
  cat(glue("\n{r5_damage_points}:\n"))
  points_info <- st_layers(r5_path)
  n_points <- points_info$features[points_info$name == r5_damage_points]
  cat(glue("  Features: {format(n_points, big.mark = ',')}\n"))
  
  points_sample <- st_read(r5_path, layer = r5_damage_points,
                           query = glue("SELECT * FROM \"{r5_damage_points}\" LIMIT 5"),
                           quiet = TRUE)
  cat(glue("  Fields: {ncol(points_sample) - 1}\n"))
  
  # SURVEYED_AREAS_FLAT
  cat(glue("\n{r5_surveyed}:\n"))
  n_surveyed <- points_info$features[points_info$name == r5_surveyed]
  cat(glue("  Features: {format(n_surveyed, big.mark = ',')}\n"))
}

# ==============================================================================
# 7. SUMMARY STATS ACROSS ALL REGIONS
# ==============================================================================

cat("\n\n=== TOTAL FEATURES ACROSS ALL REGIONS ===\n")

totals <- layer_info |>
  mutate(layer_type = gsub("_AllYears.*$", "", layer)) |>
  group_by(layer_type) |>
  summarise(
    total_features = sum(n_features, na.rm = TRUE),
    .groups = "drop"
  )

print(totals)

cat(glue("\nGrand total DAMAGE_AREAS: {format(totals$total_features[totals$layer_type == 'DAMAGE_AREAS_FLAT'], big.mark = ',')}\n"))

# ==============================================================================
# DONE
# ==============================================================================

cat("\n\n=== INSPECTION COMPLETE ===\n")
cat("Next steps:\n")
cat("  1. Review data_dictionary.csv and fill in descriptions from PDF\n")
cat("  2. Note any issues in cleaning_log.md\n")
cat("  3. Run 03_clean_ids.R\n")

