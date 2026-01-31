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
# 8. GENERATE LOOKUP TABLES
# ==============================================================================

cat("\n\n=== GENERATING LOOKUP TABLES ===\n")

# HOST_CODE lookup
host_query <- glue("SELECT HOST_CODE, HOST FROM \"{r5_damage_areas}\" WHERE HOST IS NOT NULL")
host_lookup <- st_read(r5_path, query = host_query, quiet = TRUE) |> 
  st_drop_geometry() |>
  distinct(HOST_CODE, HOST) |>
  arrange(HOST_CODE)

write_csv(host_lookup, here("01_ids/host_code_lookup.csv"))
cat(glue("Saved {nrow(host_lookup)} host codes to host_code_lookup.csv\n"))

# DCA_CODE lookup
dca_query <- glue("SELECT DCA_CODE, DCA_COMMON_NAME FROM \"{r5_damage_areas}\"")
dca_lookup <- st_read(r5_path, query = dca_query, quiet = TRUE) |> 
  st_drop_geometry() |>
  distinct(DCA_CODE, DCA_COMMON_NAME) |>
  arrange(DCA_CODE)

write_csv(dca_lookup, here("01_ids/dca_code_lookup.csv"))
cat(glue("Saved {nrow(dca_lookup)} DCA codes to dca_code_lookup.csv\n"))

# DAMAGE_TYPE lookup
damage_type_query <- glue("SELECT DAMAGE_TYPE_CODE, DAMAGE_TYPE FROM \"{r5_damage_areas}\"")
damage_type_lookup <- st_read(r5_path, query = damage_type_query, quiet = TRUE) |> 
  st_drop_geometry() |>
  distinct(DAMAGE_TYPE_CODE, DAMAGE_TYPE) |>
  arrange(DAMAGE_TYPE_CODE)

write_csv(damage_type_lookup, here("01_ids/damage_type_lookup.csv"))
cat(glue("Saved {nrow(damage_type_lookup)} damage types to damage_type_lookup.csv\n"))

# PERCENT_AFFECTED lookup
pct_query <- glue("SELECT PERCENT_AFFECTED_CODE, PERCENT_AFFECTED FROM \"{r5_damage_areas}\" WHERE PERCENT_AFFECTED IS NOT NULL")
pct_lookup <- st_read(r5_path, query = pct_query, quiet = TRUE) |> 
  st_drop_geometry() |>
  distinct(PERCENT_AFFECTED_CODE, PERCENT_AFFECTED) |>
  arrange(PERCENT_AFFECTED_CODE)

write_csv(pct_lookup, here("01_ids/percent_affected_lookup.csv"))
cat(glue("Saved {nrow(pct_lookup)} percent affected codes to percent_affected_lookup.csv\n"))

# LEGACY_SEVERITY lookup
severity_query <- glue("SELECT LEGACY_SEVERITY_CODE, LEGACY_SEVERITY FROM \"{r5_damage_areas}\" WHERE LEGACY_SEVERITY IS NOT NULL")
severity_lookup <- st_read(r5_path, query = severity_query, quiet = TRUE) |> 
  st_drop_geometry() |>
  distinct(LEGACY_SEVERITY_CODE, LEGACY_SEVERITY) |>
  arrange(LEGACY_SEVERITY_CODE)

write_csv(severity_lookup, here("01_ids/legacy_severity_lookup.csv"))
cat(glue("Saved {nrow(severity_lookup)} legacy severity codes to legacy_severity_lookup.csv\n"))

# REGION lookup (manually created - not extracted from data)
region_lookup <- tibble(
  REGION_ID = c(1, 2, 3, 4, 5, 5, 6, 8, 9, 10),
  REGION_NAME = c(
    "Northern",
    "Rocky Mountain", 
    "Southwestern",
    "Intermountain",
    "Pacific Southwest (CA)",
    "Pacific Southwest (HI)",
    "Pacific Northwest",
    "Southern",
    "Eastern",
    "Alaska"
  ),
  STATES = c(
    "MT, ND, ID panhandle",
    "CO, WY, SD, NE, KS",
    "AZ, NM",
    "UT, NV, ID, WY",
    "CA",
    "HI",
    "OR, WA",
    "13 SE states",
    "20 NE/MW states",
    "AK"
  ),
  US_AREA = c("CONUS", "CONUS", "CONUS", "CONUS", "CONUS", "HAWAII", "CONUS", "CONUS", "CONUS", "ALASKA")
)

write_csv(region_lookup, here("01_ids/region_lookup.csv"))
cat(glue("Saved {nrow(region_lookup)} regions to region_lookup.csv\n"))

# ==============================================================================
# DONE
# ==============================================================================

cat("\n\n=== INSPECTION COMPLETE ===\n")
cat("Next steps:\n")
cat("  1. Review data_dictionary.csv and fill in descriptions from PDF\n")
cat("  2. Note any issues in cleaning_log.md\n")
cat("  3. Run 03_clean_ids.R\n")

