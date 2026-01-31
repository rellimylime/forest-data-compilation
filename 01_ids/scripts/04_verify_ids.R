# ==============================================================================
# 04_verify_ids.R
# Verify cleaned IDS data before proceeding to TerraClimate merge
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(glue)

# ==============================================================================
# LOAD DATA
# ==============================================================================

cat("=== IDS CLEANED DATA VERIFICATION ===\n\n")

gpkg_path <- here("01_ids/data/processed/ids_damage_areas_cleaned.gpkg")

if (!file.exists(gpkg_path)) {
  stop("Cleaned data not found: ", gpkg_path)
}

cat("Loading data (this may take a moment)...\n")
ids <- st_read(gpkg_path, quiet = TRUE)
cat("Done.\n\n")

# ==============================================================================
# 1. BASIC PROPERTIES
# ==============================================================================

cat("=== 1. BASIC PROPERTIES ===\n\n")

cat(glue("File: {basename(gpkg_path)}\n"))
cat(glue("Size: {round(file.size(gpkg_path) / 1e9, 2)} GB\n"))
cat(glue("Features: {format(nrow(ids), big.mark = ',')}\n"))
cat(glue("Fields: {ncol(ids) - 1} (excluding geometry)\n"))
cat(glue("CRS: {st_crs(ids)$input}\n"))
cat(glue("EPSG: {st_crs(ids)$epsg}\n"))
cat(glue("Geometry type: {unique(st_geometry_type(ids))}\n"))

# Bounding box
bbox <- st_bbox(ids)
cat(glue("\nBounding box:\n"))
cat(glue("  xmin: {round(bbox['xmin'], 2)}\n"))
cat(glue("  xmax: {round(bbox['xmax'], 2)}\n"))
cat(glue("  ymin: {round(bbox['ymin'], 2)}\n"))
cat(glue("  ymax: {round(bbox['ymax'], 2)}\n"))

# ==============================================================================
# 2. FIELD STRUCTURE
# ==============================================================================

cat("\n\n=== 2. FIELD STRUCTURE ===\n\n")

expected_fields <- c(
  "OBSERVATION_ID", "DAMAGE_AREA_ID", "SURVEY_YEAR", "REGION_ID",
  "HOST_CODE", "DCA_CODE", "DAMAGE_TYPE_CODE",
  "ACRES", "AREA_TYPE", "OBSERVATION_COUNT",
  "PERCENT_AFFECTED_CODE", "PERCENT_MID",
  "LEGACY_TPA", "LEGACY_NO_TREES", "LEGACY_SEVERITY_CODE",
  "SOURCE_FILE"
)

actual_fields <- names(st_drop_geometry(ids))

cat("Expected fields:", length(expected_fields), "\n")
cat("Actual fields:", length(actual_fields), "\n\n")

missing <- setdiff(expected_fields, actual_fields)
extra <- setdiff(actual_fields, expected_fields)

if (length(missing) > 0) {
  cat("MISSING fields:\n")
  cat(paste(" -", missing, collapse = "\n"), "\n")
} else {
  cat("✓ All expected fields present\n")
}

if (length(extra) > 0) {
  cat("EXTRA fields:\n")
  cat(paste(" -", extra, collapse = "\n"), "\n")
}

cat("\nField types:\n")
for (field in actual_fields) {
  cat(glue("  {field}: {class(ids[[field]])[1]}\n"))
}

# ==============================================================================
# 3. MISSING DATA CHECK
# ==============================================================================

cat("\n\n=== 3. MISSING DATA ===\n\n")

missing_summary <- ids |>
  st_drop_geometry() |>
  summarise(across(everything(), ~sum(is.na(.)))) |>
  tidyr::pivot_longer(everything(), names_to = "field", values_to = "n_missing") |>
  mutate(pct_missing = round(n_missing / nrow(ids) * 100, 2)) |>
  arrange(desc(pct_missing))

missing_summary

# ==============================================================================
# 4. CLEANING ACTIONS VERIFICATION
# ==============================================================================

cat("\n\n=== 4. CLEANING ACTIONS VERIFICATION ===\n\n")

# Check OBSERVATION_COUNT is uppercase
obs_count_vals <- unique(ids$OBSERVATION_COUNT)
cat("OBSERVATION_COUNT values:", paste(obs_count_vals, collapse = ", "), "\n")
if (all(obs_count_vals == toupper(obs_count_vals))) {
  cat("✓ All uppercase\n")
} else {
  cat("✗ Mixed case detected\n")
}

# Check PERCENT_AFFECTED_CODE has no -1
pct_vals <- unique(ids$PERCENT_AFFECTED_CODE)
cat("\nPERCENT_AFFECTED_CODE values:", paste(sort(na.omit(pct_vals)), collapse = ", "), "\n")
if (!(-1 %in% pct_vals)) {
  cat("✓ No -1 values (recoded to NA)\n")
} else {
  cat("✗ Still contains -1 values\n")
}

# Check SOURCE_FILE populated
cat("\nSOURCE_FILE values:\n")
print(table(ids$SOURCE_FILE))

# ==============================================================================
# 5. SUMMARY STATISTICS
# ==============================================================================

cat("\n\n=== 5. SUMMARY STATISTICS ===\n\n")

# By region
cat("Features by REGION_ID:\n")
print(table(ids$REGION_ID))

# By year
cat("\nFeatures by SURVEY_YEAR:\n")
year_summary <- ids |>
  st_drop_geometry() |>
  count(SURVEY_YEAR) |>
  arrange(SURVEY_YEAR)
year_summary

cat("\nYear range:", min(ids$SURVEY_YEAR), "-", max(ids$SURVEY_YEAR), "\n")

# By damage type
cat("\nFeatures by DAMAGE_TYPE_CODE:\n")
print(table(ids$DAMAGE_TYPE_CODE))

# ACRES summary
cat("\nACRES summary:\n")
print(summary(ids$ACRES))

# Observation count
cat("\nOBSERVATION_COUNT:\n")
obs_tbl <- table(ids$OBSERVATION_COUNT)
print(obs_tbl)
cat(glue("\nMultiple observations: {round(obs_tbl['MULTIPLE'] / sum(obs_tbl) * 100, 1)}%\n"))

# ==============================================================================
# 6. INTENSITY FIELDS CHECK
# ==============================================================================

cat("\n\n=== 6. INTENSITY FIELDS (Legacy vs DMSM) ===\n\n")

# PERCENT_AFFECTED_CODE by year
cat("PERCENT_AFFECTED_CODE population by year:\n")
pct_by_year <- ids |>
  st_drop_geometry() |>
  group_by(SURVEY_YEAR) |>
  summarise(
    n = n(),
    pct_has_value = round(mean(!is.na(PERCENT_AFFECTED_CODE)) * 100, 1),
    .groups = "drop"
  )
pct_by_year

# LEGACY_TPA by year
cat("\nLEGACY_TPA population by year (non-zero):\n")
legacy_by_year <- ids |>
  st_drop_geometry() |>
  group_by(SURVEY_YEAR) |>
  summarise(
    n = n(),
    pct_nonzero = round(mean(LEGACY_TPA > 0, na.rm = TRUE) * 100, 1),
    .groups = "drop"
  )
legacy_by_year

# ==============================================================================
# 7. GEOMETRY VALIDATION
# ==============================================================================

cat("\n\n=== 7. GEOMETRY VALIDATION ===\n\n")

# Check for invalid geometries
n_invalid <- sum(!st_is_valid(ids))
cat(glue("Invalid geometries: {format(n_invalid, big.mark = ',')} ({round(n_invalid/nrow(ids)*100, 2)}%)\n"))

# Check for empty geometries
n_empty <- sum(st_is_empty(ids))
cat(glue("Empty geometries: {format(n_empty, big.mark = ',')}\n"))

# ==============================================================================
# 8. READY FOR TERRACLIMATE
# ==============================================================================

cat("\n\n=== 8. TERRACLIMATE MERGE READINESS ===\n\n")

checks <- c(
  "CRS is EPSG:4326" = st_crs(ids)$epsg == 4326,
  "Has SURVEY_YEAR" = "SURVEY_YEAR" %in% names(ids),
  "Has geometry" = inherits(ids, "sf"),
  "Feature count > 0" = nrow(ids) > 0,
  "No empty geometries" = n_empty == 0
)

for (check_name in names(checks)) {
  status <- if(checks[[check_name]]) "✓" else "✗"
  cat(glue("{status} {check_name}\n"))
}

if (all(checks)) {
  cat("\n✓ DATA READY FOR TERRACLIMATE MERGE\n")
} else {
  cat("\n✗ Issues need to be resolved before merge\n")
}

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("\n\n=== SUMMARY ===\n\n")
cat(glue("Total features: {format(nrow(ids), big.mark = ',')}\n"))
cat(glue("Years: {min(ids$SURVEY_YEAR)}-{max(ids$SURVEY_YEAR)}\n"))
cat(glue("Regions: {length(unique(ids$REGION_ID))}\n"))
cat(glue("CRS: EPSG:{st_crs(ids)$epsg}\n"))
cat(glue("File size: {round(file.size(gpkg_path) / 1e9, 2)} GB\n"))

cat("\n=== VERIFICATION COMPLETE ===\n")