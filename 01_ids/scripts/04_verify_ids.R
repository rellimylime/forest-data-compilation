# ==============================================================================
# 04_verify_ids.R
# Verify cleaned IDS data before proceeding to TerraClimate merge
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(tidyr)
library(purrr)
library(glue)

# ==============================================================================
# LOAD DATA
# ==============================================================================

cat("=== IDS CLEANED DATA VERIFICATION ===\n\n")

gpkg_path <- here("01_ids/data/processed/ids_layers_cleaned.gpkg")

if (!file.exists(gpkg_path)) {
  stop("Cleaned data not found: ", gpkg_path)
}

layer_info <- st_layers(gpkg_path)
layer_names <- layer_info$name
cat("Layers found:\n")
cat(paste(" -", layer_names, collapse = "\n"), "\n\n")

# ==============================================================================
# 1. BASIC PROPERTIES
# ==============================================================================

cat("=== 1. BASIC PROPERTIES ===\n\n")

cat(glue("File: {basename(gpkg_path)}\n"))
cat(glue("Size: {round(file.size(gpkg_path) / 1e9, 2)} GB\n\n"))

layer_expected_fields <- list(
  damage_areas = c(
    "OBSERVATION_ID", "DAMAGE_AREA_ID", "SURVEY_YEAR", "REGION_ID",
    "HOST_CODE", "DCA_CODE", "DAMAGE_TYPE_CODE",
    "ACRES", "AREA_TYPE", "OBSERVATION_COUNT",
    "PERCENT_AFFECTED_CODE", "PERCENT_MID",
    "LEGACY_TPA", "LEGACY_NO_TREES", "LEGACY_SEVERITY_CODE",
    "SOURCE_FILE"
  ),
  damage_points = c(
    "OBSERVATION_ID", "DAMAGE_AREA_ID", "SURVEY_YEAR", "REGION_ID",
    "HOST_CODE", "DCA_CODE", "DAMAGE_TYPE_CODE",
    "ACRES", "AREA_TYPE", "OBSERVATION_COUNT",
    "PERCENT_AFFECTED_CODE", "PERCENT_MID",
    "LEGACY_TPA", "LEGACY_NO_TREES", "LEGACY_SEVERITY_CODE",
    "SOURCE_FILE"
  ),
  surveyed_areas = c(
    "SURVEY_YEAR", "REGION_ID", "ACRES", "AREA_TYPE", "SOURCE_FILE",
    "SURVEY_FEATURE_ID"
  )
)

# ==============================================================================
# 2. FIELD STRUCTURE
# ==============================================================================

verify_layer <- function(layer_name) {
  cat(glue("\n\n=== VERIFYING LAYER: {layer_name} ===\n\n"))
  ids <- st_read(gpkg_path, layer = layer_name, quiet = TRUE)
  
  cat(glue("Features: {format(nrow(ids), big.mark = ',')}\n"))
  cat(glue("Fields: {ncol(ids) - 1} (excluding geometry)\n"))
  cat(glue("CRS: {st_crs(ids)$input}\n"))
  cat(glue("EPSG: {st_crs(ids)$epsg}\n"))
  cat(glue("Geometry type: {unique(st_geometry_type(ids))}\n"))
  
  bbox <- st_bbox(ids)
  cat(glue("\nBounding box:\n"))
  cat(glue("  xmin: {round(bbox['xmin'], 2)}\n"))
  cat(glue("  xmax: {round(bbox['xmax'], 2)}\n"))
  cat(glue("  ymin: {round(bbox['ymin'], 2)}\n"))
  cat(glue("  ymax: {round(bbox['ymax'], 2)}\n"))
  
  cat("\n\n=== FIELD STRUCTURE ===\n\n")
  expected_fields <- layer_expected_fields[[layer_name]]
  actual_fields <- names(st_drop_geometry(ids))
  
  if (!is.null(expected_fields)) {
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
  }
  
  cat("\nField types:\n")
  for (field in actual_fields) {
    cat(glue("  {field}: {class(ids[[field]])[1]}\n"))
  }
  
  cat("\n\n=== MISSING DATA ===\n\n")
  missing_summary <- ids |>
    st_drop_geometry() |>
    summarise(across(everything(), ~sum(is.na(.)))) |>
    tidyr::pivot_longer(everything(), names_to = "field", values_to = "n_missing") |>
    mutate(pct_missing = round(n_missing / nrow(ids) * 100, 2)) |>
    arrange(desc(pct_missing))
  
  missing_summary
  
  cat("\n\n=== CLEANING ACTIONS VERIFICATION ===\n\n")
  if ("OBSERVATION_COUNT" %in% names(ids)) {
    obs_count_vals <- unique(ids$OBSERVATION_COUNT)
    cat("OBSERVATION_COUNT values:", paste(obs_count_vals, collapse = ", "), "\n")
    obs_count_vals <- na.omit(unique(ids$OBSERVATION_COUNT))
    if (all(obs_count_vals == toupper(obs_count_vals))) {
      cat("✓ All uppercase\n")
    } else {
      cat("✗ Mixed case detected\n")
    }
  }
  
  if ("PERCENT_AFFECTED_CODE" %in% names(ids)) {
    pct_vals <- unique(ids$PERCENT_AFFECTED_CODE)
    cat("\nPERCENT_AFFECTED_CODE values:", paste(sort(na.omit(pct_vals)), collapse = ", "), "\n")
    if (!(-1 %in% pct_vals)) {
      cat("✓ No -1 values (recoded to NA)\n")
    } else {
      cat("✗ Still contains -1 values\n")
    }
  }
  
  if ("SOURCE_FILE" %in% names(ids)) {
    cat("\nSOURCE_FILE values:\n")
    print(table(ids$SOURCE_FILE))
  }
  
  cat("\n\n=== SUMMARY STATISTICS ===\n\n")
  if ("REGION_ID" %in% names(ids)) {
    cat("Features by REGION_ID:\n")
    print(table(ids$REGION_ID))
  }
  
  if ("SURVEY_YEAR" %in% names(ids)) {
    cat("\nFeatures by SURVEY_YEAR:\n")
    year_summary <- ids |>
      st_drop_geometry() |>
      count(SURVEY_YEAR) |>
      arrange(SURVEY_YEAR)
    year_summary
    
    cat("\nYear range:", min(ids$SURVEY_YEAR), "-", max(ids$SURVEY_YEAR), "\n")
  }
  
  if ("DAMAGE_TYPE_CODE" %in% names(ids)) {
    cat("\nFeatures by DAMAGE_TYPE_CODE:\n")
    print(table(ids$DAMAGE_TYPE_CODE))
  }
  
  if ("ACRES" %in% names(ids)) {
    cat("\nACRES summary:\n")
    print(summary(ids$ACRES))
  }
  
  if ("OBSERVATION_COUNT" %in% names(ids)) {
    cat("\nOBSERVATION_COUNT:\n")
    obs_tbl <- table(ids$OBSERVATION_COUNT)
    print(obs_tbl)
    if ("MULTIPLE" %in% names(obs_tbl)) {
      cat(glue("\nMultiple observations: {round(obs_tbl['MULTIPLE'] / sum(obs_tbl) * 100, 1)}%\n"))
    }
  }
  
  if (all(c("PERCENT_AFFECTED_CODE", "SURVEY_YEAR") %in% names(ids))) {
    cat("\n\n=== INTENSITY FIELDS (Legacy vs DMSM) ===\n\n")
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
  }
  
  if (all(c("LEGACY_TPA", "SURVEY_YEAR") %in% names(ids))) {
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
  }
  
  cat("\n\n=== GEOMETRY VALIDATION ===\n\n")
  n_invalid <- sum(!st_is_valid(ids))
  cat(glue("Invalid geometries: {format(n_invalid, big.mark = ',')} ({round(n_invalid/nrow(ids)*100, 2)}%)\n"))
  
  n_empty <- sum(st_is_empty(ids))
  cat(glue("Empty geometries: {format(n_empty, big.mark = ',')}\n"))
  
  cat("\n\n=== TERRACLIMATE MERGE READINESS ===\n\n")
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
  
  list(
    layer = layer_name,
    n_features = nrow(ids),
    min_year = if ("SURVEY_YEAR" %in% names(ids)) min(ids$SURVEY_YEAR) else NA_integer_,
    max_year = if ("SURVEY_YEAR" %in% names(ids)) max(ids$SURVEY_YEAR) else NA_integer_,
    epsg = st_crs(ids)$epsg
  )
}

layer_summaries <- map(layer_names, verify_layer)

cat("\n\n=== SUMMARY ===\n\n")
summary_df <- bind_rows(layer_summaries)
print(summary_df)
cat(glue("\nFile size: {round(file.size(gpkg_path) / 1e9, 2)} GB\n"))

cat("\n=== VERIFICATION COMPLETE ===\n")
