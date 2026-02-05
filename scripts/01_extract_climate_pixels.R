# ==============================================================================
# 01_extract_climate_pixels.R
# Generic pixel-level climate extraction for IDS polygons (multi-source)
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(glue)
library(readr)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/climate_extraction_utils.R"))

# ==============================================================================
# USER PARAMETERS
# ==============================================================================

# One of: terraclimate, worldclim, prism, era5_daily
climate_source <- "terraclimate"

# suggested = smaller analysis-ready set, all = broadest set, custom = manually set
variable_mode <- "all"
custom_variables <- NULL

# IDS filter: set to NULL for all observations, or provide exact DAMAGE_AGENT string
damage_agent_filter <- NULL

# One IDS layer at a time (damage_areas recommended for polygon climate extraction)
ids_layer <- "damage_areas"
ids_id_col <- "DAMAGE_AREA_ID"

# Output layout: keep climate pixel table separate from IDS observation table.
output_dir <- here("data/raw/climate_pixels")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# ==============================================================================
# SETUP
# ==============================================================================

cat("=== MULTI-SOURCE CLIMATE PIXEL EXTRACTION ===\n\n")

config <- load_config()
ee <- init_gee()

source_spec <- resolve_climate_source(
  source = climate_source,
  variable_mode = variable_mode,
  custom_variables = custom_variables
)

cat(glue("Source: {source_spec$source}\n"))
cat(glue("GEE collection: {source_spec$gee_collection}\n"))
cat(glue("Temporal resolution: {source_spec$temporal_resolution}\n"))
cat(glue("Variables ({length(source_spec$variables)}): {paste(source_spec$variables, collapse = ', ')}\n\n"))

ids_path <- here("01_ids/data/processed/ids_layers_cleaned.gpkg")
if (!file.exists(ids_path)) {
  stop("IDS data not found. Run IDS processing first.")
}

# ==============================================================================
# IDS BATCH DISCOVERY
# ==============================================================================

cat("[1] Discovering IDS region-year batches...\n")

batch_query <- glue("SELECT DISTINCT REGION_ID, SURVEY_YEAR FROM {ids_layer} ORDER BY REGION_ID, SURVEY_YEAR")
batches <- st_read(ids_path, query = batch_query, quiet = TRUE) |> st_drop_geometry()

if (nrow(batches) == 0) {
  stop("No IDS batches found for extraction.")
}

cat(glue("Total batches: {nrow(batches)}\n"))
cat(glue("Year range: {min(batches$SURVEY_YEAR)}-{max(batches$SURVEY_YEAR)}\n\n"))

# ==============================================================================
# EXTRACTION LOOP
# ==============================================================================

cat("[2] Extracting polygon-level climate pixels by batch...\n\n")

errors <- list()
for (i in seq_len(nrow(batches))) {
  region <- batches$REGION_ID[i]
  year <- batches$SURVEY_YEAR[i]

  out_file <- file.path(output_dir, glue("{climate_source}_{ids_layer}_pixels_r{region}_{year}.csv"))
  if (file.exists(out_file)) {
    cat(glue("Batch {i}/{nrow(batches)} R{region} {year}: already exists, skipping\n"))
    next
  }

  cat(glue("Batch {i}/{nrow(batches)} R{region} {year}: loading IDS polygons... "))

  tryCatch({
    where_filter <- glue("REGION_ID = {region} AND SURVEY_YEAR = {year}")
    if (!is.null(damage_agent_filter)) {
      where_filter <- glue("{where_filter} AND DAMAGE_AGENT = '{damage_agent_filter}'")
    }

    ids_query <- glue("SELECT REGION_ID, SURVEY_YEAR, OBSERVATION_ID, {ids_id_col}, geom FROM {ids_layer} WHERE {where_filter}")

    ids_batch <- st_read(ids_path, query = ids_query, quiet = TRUE) |>
      st_make_valid() |>
      distinct(.data[[ids_id_col]], .keep_all = TRUE)

    if (nrow(ids_batch) == 0) {
      cat("no rows after filters, skipping\n")
      next
    }

    cat(glue("{nrow(ids_batch)} polygons... extracting\n"))

    climate_pixels <- extract_polygon_pixel_timeseries(
      sf_polygons = ids_batch,
      id_col = ids_id_col,
      source_spec = source_spec,
      year = year,
      ee = ee,
      batch_size = 250,
      include_pixel_geometry = TRUE,
      tile_scale = 4
    )

    if (nrow(climate_pixels) == 0) {
      warning(glue("No climate pixel values returned for R{region} {year}."))
    }

    if (all(c("pixel_lon", "pixel_lat") %in% names(climate_pixels))) {
      climate_pixels <- climate_pixels |>
        mutate(climate_pixel_id = paste0(round(pixel_lon, 6), "_", round(pixel_lat, 6)))
    }

    # keep linkage separate: climate pixel table has polygon IDs only, no IDS attributes
    write_csv(climate_pixels, out_file)

    cat(glue("  wrote {nrow(climate_pixels)} rows -> {out_file}\n"))
  }, error = function(e) {
    msg <- glue("R{region} {year} failed: {e$message}")
    cat(glue("  ERROR: {msg}\n"))
    errors[[length(errors) + 1]] <- msg
  })
}

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("\n=== EXTRACTION COMPLETE ===\n")
cat(glue("Output directory: {output_dir}\n"))

if (length(errors) > 0) {
  cat(glue("Errors: {length(errors)}\n"))
  cat(paste0(" - ", unlist(errors), collapse = "\n"), "\n")
} else {
  cat("No errors recorded.\n")
}

cat("\nNote: PRISM coverage is CONUS-only; Alaska/Hawaii IDS batches will return empty outputs.\n")
