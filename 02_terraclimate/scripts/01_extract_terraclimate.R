# ==============================================================================
# 01_extract_terraclimate.R
# Extract TerraClimate annual means at IDS polygon centroids
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(glue)
library(readr)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))

# ==============================================================================
# SETUP
# ==============================================================================

cat("=== TERRACLIMATE EXTRACTION ===\n\n")

config <- load_config()
ee <- init_gee()

tc_config <- config$raw$terraclimate
output_dir <- here("02_terraclimate/data/raw")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# All variables to extract
all_vars <- names(tc_config$variables)
cat(glue("Variables to extract: {paste(all_vars, collapse=', ')}\n\n"))

# ==============================================================================
# LOAD IDS DATA
# ==============================================================================

cat("[1] Loading IDS data...\n")

ids_path <- here("01_ids/data/processed/ids_layers_cleaned.gpkg")

if (!file.exists(ids_path)) {
  stop("IDS data not found. Run IDS cleaning scripts first.")
}

layer_specs <- list(
  damage_areas = list(
    layer = "damage_areas",
    id_col = "OBSERVATION_ID",
    geom_id = "DAMAGE_AREA_ID",
    method = "polygon"
  ),
  damage_points = list(
    layer = "damage_points",
    id_col = "OBSERVATION_ID",
    geom_id = NULL,
    method = "centroid"
  ),
  surveyed_areas = list(
    layer = "surveyed_areas",
    id_col = "SURVEY_FEATURE_ID",
    geom_id = NULL,
    method = "polygon"
  )
)

# ==============================================================================
# EXTRACTION LOOP
# ==============================================================================

cat("[2] Starting extraction...\n\n")

total_features <- 0
total_time <- 0
errors <- list()

for (layer_name in names(layer_specs)) {
  spec <- layer_specs[[layer_name]]
  cat(glue("=== LAYER: {layer_name} ===\n"))
  
  batch_query <- glue("SELECT DISTINCT REGION_ID, SURVEY_YEAR FROM {spec$layer} ORDER BY REGION_ID, SURVEY_YEAR")
  batches <- st_read(ids_path, query = batch_query, quiet = TRUE) |> st_drop_geometry()
  
  cat(glue("  Total batches (region × year): {nrow(batches)}\n"))
  cat(glue("  Years: {min(batches$SURVEY_YEAR)} - {max(batches$SURVEY_YEAR)}\n"))
  cat(glue("  Regions: {paste(sort(unique(batches$REGION_ID)), collapse=', ')}\n\n"))
  
  completed_files <- list.files(
    output_dir,
    pattern = glue("^tc_{layer_name}_r\\d+_\\d{{4}}\\.csv$")
  )
  completed_batches <- gsub(glue("tc_{layer_name}_r(\\d+)_(\\d{{4}})\\.csv"), "\\1_\\2", completed_files)
  
  batches$batch_id <- paste(batches$REGION_ID, batches$SURVEY_YEAR, sep = "_")
  batches$completed <- batches$batch_id %in% completed_batches
  
  n_completed <- sum(batches$completed)
  n_remaining <- sum(!batches$completed)
  
  cat(glue("  Completed: {n_completed}/{nrow(batches)}\n"))
  cat(glue("  Remaining: {n_remaining}\n\n"))
  
  if (n_remaining == 0) {
    cat("  All batches complete. Skipping layer.\n\n")
    next
  }
  
  batches_todo <- batches |> filter(!completed)
  
  for (i in seq_len(nrow(batches_todo))) {
    region <- batches_todo$REGION_ID[i]
    year <- batches_todo$SURVEY_YEAR[i]
    
    cat(glue("Batch {i}/{nrow(batches_todo)}: Region {region}, Year {year}... "))
    
    t_start <- Sys.time()
    
    tryCatch({
      select_cols <- c("REGION_ID", "SURVEY_YEAR", spec$id_col, "geom")
      if (!is.null(spec$geom_id)) {
        select_cols <- c(select_cols, spec$geom_id)
      }
      
      query <- glue("SELECT {paste(select_cols, collapse = ', ')} FROM {spec$layer} 
                    WHERE REGION_ID = {region} AND SURVEY_YEAR = {year}")
      
      ids_batch <- st_read(ids_path, query = query, quiet = TRUE)
      ids_batch <- st_make_valid(ids_batch)
      n_features <- nrow(ids_batch)
      
      if (n_features == 0) {
        cat("no features, skipping\n")
        next
      }
      
      tc_annual <- get_terraclimate_annual(year, all_vars, ee)
      
      if (layer_name == "damage_areas") {
        unique_areas <- ids_batch |>
          distinct(DAMAGE_AREA_ID, .keep_all = TRUE)
        
        batch_result <- extract_in_batches(
          unique_areas,
          id_col = "DAMAGE_AREA_ID",
          image = tc_annual,
          ee = ee,
          method = "polygon",
          scale = 4000
        )
        
        batch_result <- ids_batch |>
          st_drop_geometry() |>
          select(OBSERVATION_ID, DAMAGE_AREA_ID, REGION_ID, SURVEY_YEAR) |>
          left_join(batch_result, by = "DAMAGE_AREA_ID")
      } else if (layer_name == "surveyed_areas") {
        batch_result <- extract_in_batches(
          ids_batch,
          id_col = "SURVEY_FEATURE_ID",
          image = tc_annual,
          ee = ee,
          method = "polygon",
          scale = 4000
        )
        
        batch_result <- ids_batch |>
          st_drop_geometry() |>
          select(SURVEY_FEATURE_ID, REGION_ID, SURVEY_YEAR) |>
          left_join(batch_result, by = "SURVEY_FEATURE_ID")
      } else {
        batch_result <- extract_in_batches(
          ids_batch,
          id_col = spec$id_col,
          image = tc_annual,
          ee = ee,
          method = spec$method,
          scale = 4000
        )
        
        batch_result <- ids_batch |>
          st_drop_geometry() |>
          select(OBSERVATION_ID, REGION_ID, SURVEY_YEAR) |>
          left_join(batch_result, by = "OBSERVATION_ID")
      }
      
      output_file <- file.path(output_dir, glue("tc_{layer_name}_r{region}_{year}.csv"))
      write_csv(batch_result, output_file)
      
      t_end <- Sys.time()
      elapsed <- as.numeric(difftime(t_end, t_start, units = "secs"))
      
      total_features <- total_features + n_features
      total_time <- total_time + elapsed
      
      cat(glue("{n_features} features, {round(elapsed, 1)}s\n"))
      
    }, error = function(e) {
      cat(glue("ERROR: {e$message}\n"))
      errors[[length(errors) + 1]] <- list(layer = layer_name, region = region, year = year, error = e$message)
    })
  }
  
  cat("\n")
}

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("\n")
cat("================================================================================\n")
cat("EXTRACTION COMPLETE\n")
cat("================================================================================\n\n")

cat(glue("Total features extracted: {format(total_features, big.mark=',')}\n"))
cat(glue("Total time: {round(total_time/60, 1)} minutes\n"))
cat(glue("Average rate: {round(total_features/total_time, 0)} features/sec\n\n"))

if (length(errors) > 0) {
  cat(glue("Errors encountered: {length(errors)}\n"))
  for (err in errors) {
    cat(glue("  - Region {err$region}, Year {err$year}: {err$error}\n"))
  }
  cat("\nRe-run script to retry failed batches.\n")
} else {
  cat("No errors.\n")
}

cat(glue("\nOutput files: {output_dir}/tc_*_r*_*.csv\n"))
cat("================================================================================\n")
