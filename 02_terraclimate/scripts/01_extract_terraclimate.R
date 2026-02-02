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

ids_path <- here("01_ids/data/processed/ids_damage_areas_cleaned.gpkg")

if (!file.exists(ids_path)) {
  stop("IDS data not found. Run IDS cleaning scripts first.")
}

# Get unique region-year combinations for batching
batch_query <- "SELECT DISTINCT REGION_ID, SURVEY_YEAR FROM ids_damage_areas_cleaned ORDER BY REGION_ID, SURVEY_YEAR"
batches <- st_read(ids_path, query = batch_query, quiet = TRUE) |> st_drop_geometry()

cat(glue("  Total batches (region × year): {nrow(batches)}\n"))
cat(glue("  Years: {min(batches$SURVEY_YEAR)} - {max(batches$SURVEY_YEAR)}\n"))
cat(glue("  Regions: {paste(sort(unique(batches$REGION_ID)), collapse=', ')}\n\n"))

# ==============================================================================
# CHECK FOR EXISTING PROGRESS
# ==============================================================================

cat("[2] Checking existing progress...\n")

completed_files <- list.files(output_dir, pattern = "^tc_r\\d+_\\d{4}\\.csv$")
completed_batches <- gsub("tc_r(\\d+)_(\\d{4})\\.csv", "\\1_\\2", completed_files)

batches$batch_id <- paste(batches$REGION_ID, batches$SURVEY_YEAR, sep = "_")
batches$completed <- batches$batch_id %in% completed_batches

n_completed <- sum(batches$completed)
n_remaining <- sum(!batches$completed)

cat(glue("  Completed: {n_completed}/{nrow(batches)}\n"))
cat(glue("  Remaining: {n_remaining}\n\n"))

if (n_remaining == 0) {
  cat("All batches complete. Nothing to do.\n")
  quit(save = "no")
}

# Filter to remaining batches
batches_todo <- batches |> filter(!completed)

# ==============================================================================
# EXTRACTION LOOP
# ==============================================================================

cat("[3] Starting extraction...\n\n")

total_features <- 0
total_time <- 0
errors <- list()

for (i in seq_len(nrow(batches_todo))) {
  
  region <- batches_todo$REGION_ID[i]
  year <- batches_todo$SURVEY_YEAR[i]
  
  cat(glue("Batch {i}/{nrow(batches_todo)}: Region {region}, Year {year}... "))
  
  t_start <- Sys.time()
  
  tryCatch({
    
    # Load features for this batch
    query <- glue("SELECT OBSERVATION_ID, geom FROM ids_damage_areas_cleaned 
                   WHERE REGION_ID = {region} AND SURVEY_YEAR = {year}")
    
    ids_batch <- st_read(ids_path, query = query, quiet = TRUE)
    ids_batch <- st_make_valid(ids_batch)
    n_features <- nrow(ids_batch)
    
    if (n_features == 0) {
      cat("no features, skipping\n")
      next
    }
    
    # Get centroids
    centroids <- st_point_on_surface(ids_batch)
    
    coords <- st_coordinates(centroids)
    valid_coords <- !is.na(coords[,1]) & !is.na(coords[,2])
    
    if (!all(valid_coords)) {
      cat(glue("Removing {sum(!valid_coords)} features with invalid coordinates... "))
      centroids <- centroids[valid_coords, ]
      ids_batch <- ids_batch[valid_coords, ]
      n_features <- nrow(ids_batch)
    }
    
    # Get TerraClimate annual image
    tc_annual <- get_terraclimate_annual(year, all_vars, ee)
    
    # Extract in sub-batches if needed (GEE limit ~5000)
    batch_size <- 5000
    n_sub_batches <- ceiling(n_features / batch_size)
    
    results <- list()
    
    for (j in seq_len(n_sub_batches)) {
      start_idx <- (j - 1) * batch_size + 1
      end_idx <- min(j * batch_size, n_features)
      
      sub_centroids <- centroids[start_idx:end_idx, ]
      centroids_ee <- sf_points_to_ee(sub_centroids, "OBSERVATION_ID", ee)
      
      sub_result <- extract_at_points(tc_annual, centroids_ee, scale = 4000, ee)
      results[[j]] <- sub_result
    }
    
    # Combine results
    batch_result <- bind_rows(results)
    batch_result$REGION_ID <- region
    batch_result$SURVEY_YEAR <- year
    
    # Save to CSV
    output_file <- file.path(output_dir, glue("tc_r{region}_{year}.csv"))
    write_csv(batch_result, output_file)
    
    t_end <- Sys.time()
    elapsed <- as.numeric(difftime(t_end, t_start, units = "secs"))
    
    total_features <- total_features + n_features
    total_time <- total_time + elapsed
    
    cat(glue("{n_features} features, {round(elapsed, 1)}s\n"))
    
  }, error = function(e) {
    cat(glue("ERROR: {e$message}\n"))
    errors[[length(errors) + 1]] <- list(region = region, year = year, error = e$message)
  })
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

cat(glue("\nOutput files: {output_dir}/tc_r*_*.csv\n"))
cat("================================================================================\n")