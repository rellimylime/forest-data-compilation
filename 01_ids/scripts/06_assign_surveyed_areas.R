# ==============================================================================
# 01_ids/scripts/06_assign_surveyed_areas.R
# Spatially assign each DAMAGE_AREA_ID to its best-matching SURVEYED_AREA_ID
#
# Method: polygon intersection with max-overlap selection.
# Processed in chunks to handle millions of features.
#
# Output: processed/ids/damage_area_to_surveyed_area.parquet
#   DAMAGE_AREA_ID, SURVEYED_AREA_ID, overlap_m2, match_quality_flag
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))

config <- load_config()
ids_config <- config$processed$ids

# Paths
ids_path <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
output_dir <- here("processed/ids")
output_file <- file.path(output_dir, "damage_area_to_surveyed_area.parquet")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("Damage Area -> Surveyed Area Assignment\n")
cat("========================================\n\n")

if (file.exists(output_file)) {
  cat("Output already exists:", output_file, "\n")
  cat("Delete to rerun.\n")
  quit(save = "no")
}

# ------------------------------------------------------------------------------
# Step 1: Load IDS layers
# ------------------------------------------------------------------------------

cat("Step 1: Loading IDS layers...\n")

damage_areas <- st_read(ids_path, layer = "damage_areas", quiet = TRUE)
surveyed_areas <- st_read(ids_path, layer = "surveyed_areas", quiet = TRUE)

# Get unique damage geometries (one row per DAMAGE_AREA_ID)
damage_unique <- damage_areas %>%
  group_by(DAMAGE_AREA_ID) %>%
  slice(1) %>%
  ungroup() %>%
  select(DAMAGE_AREA_ID, SURVEY_YEAR)

cat(sprintf("  Damage areas: %d unique geometries (from %d observations)\n",
            nrow(damage_unique), nrow(damage_areas)))
cat(sprintf("  Surveyed areas: %d features\n", nrow(surveyed_areas)))

rm(damage_areas)
gc()

# ------------------------------------------------------------------------------
# Step 2: Transform to EPSG:5070 for area calculations
# ------------------------------------------------------------------------------

cat("\nStep 2: Transforming to EPSG:5070 (Conus Albers) for area calculation...\n")

damage_unique <- st_transform(damage_unique, 5070)
surveyed_areas <- st_transform(surveyed_areas, 5070)

cat("  CRS transformed\n")

# ------------------------------------------------------------------------------
# Step 3: Spatial assignment in chunks
# ------------------------------------------------------------------------------

cat("\nStep 3: Assigning damage areas to surveyed areas (chunked)...\n")

chunk_size <- 10000
n_damage <- nrow(damage_unique)
n_chunks <- ceiling(n_damage / chunk_size)

cat(sprintf("  Processing %d damage geometries in %d chunks of %d\n",
            n_damage, n_chunks, chunk_size))

# Pre-build spatial index on surveyed areas
surveyed_areas <- surveyed_areas %>%
  select(SURVEYED_AREA_ID = SURVEY_FEATURE_ID, SURVEY_YEAR)

results <- list()

for (i in seq_len(n_chunks)) {
  start_idx <- (i - 1) * chunk_size + 1
  end_idx <- min(i * chunk_size, n_damage)
  chunk <- damage_unique[start_idx:end_idx, ]

  # Find intersections between damage chunk and surveyed areas
  # Filter surveyed areas to matching survey years for efficiency
  chunk_years <- unique(chunk$SURVEY_YEAR)
  survey_subset <- surveyed_areas[surveyed_areas$SURVEY_YEAR %in% chunk_years, ]

  if (nrow(survey_subset) == 0) {
    cat(sprintf("  Chunk %d/%d: no matching surveyed areas for years, skipping\n", i, n_chunks))
    next
  }

  # Compute intersection
  isect <- tryCatch(
    st_intersection(chunk, survey_subset),
    error = function(e) {
      cat(sprintf("  Chunk %d/%d: intersection error - %s\n", i, n_chunks, e$message))
      NULL
    }
  )

  if (is.null(isect) || nrow(isect) == 0) {
    cat(sprintf("  Chunk %d/%d: no intersections found\n", i, n_chunks))
    next
  }

  # Compute overlap area
  isect$overlap_m2 <- as.numeric(st_area(isect))

  # Pick best match (max overlap) per DAMAGE_AREA_ID
  chunk_result <- isect %>%
    st_drop_geometry() %>%
    group_by(DAMAGE_AREA_ID) %>%
    slice_max(overlap_m2, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    select(DAMAGE_AREA_ID, SURVEYED_AREA_ID, overlap_m2)

  results[[i]] <- chunk_result

  if (i %% 10 == 0 || i == n_chunks) {
    cat(sprintf("  Chunk %d/%d: %d assignments\n", i, n_chunks, nrow(chunk_result)))
  }
}

# Combine results
assignments <- bind_rows(results)

# ------------------------------------------------------------------------------
# Step 4: Add match quality flag
# ------------------------------------------------------------------------------

cat("\nStep 4: Adding match quality flags...\n")

# Determine which damage areas had no match
all_damage_ids <- damage_unique$DAMAGE_AREA_ID
matched_ids <- assignments$DAMAGE_AREA_ID
unmatched_ids <- setdiff(all_damage_ids, matched_ids)

# Add unmatched rows
if (length(unmatched_ids) > 0) {
  unmatched_df <- data.frame(
    DAMAGE_AREA_ID = unmatched_ids,
    SURVEYED_AREA_ID = NA_character_,
    overlap_m2 = NA_real_
  )
  assignments <- bind_rows(assignments, unmatched_df)
}

# Quality flag:
#   "matched"      - intersection found
#   "no_survey"    - no surveyed area intersects this damage area
assignments$match_quality_flag <- ifelse(
  is.na(assignments$SURVEYED_AREA_ID),
  "no_survey",
  "matched"
)

cat(sprintf("  Matched: %d\n", sum(assignments$match_quality_flag == "matched")))
cat(sprintf("  No survey polygon: %d\n", sum(assignments$match_quality_flag == "no_survey")))

# ------------------------------------------------------------------------------
# Step 5: Save output
# ------------------------------------------------------------------------------

cat("\nStep 5: Saving output...\n")

write_parquet(assignments, output_file)

cat(sprintf("  Saved %d rows to %s\n", nrow(assignments), output_file))
cat(sprintf("  Columns: %s\n", paste(names(assignments), collapse = ", ")))

cat("\nDone.\n")
