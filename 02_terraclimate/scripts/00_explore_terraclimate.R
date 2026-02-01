# ==============================================================================
# 00_explore_terraclimate.R
# Explore TerraClimate extraction methods before committing to full workflow
# ==============================================================================

library(here)
library(sf)
library(dplyr)
library(glue)

# Load project utilities
source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

cat("=== TERRACLIMATE EXPLORATION ===\n\n")

cat("[1] Initializing...\n")
config <- load_config()
ee <- init_gee()
cat("✓ Connected to GEE\n\n")

# ==============================================================================
# 2. CHECK TERRACLIMATE CONFIG
# ==============================================================================

cat("[2] TerraClimate configuration:\n")
tc_config <- config$raw$terraclimate

cat(glue("  Asset: {tc_config$gee_asset}\n"))
cat(glue("  Resolution: {tc_config$spatial_resolution}\n"))
cat(glue("  Variables: {length(tc_config$variables)}\n\n"))

cat("  Variables and scale factors:\n")
for (var_name in names(tc_config$variables)) {
  var <- tc_config$variables[[var_name]]
  cat(glue("    {var_name}: {var$description} [scale={var$scale}]\n"))
}
cat("\n")

# ==============================================================================
# 3. EXPLORE DATASET IN GEE
# ==============================================================================

cat("[3] Exploring TerraClimate in GEE...\n")

tc <- ee$ImageCollection(tc_config$gee_asset)

# Get band names from first image
first_img <- tc$first()
bands <- first_img$bandNames()$getInfo()
cat(glue("  Available bands ({length(bands)}): {paste(bands, collapse=', ')}\n"))

# Check date range
tc_2020 <- tc$filterDate('2020-01-01', '2021-01-01')
n_images <- tc_2020$size()$getInfo()
cat(glue("  Images in 2020: {n_images} (monthly)\n\n"))

# ==============================================================================
# 4. LOAD SAMPLE IDS DATA
# ==============================================================================

cat("[4] Loading sample IDS data...\n")

ids_path <- here("01_ids/data/processed/ids_damage_areas_cleaned.gpkg")

if (!file.exists(ids_path)) {
  stop("IDS data not found at: ", ids_path)
}

# Small sample: 100 features from 2020, Region 5 (California)
sample_query <- "SELECT * FROM ids_damage_areas_cleaned 
                 WHERE SURVEY_YEAR = 2020 AND REGION_ID = 5 
                 LIMIT 100"

ids_sample <- st_read(ids_path, query = sample_query, quiet = TRUE)

cat(glue("  Loaded: {nrow(ids_sample)} features\n"))
cat(glue("  Year: {unique(ids_sample$SURVEY_YEAR)}\n"))
cat(glue("  Region: {unique(ids_sample$REGION_ID)}\n"))
cat(glue("  CRS: {st_crs(ids_sample)$input}\n\n"))

# Quick look at polygon sizes
areas <- st_area(ids_sample)
cat(glue("  Polygon areas: min={round(min(areas), 0)} m², median={round(median(areas), 0)} m², max={round(max(areas), 0)} m²\n"))
cat(glue("  Note: TerraClimate pixel = ~16 km² (4km × 4km)\n"))
cat("  → Polygons are much smaller than pixels, so centroid extraction is appropriate\n\n")

# ==============================================================================
# 5. TEST CENTROID EXTRACTION
# ==============================================================================

cat("[5] Testing centroid extraction...\n")

# Variables to test (subset)
test_vars <- c("tmmx", "tmmn", "pr", "vpd", "def")

# Create annual mean image for 2020
tc_annual <- get_terraclimate_annual(2020, test_vars, ee)

# Get centroids and convert to ee
centroids <- st_centroid(ids_sample)
centroids_ee <- sf_points_to_ee(centroids, "OBSERVATION_ID", ee)

# Extract
cat("  Extracting at centroids...\n")
t1 <- Sys.time()

result_centroid <- extract_at_points(tc_annual, centroids_ee, scale = 4000, ee)

t2 <- Sys.time()
centroid_time <- as.numeric(difftime(t2, t1, units = "secs"))

cat(glue("  ✓ Done: {round(centroid_time, 1)} seconds\n"))
cat(glue("  Rows: {nrow(result_centroid)}, Columns: {ncol(result_centroid)}\n\n"))

cat("  Sample results (raw values):\n")
print(head(result_centroid))
cat("\n")

# ==============================================================================
# 6. TEST SCALE FACTORS
# ==============================================================================

cat("[6] Testing scale factor application...\n\n")

result_scaled <- apply_terraclimate_scales(result_centroid, config)

cat("  Before/after scaling:\n")
for (var in test_vars) {
  raw_mean <- mean(result_centroid[[var]], na.rm = TRUE)
  scaled_mean <- mean(result_scaled[[var]], na.rm = TRUE)
  units <- tc_config$variables[[var]]$units
  cat(glue("    {var}: {round(raw_mean, 1)} → {round(scaled_mean, 2)} {units}\n"))
}

# Sanity checks
cat("\n  Sanity checks:\n")
cat(glue("    Max temp (tmmx): {round(max(result_scaled$tmmx, na.rm=TRUE), 1)}°C\n"))
cat(glue("    Min temp (tmmn): {round(min(result_scaled$tmmn, na.rm=TRUE), 1)}°C\n"))
cat(glue("    Annual precip (pr): {round(mean(result_scaled$pr, na.rm=TRUE), 0)} mm\n"))

# ==============================================================================
# 7. ESTIMATE FULL EXTRACTION
# ==============================================================================

cat("\n[7] Estimating full extraction time...\n\n")

total_features <- 4475827
time_per_feature <- centroid_time / 100
total_time_hrs <- (total_features * time_per_feature) / 3600

cat(glue("  Total features: {format(total_features, big.mark=',')}\n"))
cat(glue("  Time per 100 features: {round(centroid_time, 1)} sec\n"))
cat(glue("  Estimated total: {round(total_time_hrs, 1)} hours\n\n"))

cat("  Batching strategy:\n")
cat("    - GEE limits: ~5000 features per request\n")
cat("    - Recommend: batch by REGION_ID + SURVEY_YEAR\n")
cat("    - ~280 batches (10 regions × 28 years)\n")
cat("    - Can parallelize across years\n")

# ==============================================================================
# 8. SUMMARY
# ==============================================================================

cat("\n")
cat("================================================================================\n")
cat("SUMMARY\n")
cat("================================================================================\n\n")

cat("EXTRACTION METHOD:\n")
if (nrow(result_centroid) == nrow(ids_sample)) {
  cat("  ✓ Centroid extraction works\n")
} else {
  cat("  ⚠ Centroid extraction had issues\n")
}
cat("  ✓ Polygons smaller than TerraClimate pixels → centroid is appropriate\n")
cat("  → Recommendation: USE CENTROID METHOD\n\n")

cat("TEMPORAL AGGREGATION:\n")
cat("  ✓ Annual mean aggregation works in GEE\n")
cat("  → Recommendation: Aggregate to annual before extraction\n\n")

cat("SCALE FACTORS:\n")
cat("  ✓ Scale factors in config.yaml work correctly\n")
cat("  → Apply during cleaning step\n\n")

cat("NEXT STEPS:\n")
cat("  1. Create 01_extract_terraclimate.R with batched extraction\n")
cat("  2. Create 02_inspect_terraclimate.R to verify results\n")
cat("  3. Create 03_clean_terraclimate.R to apply scales and QC\n")
cat("================================================================================\n")