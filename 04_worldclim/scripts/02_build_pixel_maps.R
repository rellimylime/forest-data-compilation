# ==============================================================================
# 04_worldclim/scripts/02_build_pixel_maps.R
# Build pixel maps linking IDS observations to WorldClim ~4.5km raster pixels
#
# Re-run safe: skips any layer whose parquet already exists in pixel_maps/.
# Progress bars from exactextractr show polygon-pixel mapping progress.
# ==============================================================================

library(here)
library(yaml)
library(terra)
library(sf)
library(dplyr)
library(arrow)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
wc_config  <- config$raw$worldclim
ids_config <- config$processed$ids

# Paths
ids_path      <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
raw_dir       <- here(wc_config$local_dir)
pixel_map_dir <- here(wc_config$output_dir, "pixel_maps")

t_total_start <- Sys.time()

cat("WorldClim Pixel Map Builder\n")
cat("===========================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Load reference raster from downloaded WorldClim data
# ------------------------------------------------------------------------------

cat("Step 1: Loading reference raster...\n")

# Find any WorldClim GeoTIFF to use as reference
tif_files <- list.files(raw_dir, pattern = "\\.tif$", recursive = TRUE, full.names = TRUE)

if (length(tif_files) == 0) {
  stop("No WorldClim GeoTIFF files found. Run 01_download_worldclim.R first.")
}

ref_raster <- rast(tif_files[1])
# Take just first band for reference grid
ref_raster <- ref_raster[[1]]

cat(sprintf("  Reference file : %s\n", basename(tif_files[1])))
cat(sprintf("  Resolution     : %.4f x %.4f degrees (~4.5km)\n",
            res(ref_raster)[1], res(ref_raster)[2]))
cat(sprintf("  Dimensions     : %d cols x %d rows = %d pixels\n",
            ncol(ref_raster), nrow(ref_raster), ncell(ref_raster)))
e <- as.vector(ext(ref_raster))
cat(sprintf("  Extent         : lon [%.2f, %.2f], lat [%.2f, %.2f]\n",
            e[1], e[2], e[3], e[4]))

# ------------------------------------------------------------------------------
# Step 2: Build pixel maps for each IDS layer
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel maps for IDS layers...\n")
cat("  (Existing layers will be skipped; delete parquet files to rebuild)\n\n")

pixel_maps <- build_ids_pixel_maps(
  ids_path         = ids_path,
  reference_raster = ref_raster,
  output_dir       = pixel_map_dir,
  layers           = c("damage_areas", "damage_points", "surveyed_areas"),
  conus_only       = FALSE  # WorldClim has global coverage
)

# ------------------------------------------------------------------------------
# Validation summary
# ------------------------------------------------------------------------------

total_unique_pixels <- bind_rows(pixel_maps) |>
  distinct(pixel_id) |>
  nrow()

t_elapsed <- as.numeric(difftime(Sys.time(), t_total_start, units = "mins"))

cat("\n===========================\n")
cat("Validation Summary:\n\n")

for (layer in names(pixel_maps)) {
  pm     <- pixel_maps[[layer]]
  id_col <- names(pm)[1]

  n_obs    <- length(unique(pm[[id_col]]))
  n_pixels <- length(unique(pm$pixel_id))

  ppo <- pm |>
    group_by(across(all_of(id_col))) |>
    summarise(n = n(), .groups = "drop") |>
    pull(n)

  cat(sprintf("  %s:\n", layer))
  cat(sprintf("    Observations  : %d\n", n_obs))
  cat(sprintf("    Unique pixels : %d\n", n_pixels))
  cat(sprintf("    Pixels/obs    : min=%.0f  mean=%.1f  max=%.0f\n",
              min(ppo), mean(ppo), max(ppo)))

  if ("coverage_fraction" %in% names(pm)) {
    cf <- pm$coverage_fraction
    cat(sprintf("    Coverage frac : min=%.3f  mean=%.3f  max=%.3f\n",
                min(cf, na.rm = TRUE),
                mean(cf, na.rm = TRUE),
                max(cf, na.rm = TRUE)))
  }

  if ("DAMAGE_AREA_ID" %in% names(pm)) {
    n_geoms <- length(unique(pm$DAMAGE_AREA_ID))
    cat(sprintf("    Unique geoms  : %d  (%.1fx obs, pancake features)\n",
                n_geoms, n_obs / n_geoms))
  }

  n_na <- sum(is.na(pm$pixel_id))
  if (n_na > 0) {
    cat(sprintf("    WARNING       : %d rows with NA pixel_id\n", n_na))
  }

  cat("\n")
}

cat(sprintf("Total unique pixels (all layers) : %d\n", total_unique_pixels))
cat(sprintf("Total elapsed                    : %.1f min\n", t_elapsed))
cat("\nPixel maps saved to:\n")
cat(sprintf("  %s\n", pixel_map_dir))
cat("\nNext: Run 03_extract_worldclim.R to extract climate values\n")
