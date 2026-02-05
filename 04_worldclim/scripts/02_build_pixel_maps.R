# ==============================================================================
# 04_worldclim/scripts/02_build_pixel_maps.R
# Build pixel maps linking IDS observations to WorldClim ~4.5km raster pixels
# ==============================================================================

library(here)
library(yaml)
library(terra)
library(sf)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
wc_config <- config$raw$worldclim
ids_config <- config$processed$ids

# Paths
ids_path <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
raw_dir <- here(wc_config$local_dir)
pixel_map_dir <- here(wc_config$output_dir, "pixel_maps")

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

cat(sprintf("  Reference file: %s\n", basename(tif_files[1])))
cat(sprintf("  Resolution: %.4f x %.4f degrees (~4.5km)\n", res(ref_raster)[1], res(ref_raster)[2]))
cat(sprintf("  Extent: %.2f to %.2f lon, %.2f to %.2f lat\n",
            ext(ref_raster)[1], ext(ref_raster)[2], ext(ref_raster)[3], ext(ref_raster)[4]))

# ------------------------------------------------------------------------------
# Step 2: Build pixel maps for each IDS layer
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel maps for IDS layers...\n")

pixel_maps <- build_ids_pixel_maps(
  ids_path = ids_path,
  reference_raster = ref_raster,
  output_dir = pixel_map_dir,
  layers = c("damage_areas", "damage_points", "surveyed_areas"),
  conus_only = FALSE  # WorldClim has global coverage
)

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n===========================\n")
cat("Summary:\n")
for (layer in names(pixel_maps)) {
  pm <- pixel_maps[[layer]]
  n_obs <- length(unique(pm[[1]]))
  n_pixels <- length(unique(pm$pixel_id))
  cat(sprintf("  %s: %d observations -> %d unique pixels\n", layer, n_obs, n_pixels))
}

cat("\nPixel maps saved to:\n")
cat(sprintf("  %s\n", pixel_map_dir))
cat("\nNext: Run 03_extract_worldclim.R to extract climate values\n")
