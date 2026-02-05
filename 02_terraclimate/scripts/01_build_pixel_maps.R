# ==============================================================================
# 02_terraclimate/scripts/01_build_pixel_maps.R
# Build pixel maps linking IDS observations to TerraClimate raster pixels
# ==============================================================================

library(here)
library(yaml)
library(terra)
library(sf)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config <- load_config()
tc_config <- config$raw$terraclimate
ids_config <- config$processed$ids

# Paths
ids_path <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
ref_raster_path <- here(tc_config$local_dir, "terraclimate_reference.tif")
pixel_map_dir <- here(tc_config$output_dir, "pixel_maps")

cat("TerraClimate Pixel Map Builder\n")
cat("================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Get reference raster from GEE
# ------------------------------------------------------------------------------

cat("Step 1: Getting reference raster from GEE...\n")

if (!file.exists(ref_raster_path)) {
  ee <- init_gee()

  ref_raster <- get_reference_raster_from_gee(
    gee_asset = tc_config$gee_asset,
    output_path = ref_raster_path,
    ee = ee,
    band = "tmmx",  # Any band works, just need the grid
    scale = tc_config$gee_scale
  )

  cat(sprintf("  Downloaded reference raster: %s\n", ref_raster_path))
} else {
  ref_raster <- rast(ref_raster_path)
  cat(sprintf("  Using existing reference raster: %s\n", ref_raster_path))
}

cat(sprintf("  Resolution: %.4f x %.4f degrees\n", res(ref_raster)[1], res(ref_raster)[2]))
cat(sprintf("  Dimensions: %d x %d pixels\n", ncol(ref_raster), nrow(ref_raster)))

# ------------------------------------------------------------------------------
# Step 2: Build pixel maps for each IDS layer
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel maps for IDS layers...\n")

pixel_maps <- build_ids_pixel_maps(
  ids_path = ids_path,
  reference_raster = ref_raster,
  output_dir = pixel_map_dir,
  layers = c("damage_areas", "damage_points", "surveyed_areas"),
  conus_only = FALSE  # TerraClimate has global coverage
)

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n================================\n")
cat("Summary:\n")
for (layer in names(pixel_maps)) {
  pm <- pixel_maps[[layer]]
  n_obs <- length(unique(pm[[1]]))  # First column is the ID
  n_pixels <- length(unique(pm$pixel_id))
  cat(sprintf("  %s: %d observations -> %d unique pixels\n", layer, n_obs, n_pixels))
}

cat("\nPixel maps saved to:\n")
cat(sprintf("  %s\n", pixel_map_dir))
cat("\nNext: Run 02_extract_terraclimate.R to extract climate values\n")
