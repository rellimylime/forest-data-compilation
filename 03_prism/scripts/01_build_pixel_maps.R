# ==============================================================================
# 03_prism/scripts/01_build_pixel_maps.R
# Build pixel maps linking IDS observations to PRISM 800m raster pixels
# CONUS only (excludes Alaska and Hawaii)
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
prism_config <- config$raw$prism
ids_config <- config$processed$ids

# Paths
ids_path <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
ref_raster_path <- here(prism_config$local_dir, "prism_reference.tif")
pixel_map_dir <- here(prism_config$output_dir, "pixel_maps")

cat("PRISM Pixel Map Builder (800m, CONUS only)\n")
cat("==========================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Get reference raster from GEE
# ------------------------------------------------------------------------------

cat("Step 1: Getting reference raster from GEE...\n")

if (!file.exists(ref_raster_path)) {
  ee <- init_gee()

  # PRISM covers CONUS only - define region
  conus_region <- ee$Geometry$Rectangle(c(-125, 24, -66, 50))

  ref_raster <- get_reference_raster_from_gee(
    gee_asset = prism_config$gee_asset,
    output_path = ref_raster_path,
    ee = ee,
    band = "ppt",  # Any band works
    scale = prism_config$gee_scale,
    region = conus_region
  )

  cat(sprintf("  Downloaded reference raster: %s\n", ref_raster_path))
} else {
  ref_raster <- rast(ref_raster_path)
  cat(sprintf("  Using existing reference raster: %s\n", ref_raster_path))
}

cat(sprintf("  Resolution: %.6f x %.6f degrees (~800m)\n", res(ref_raster)[1], res(ref_raster)[2]))
cat(sprintf("  Dimensions: %d x %d pixels\n", ncol(ref_raster), nrow(ref_raster)))

# ------------------------------------------------------------------------------
# Step 2: Build pixel maps for each IDS layer (CONUS only)
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel maps for IDS layers (CONUS only)...\n")
cat("  Note: Alaska (R10) and Hawaii observations will be excluded\n\n")

pixel_maps <- build_ids_pixel_maps(
  ids_path = ids_path,
  reference_raster = ref_raster,
  output_dir = pixel_map_dir,
  layers = c("damage_areas", "damage_points", "surveyed_areas"),
  conus_only = TRUE  # Filter to CONUS only
)

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n==========================================\n")
cat("Summary:\n")
for (layer in names(pixel_maps)) {
  pm <- pixel_maps[[layer]]
  n_obs <- length(unique(pm[[1]]))
  n_pixels <- length(unique(pm$pixel_id))
  cat(sprintf("  %s: %d observations -> %d unique pixels\n", layer, n_obs, n_pixels))
}

cat("\nNote: At 800m resolution, expect many more pixels than TerraClimate (4km)\n")
cat("\nPixel maps saved to:\n")
cat(sprintf("  %s\n", pixel_map_dir))
cat("\nNext: Run 02_extract_prism.R to extract climate values\n")
