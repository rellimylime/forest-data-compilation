# ==============================================================================
# 03_prism/scripts/01_build_pixel_maps.R
# Build pixel maps linking IDS observations to PRISM 800m raster pixels
# CONUS only (excludes Alaska and Hawaii)
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
prism_config <- config$raw$prism
ids_config <- config$processed$ids

# Paths
ids_path      <- here(ids_config$local_dir, ids_config$files$cleaned$filename)
ref_raster_path <- here(prism_config$local_dir, "prism_reference.tif")
pixel_map_dir <- here(prism_config$output_dir, "pixel_maps")

cat("PRISM Pixel Map Builder (800m, CONUS only)\n")
cat("==========================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Get reference raster from PRISM web service
# ------------------------------------------------------------------------------

cat("Step 1: Getting reference raster from PRISM web service...\n")

if (!file.exists(ref_raster_path)) {

  dir.create(dirname(ref_raster_path), recursive = TRUE, showWarnings = FALSE)

  # Download one monthly raster to define the pixel grid.
  # Any stable month/variable works — ppt Jan 2000 is well-QC'd.
  ref_var  <- "ppt"
  ref_date <- "200001"
  url <- sprintf("https://services.nacse.org/prism/data/get/us/800m/%s/%s",
                 ref_var, ref_date)

  cat(sprintf("  Downloading: %s\n", url))

  zip_path    <- tempfile(fileext = ".zip")
  extract_dir <- file.path(tempdir(), paste0("prism_ref_", Sys.getpid()))
  dir.create(extract_dir, showWarnings = FALSE)

  tryCatch({

    download.file(url, zip_path, mode = "wb", quiet = FALSE)
    unzip(zip_path, exdir = extract_dir)

    raster_files <- list.files(extract_dir,
                               pattern = "\\.(tif|bil)$",
                               full.names = TRUE,
                               ignore.case = TRUE)

    if (length(raster_files) == 0) stop("No raster file found in downloaded zip")

    r <- rast(raster_files[1])
    writeRaster(r, ref_raster_path, overwrite = TRUE)
    cat(sprintf("  Saved reference raster: %s\n", ref_raster_path))

  }, finally = {
    unlink(zip_path)
    unlink(extract_dir, recursive = TRUE)
  })

} else {
  cat(sprintf("  Using existing reference raster: %s\n", ref_raster_path))
}

ref_raster <- rast(ref_raster_path)
cat(sprintf("  Resolution:  %.6f x %.6f degrees (~800m)\n", res(ref_raster)[1], res(ref_raster)[2]))
cat(sprintf("  Dimensions:  %d cols x %d rows = %d pixels\n",
            ncol(ref_raster), nrow(ref_raster), ncell(ref_raster)))
e <- as.vector(ext(ref_raster))
cat(sprintf("  Extent     : lon [%.3f, %.3f], lat [%.3f, %.3f]\n",
            e[1], e[2], e[3], e[4]))
cat(sprintf("  CRS        : %s\n", crs(ref_raster, proj = TRUE)))

# ------------------------------------------------------------------------------
# Step 2: Build pixel maps for each IDS layer (CONUS only)
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel maps for IDS layers (CONUS only)...\n")
cat("  Note: Alaska (R10) and Hawaii observations will be excluded\n\n")

pixel_maps <- build_ids_pixel_maps(
  ids_path         = ids_path,
  reference_raster = ref_raster,
  output_dir       = pixel_map_dir,
  layers           = c("damage_areas", "damage_points", "surveyed_areas"),
  conus_only       = TRUE
)

# ------------------------------------------------------------------------------
# Summary and validation
# ------------------------------------------------------------------------------

total_unique_pixels <- dplyr::bind_rows(pixel_maps) |>
  dplyr::distinct(pixel_id) |>
  nrow()

cat("\n==========================================\n")
cat("Validation Summary:\n\n")

for (layer in names(pixel_maps)) {
  pm     <- pixel_maps[[layer]]
  id_col <- names(pm)[1]

  n_obs    <- length(unique(pm[[id_col]]))
  n_pixels <- length(unique(pm$pixel_id))

  # Pixels per observation
  ppo <- pm |>
    dplyr::group_by(dplyr::across(dplyr::all_of(id_col))) |>
    dplyr::summarise(n = dplyr::n(), .groups = "drop") |>
    dplyr::pull(n)

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

  # Flag any NA pixel_ids (outside raster extent)
  n_na <- sum(is.na(pm$pixel_id))
  if (n_na > 0) {
    cat(sprintf("    WARNING       : %d rows with NA pixel_id\n", n_na))
  }

  cat("\n")
}

cat(sprintf("Total unique pixels (all layers) : %d\n", total_unique_pixels))
cat("Note: At 800m, expect ~25x more pixels than TerraClimate (4km)\n")
cat("\nPixel maps saved to:\n")
cat(sprintf("  %s\n", pixel_map_dir))
cat("\nNext: Run 02_extract_prism.R to extract climate values\n")
