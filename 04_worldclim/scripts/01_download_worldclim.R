# ==============================================================================
# 04_worldclim/scripts/01_download_worldclim.R
# Download WorldClim historical monthly weather data (GeoTIFF)
# ==============================================================================

library(here)
library(yaml)

source(here("scripts/utils/load_config.R"))

# Load config
config <- load_config()
wc_config <- config$raw$worldclim

# Paths
output_dir <- here(wc_config$local_dir)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("WorldClim Monthly Weather Downloader\n")
cat("====================================\n\n")

cat(sprintf("Source: %s\n", wc_config$source))
cat(sprintf("Resolution: %s\n", wc_config$spatial_resolution))
cat(sprintf("Coverage: %s\n\n", wc_config$coverage))

# ------------------------------------------------------------------------------
# Download decade files for each variable
# ------------------------------------------------------------------------------

variables <- names(wc_config$variables)
decades <- wc_config$decades
url_pattern <- wc_config$download_url_pattern

total_files <- length(variables) * length(decades)
downloaded <- 0
skipped <- 0

cat(sprintf("Downloading %d files (%d variables x %d decades)...\n\n",
            total_files, length(variables), length(decades)))

for (var in variables) {
  var_dir <- file.path(output_dir, var)
  dir.create(var_dir, recursive = TRUE, showWarnings = FALSE)

  for (decade in decades) {
    # Build URL and output path
    url <- gsub("\\{variable\\}", var, url_pattern)
    url <- gsub("\\{decade\\}", decade, url)

    zip_file <- file.path(var_dir, paste0(var, "_", decade, ".zip"))
    tif_pattern <- file.path(var_dir, paste0("wc2.1_2.5m_", var, "_", gsub("-", "_", decade), "*.tif"))

    # Check if already extracted
    existing_tifs <- Sys.glob(tif_pattern)
    if (length(existing_tifs) > 0) {
      cat(sprintf("  %s %s: already extracted, skipping\n", var, decade))
      skipped <- skipped + 1
      next
    }

    # Download if needed
    if (!file.exists(zip_file)) {
      cat(sprintf("  %s %s: downloading...", var, decade))
      tryCatch({
        download.file(url, zip_file, mode = "wb", quiet = TRUE)
        cat(" done\n")
      }, error = function(e) {
        cat(sprintf(" ERROR: %s\n", e$message))
        if (file.exists(zip_file)) file.remove(zip_file)
      })
    }

    # Extract
    if (file.exists(zip_file)) {
      cat(sprintf("  %s %s: extracting...", var, decade))
      tryCatch({
        unzip(zip_file, exdir = var_dir, overwrite = FALSE)
        file.remove(zip_file)  # Clean up zip after extraction
        cat(" done\n")
        downloaded <- downloaded + 1
      }, error = function(e) {
        cat(sprintf(" ERROR: %s\n", e$message))
      })
    }
  }
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

cat("\n====================================\n")
cat("Download Summary:\n")
cat(sprintf("  Downloaded and extracted: %d\n", downloaded))
cat(sprintf("  Already existed: %d\n", skipped))
cat(sprintf("  Output directory: %s\n", output_dir))

# List downloaded files
for (var in variables) {
  var_dir <- file.path(output_dir, var)
  tifs <- list.files(var_dir, pattern = "\\.tif$")
  cat(sprintf("\n  %s: %d GeoTIFF files\n", var, length(tifs)))
}

cat("\nNext: Run 02_build_pixel_maps.R to build pixel maps\n")
cat("      Then 03_extract_worldclim.R to extract values\n")
