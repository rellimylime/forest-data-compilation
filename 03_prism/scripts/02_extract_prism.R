# ==============================================================================
# 03_prism/scripts/02_extract_prism.R
# Extract PRISM 800m climate values via direct web service download
#
# Strategy: download-extract-delete
#   For each year x month x variable:
#     1. Download zip from services.nacse.org
#     2. terra::extract() at unique pixel coordinates
#     3. Delete the zip and unzipped files immediately
#   Results are saved as one parquet per year (same schema as TerraClimate).
#
# Rate limits: PRISM allows each unique file to be downloaded twice per 24h.
#   Since we download each file only once, this is not a constraint.
#   A 0.5s courtesy delay is added between requests.
#
# Runtime estimate: ~20-40 hours depending on server speed and pixel count.
#   Safe to interrupt and resume; completed years are skipped automatically.
# ==============================================================================

library(here)
library(yaml)
library(dplyr)
library(arrow)
library(terra)
library(progress)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))

# Load config
config       <- load_config()
prism_config <- config$raw$prism
time_config  <- config$params$time_range

# Paths
pixel_map_dir <- here(prism_config$output_dir, "pixel_maps")
output_dir    <- here(prism_config$output_dir, "pixel_values")

cat("PRISM Extraction (800m, direct web service)\n")
cat("============================================\n\n")

# ------------------------------------------------------------------------------
# Step 1: Load pixel maps, get unique pixel coordinates
# ------------------------------------------------------------------------------

cat("Step 1: Loading pixel maps...\n")

all_pixels <- list()
for (layer in c("damage_areas", "damage_points", "surveyed_areas")) {
  pm_file <- file.path(pixel_map_dir, paste0(layer, "_pixel_map.parquet"))
  if (file.exists(pm_file)) {
    pm <- read_parquet(pm_file)
    all_pixels[[layer]] <- get_unique_pixels(pm)
    cat(sprintf("  %s: %d unique pixels\n", layer, nrow(all_pixels[[layer]])))
  } else {
    cat(sprintf("  %s: pixel map not found, skipping\n", layer))
  }
}

if (length(all_pixels) == 0) stop("No pixel maps found. Run 01_build_pixel_maps.R first.")

pixel_coords <- bind_rows(all_pixels) %>%
  distinct(pixel_id, x, y)

n_pixels <- nrow(pixel_coords)
cat(sprintf("\nTotal unique pixels across all layers: %d\n", n_pixels))

# Coordinate matrix passed to terra::extract() — row order matches pixel_coords
coords_matrix <- as.matrix(pixel_coords[, c("x", "y")])

# ------------------------------------------------------------------------------
# Step 2: Extract climate values (download -> extract -> delete)
# ------------------------------------------------------------------------------

variables <- names(prism_config$variables)
years     <- time_config$start_year:time_config$end_year

n_downloads <- length(years) * 12L * length(variables)

cat("\nStep 2: Extracting PRISM values...\n")
cat(sprintf("  Variables : %s\n", paste(variables, collapse = ", ")))
cat(sprintf("  Years     : %d-%d (%d years)\n", min(years), max(years), length(years)))
cat(sprintf("  Pixels    : %d\n", n_pixels))
cat(sprintf("  Downloads : %d total\n\n", n_downloads))

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Progress bar: counts pending downloads only (skips already-done years)
n_pending <- sum(!file.exists(file.path(
  output_dir,
  sprintf("%s_%d.parquet", prism_config$output_prefix, years)
))) * 12L * length(variables)

pb <- progress_bar$new(
  format = paste0("  [:bar] :current/:total (:percent)",
                  " | elapsed: :elapsed | eta: :eta"),
  total  = max(1L, n_pending),
  clear  = FALSE,
  width  = 72
)
n_success_total <- 0L
n_fail_total    <- 0L

for (year in years) {

  output_file <- file.path(output_dir,
                           sprintf("%s_%d.parquet", prism_config$output_prefix, year))

  if (file.exists(output_file)) {
    cat(sprintf("  %d: exists, skipping\n", year))
    next
  }

  cat(sprintf("  %d:\n", year))
  t_year_start <- Sys.time()

  month_results <- vector("list", 12)

  for (month in 1:12) {

    date_str <- sprintf("%d%02d", year, month)

    # Start with pixel IDs and time columns for this month
    month_df <- data.frame(
      pixel_id = pixel_coords$pixel_id,
      x        = pixel_coords$x,
      y        = pixel_coords$y,
      year     = year,
      month    = month
    )

    var_status <- character(length(variables))
    names(var_status) <- variables

    for (var in variables) {

      url         <- sprintf("https://services.nacse.org/prism/data/get/us/800m/%s/%s",
                             var, date_str)
      zip_path    <- tempfile(fileext = ".zip")
      extract_dir <- file.path(tempdir(),
                               paste0("prism_", var, "_", date_str, "_", Sys.getpid()))
      dir.create(extract_dir, showWarnings = FALSE)

      vals <- tryCatch({

        download.file(url, zip_path, mode = "wb", quiet = TRUE)
        unzip(zip_path, exdir = extract_dir, overwrite = TRUE)

        raster_files <- list.files(extract_dir,
                                   pattern = "\\.(tif|bil)$",
                                   full.names = TRUE,
                                   ignore.case = TRUE)

        if (length(raster_files) == 0) stop("no raster file in zip")

        r <- rast(raster_files[1])

        # terra::extract with a coordinate matrix returns a 1-column data.frame
        # (no ID column — that's only added for SpatVector/sf input)
        terra::extract(r, coords_matrix)[[1L]]

      }, error = function(e) {
        cat(sprintf("    WARN: %s %s failed: %s\n", var, date_str, conditionMessage(e)))
        rep(NA_real_, n_pixels)
      }, finally = {
        unlink(zip_path)
        unlink(extract_dir, recursive = TRUE)
      })

      month_df[[var]] <- vals
      if (all(is.na(vals))) {
        var_status[var]  <- "FAIL"
        n_fail_total     <- n_fail_total + 1L
      } else {
        var_status[var]  <- "ok"
        n_success_total  <- n_success_total + 1L
      }

      Sys.sleep(0.5)  # Courtesy delay between requests
      pb$tick()
    }

    month_results[[month]] <- month_df

    status_str <- paste(sprintf("%s:%s", variables, var_status), collapse = "  ")
    cat(sprintf("    %02d  %s\n", month, status_str))
  }

  year_data <- bind_rows(month_results)
  write_parquet(year_data, output_file)

  t_elapsed <- as.numeric(difftime(Sys.time(), t_year_start, units = "mins"))
  cat(sprintf("  -> %d rows saved (%.1f min)\n\n", nrow(year_data), t_elapsed))
}

# ------------------------------------------------------------------------------
# Summary and validation
# ------------------------------------------------------------------------------

cat("\n============================================\n")
cat("Extraction complete!\n\n")

output_files <- list.files(output_dir, pattern = "\\.parquet$",
                           full.names = TRUE)
cat(sprintf("Years complete : %d of %d\n",
            length(output_files), length(years)))

if (n_success_total + n_fail_total > 0) {
  cat(sprintf(
    "Downloads      : %d ok, %d failed (%.1f%% success rate)\n",
    n_success_total, n_fail_total,
    100 * n_success_total / (n_success_total + n_fail_total)
  ))
} else {
  cat("Downloads      : none (all years already complete)\n")
}
cat(sprintf("Output dir     : %s\n\n", output_dir))

if (length(output_files) > 0) {
  # Value ranges from the most recently completed year
  sample_file <- tail(sort(output_files), 1)
  sample_data <- read_parquet(sample_file)
  sample_yr   <- sub(".*_(\\d{4})\\.parquet$", "\\1",
                     basename(sample_file))

  cat(sprintf("Value ranges (%s — %d pixels x 12 months):\n",
              sample_yr, n_pixels))
  cat(sprintf("  %-8s  %8s  %8s  %6s\n",
              "variable", "min", "max", "NA%"))

  for (var in variables) {
    if (var %in% names(sample_data)) {
      v      <- sample_data[[var]]
      na_pct <- 100 * mean(is.na(v))
      if (na_pct < 100) {
        cat(sprintf("  %-8s  %8.2f  %8.2f  %5.1f%%\n",
                    var, min(v, na.rm = TRUE),
                    max(v, na.rm = TRUE), na_pct))
      } else {
        cat(sprintf("  %-8s  %8s  %8s  %5.1f%%  WARNING: all NA\n",
                    var, "--", "--", na_pct))
      }
    }
  }

  sizes_mb <- file.size(output_files) / 1e6
  cat(sprintf(
    "\nFile sizes: %.1f-%.1f MB/year (%.1f MB total)\n",
    min(sizes_mb), max(sizes_mb), sum(sizes_mb)
  ))
}

cat("\nNext: Use join_to_observations() to link pixel values to IDS\n")
