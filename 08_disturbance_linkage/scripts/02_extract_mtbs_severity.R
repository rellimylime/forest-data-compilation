# ==============================================================================
# 02_extract_mtbs_severity.R
# Summarize MTBS annual burn-severity mosaics within each plot footprint.
#
# SCAFFOLD: the zonal-stats structure is here; the parts that depend on the
# actual MTBS download are marked TODO. MTBS annual mosaics are thematic rasters
# (one per year) with pixel values 1-6 (see config mtbs.severity_classes).
#
# Output grain: one row per stable_plot_id x fire_year that intersects a footprint
#   plot_mtbs_fire_events.parquet
#   columns: stable_plot_id, fire_year, n_pixels_total, n_pixels_valid,
#            n_pixels_masked, frac_pixels_masked, frac_high_severity,
#            dominant_severity_class
#
# Input:  plot_footprints.gpkg (script 01); MTBS rasters in config mtbs_raw_dir
#
# Usage:
#   Rscript 08_disturbance_linkage/scripts/02_extract_mtbs_severity.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(terra)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/mtbs_severity.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
area_crs <- link_cfg$area_crs

out_dir <- here(link_cfg$output_dir)
footprints_path <- file.path(out_dir, link_cfg$files$plot_footprints)
mtbs_dir <- here(link_cfg$inputs$mtbs_raw_dir)
out_path <- file.path(out_dir, link_cfg$files$plot_mtbs_fire_events)

# Severity-class map and which classes count as high severity.
sev_map <- link_cfg$mtbs$severity_classes                 # named list "1".."6"
high_sev <- as.integer(unlist(link_cfg$mtbs$high_severity_classes))
non_processing <- as.integer(unlist(link_cfg$mtbs$non_processing_classes))
valid_sev <- as.integer(names(sev_map))

if (length(non_processing) == 0 || any(is.na(non_processing))) {
  stop("Configure mtbs.non_processing_classes (MTBS code 6) before extraction.")
}
if (any(high_sev %in% non_processing)) {
  stop("MTBS high-severity classes cannot also be non-processing mask classes.")
}

if (!file.exists(footprints_path)) {
  stop(glue("Footprints not found: {footprints_path}. Run script 01 first."))
}
if (!dir_exists(mtbs_dir) || length(dir_ls(mtbs_dir, glob = "*.tif")) == 0) {
  stop(glue(
    "No MTBS rasters in {mtbs_dir}. Download annual burn-severity mosaics from ",
    "https://www.mtbs.gov/ (one GeoTIFF per year) before running this script."
  ))
}

cat("Extract MTBS Severity\n=====================\n\n")

footprints <- st_read(footprints_path, quiet = TRUE)
footprints <- st_transform(footprints, area_crs)

# --- Inventory the MTBS rasters and parse the fire year from each filename. ----
# TODO: confirm the MTBS filename convention you downloaded and adjust the regex.
#       MTBS mosaics are commonly named like "mtbs_CONUS_<year>.tif".
raster_files <- dir_ls(mtbs_dir, glob = "*.tif")
parse_year <- function(path) as.integer(stringr::str_extract(path_file(path), "\\d{4}"))
years <- vapply(raster_files, parse_year, integer(1))

extract_one_year <- function(rf, yr) {
  r <- terra::rast(rf)
  fp <- terra::vect(st_transform(footprints, terra::crs(r)))
  # Pixel-value counts per footprint. exactextractr is faster if available;
  # terra::extract keeps the dependency surface small for the scaffold.
  vals <- terra::extract(r, fp)
  names(vals)[2] <- "sev"
  dt <- as.data.table(vals)
  dt[, stable_plot_id := footprints$stable_plot_id[ID]]
  dt <- dt[!is.na(sev) & sev %in% valid_sev]
  if (nrow(dt) == 0) return(NULL)
  agg <- dt[, summarize_mtbs_classes(
    severity = sev,
    valid_classes = valid_sev,
    high_severity_classes = high_sev,
    non_processing_classes = non_processing
  ), by = stable_plot_id]
  # A footprint with mask pixels only has no observed severity and is not an
  # interpretable fire-severity event. Mask coverage remains explicit for every
  # retained footprint/year.
  agg <- agg[n_pixels_valid > 0]
  if (nrow(agg) == 0) return(NULL)
  agg[, fire_year := yr]
  agg[]
}

events <- rbindlist(Map(extract_one_year, raster_files, years), use.names = TRUE, fill = TRUE)
setcolorder(events, c("stable_plot_id", "fire_year"))
setorder(events, stable_plot_id, fire_year)

dir_create(out_dir)
write_parquet(events, out_path, compression = "snappy")

cat(glue("Wrote {format(nrow(events), big.mark = ',')} plot-fire-year events -> {out_path}"), "\n")
