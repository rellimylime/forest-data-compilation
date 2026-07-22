# ==============================================================================
# 01_build_plot_footprints.R
# Buffer each FIA plot coordinate into a footprint polygon for spatial linkage.
#
# One footprint per stable_plot_id whose public coordinate is constant across
# the records grouped under that identifier. Public FIA coordinates are
# approximate. A small number of repository stable_plot_id groups also contain
# more than one coordinate pair; those groups are excluded rather than choosing
# an arbitrary visit coordinate for a stable-plot-grain product.
#
# Input:  config processed.disturbance_linkage.inputs.fia_plot_source
# Output: plot_footprints.gpkg  (config processed.disturbance_linkage.files)
#
# Usage:
#   Rscript 08_disturbance_linkage/scripts/01_build_plot_footprints.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
buffer_m <- as.numeric(link_cfg$buffer_m)
area_crs <- link_cfg$area_crs

plot_source <- here(link_cfg$inputs$fia_plot_source)
out_dir <- here(link_cfg$output_dir)
out_path <- file.path(out_dir, link_cfg$files$plot_footprints)
dir_create(out_dir)

if (!file.exists(plot_source)) {
  stop(glue("FIA plot source not found: {plot_source}"))
}

cat("Build Plot Footprints\n=====================\n\n")
cat(glue("Source: {plot_source}"), "\n")
cat(glue("Buffer: {buffer_m} m in {area_crs}"), "\n\n")

# Retain only stable-plot groups with exactly one observed public coordinate.
# Selecting the first coordinate would make spatial-linkage results depend on
# row order and could silently associate an external event with the wrong visit.
plots <- as.data.table(read_parquet(
  plot_source,
  col_select = tidyselect::all_of(c("stable_plot_id", "LAT", "LON"))
))
plots <- plots[!is.na(stable_plot_id) & !is.na(LAT) & !is.na(LON)]
plots <- unique(plots, by = c("stable_plot_id", "LAT", "LON"))
coordinate_counts <- plots[, .(coordinate_pair_count = .N), by = stable_plot_id]
ambiguous_ids <- coordinate_counts[coordinate_pair_count > 1L, stable_plot_id]
plots <- plots[!stable_plot_id %in% ambiguous_ids]
plots <- merge(plots, coordinate_counts, by = "stable_plot_id", all.x = TRUE)

cat(glue("Distinct plots with coordinates: {format(nrow(plots), big.mark = ',')}"), "\n")
cat(glue(
  "Excluded multi-coordinate stable_plot_id groups: {format(length(ambiguous_ids), big.mark = ',')}"
), "\n")

# Point layer in geographic CRS, then buffer in the equal-area CRS.
pts <- st_as_sf(plots, coords = c("LON", "LAT"), crs = 4326)
pts <- st_transform(pts, area_crs)
footprints <- st_buffer(pts, dist = buffer_m)

dir_create(dirname(out_path))
if (file.exists(out_path)) file_delete(out_path)
st_write(footprints, out_path, quiet = TRUE)

cat("\nDone.\n")
cat(glue("Footprints: {out_path} ({nrow(footprints)} polygons)"), "\n")
