# ==============================================================================
# 04_build_plot_disturbance_linkage.R
# Combine the MTBS and IDS per-plot event tables into one join-ready product.
#
# This is the capstone: scripts 02 and 03 each write a source-specific event
# table; this stacks them into a single long table with one schema, keyed on
# stable_plot_id, so 07_thermophilization can join disturbance history to survey
# intervals without knowing each source's columns.
#
# Output grain: one row per stable_plot_id x source x year x source_event_code
#   plot_disturbance_linkage_events.parquet
# Source codes and labels are kept separately. Source-specific measurements are
# not collapsed into a generic magnitude because MTBS severity fractions and IDS
# polygon-overlap areas have different meanings and units.
#
# Input:  plot_mtbs_fire_events.parquet (script 02), plot_ids_agent_events.parquet (script 03)
#         -- whichever exist; at least one is required.
#
# Usage:
#   Rscript 08_disturbance_linkage/scripts/04_build_plot_disturbance_linkage.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
out_dir <- here(link_cfg$output_dir)

mtbs_path <- file.path(out_dir, link_cfg$files$plot_mtbs_fire_events)
ids_path <- file.path(out_dir, link_cfg$files$plot_ids_agent_events)
out_path <- file.path(out_dir, link_cfg$files$plot_disturbance_linkage_events)

high_sev_classes <- as.integer(unlist(link_cfg$mtbs$high_severity_classes))
sev_map <- link_cfg$mtbs$severity_classes

cat("Build Plot Disturbance Linkage\n==============================\n\n")

parts <- list()

if (file.exists(mtbs_path)) {
  m <- as.data.table(read_parquet(mtbs_path))
  required_mtbs <- c(
    "stable_plot_id", "fire_year", "dominant_severity_class",
    "frac_high_severity", "n_pixels_valid", "n_pixels_masked"
  )
  missing_mtbs <- setdiff(required_mtbs, names(m))
  if (length(missing_mtbs) > 0) {
    stop("MTBS event table missing: ", paste(missing_mtbs, collapse = ", "))
  }
  parts[["mtbs"]] <- data.table(
    stable_plot_id = m$stable_plot_id,
    source = "mtbs",
    year = m$fire_year,
    source_event_code = as.character(m$dominant_severity_class),
    source_event_label = unlist(sev_map[as.character(m$dominant_severity_class)]),
    event_type = "fire",
    is_high_severity_fire = m$dominant_severity_class %in% high_sev_classes,
    is_bark_beetle = FALSE,
    linkage_method = "thematic_raster_within_public_fia_buffer",
    dominant_severity_class = m$dominant_severity_class,
    frac_high_severity = m$frac_high_severity,
    n_pixels_valid = m$n_pixels_valid,
    n_pixels_masked = m$n_pixels_masked,
    dca_code = NA_integer_,
    overlap_acres = NA_real_,
    footprint_overlap_fraction = NA_real_,
    n_source_polygons = NA_integer_,
    source_polygon_acres_sum = NA_real_
  )
  cat(glue("MTBS events:  {format(nrow(m), big.mark = ',')}"), "\n")
} else {
  cat("MTBS events:  (none -- run script 02)\n")
}

if (file.exists(ids_path)) {
  d <- as.data.table(read_parquet(ids_path))
  required_ids <- c(
    "stable_plot_id", "survey_year", "dca_code", "dca_common_name",
    "is_bark_beetle", "overlap_acres", "footprint_overlap_fraction",
    "n_source_polygons", "source_polygon_acres_sum", "linkage_method"
  )
  missing_ids <- setdiff(required_ids, names(d))
  if (length(missing_ids) > 0) {
    stop(
      "IDS event table uses an obsolete contract; rebuild script 03. Missing: ",
      paste(missing_ids, collapse = ", ")
    )
  }
  parts[["ids"]] <- data.table(
    stable_plot_id = d$stable_plot_id,
    source = "ids",
    year = d$survey_year,
    source_event_code = as.character(d$dca_code),
    source_event_label = d$dca_common_name,
    event_type = fifelse(d$is_bark_beetle, "bark_beetle", "insect_disease"),
    is_high_severity_fire = FALSE,
    is_bark_beetle = d$is_bark_beetle,
    linkage_method = d$linkage_method,
    dominant_severity_class = NA_integer_,
    frac_high_severity = NA_real_,
    n_pixels_valid = NA_integer_,
    n_pixels_masked = NA_integer_,
    dca_code = d$dca_code,
    overlap_acres = d$overlap_acres,
    footprint_overlap_fraction = d$footprint_overlap_fraction,
    n_source_polygons = d$n_source_polygons,
    source_polygon_acres_sum = d$source_polygon_acres_sum
  )
  cat(glue("IDS events:   {format(nrow(d), big.mark = ',')}"), "\n")
} else {
  cat("IDS events:   (none -- run script 03)\n")
}

if (length(parts) == 0) {
  stop("Neither MTBS nor IDS event table exists. Run scripts 02 and/or 03 first.")
}

events <- rbindlist(parts, use.names = TRUE)
setorder(events, stable_plot_id, year, source, source_event_code)

event_key <- c("stable_plot_id", "source", "year", "source_event_code")
if (anyDuplicated(events[, ..event_key])) {
  stop("Integrated disturbance events are duplicated at the declared source-code grain.")
}

dir_create(out_dir)
write_parquet_atomic(events, out_path, compression = "snappy")

cat("\nDone.\n")
cat(glue("Linked events: {out_path} ({format(nrow(events), big.mark = ',')} rows)"), "\n")
