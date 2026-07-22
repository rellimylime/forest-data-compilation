# ==============================================================================
# 01_validate_disturbance_linkage.R
# Structural validation of the disturbance-linkage outputs.
#
# SCAFFOLD: runs once scripts 01-03 have produced outputs. Checks facts that must
# hold if the linkage is correct -- it does not judge the science.
#   * each output has its documented grain (no duplicate keys)
#   * every stable_plot_id joins back to the FIA plot source
#   * MTBS severity and mask fractions are within [0, 1]
#   * event years fall within the IDS/MTBS plausible range
#
# Output: disturbance_linkage_validation_checks.csv
#
# Usage:
#   Rscript 08_disturbance_linkage/qa/01_validate_disturbance_linkage.R
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
out_dir <- here(link_cfg$output_dir)
qa_dir <- here("08_disturbance_linkage/qa/outputs")
dir_create(qa_dir)

footprints_path <- file.path(out_dir, link_cfg$files$plot_footprints)
mtbs_path <- file.path(out_dir, link_cfg$files$plot_mtbs_fire_events)
ids_path <- file.path(out_dir, link_cfg$files$plot_ids_agent_events)
linked_path <- file.path(out_dir, link_cfg$files$plot_disturbance_linkage_events)
fia_source <- here(link_cfg$inputs$fia_plot_source)

checks <- list()
add_check <- function(check, status, value = NA, expected = NA) {
  checks[[length(checks) + 1]] <<- data.table(
    check = check, status = status,
    value = as.character(value), expected = as.character(expected))
}

fia_ids <- unique(as.data.table(read_parquet(
  fia_source, col_select = tidyselect::all_of("stable_plot_id")))$stable_plot_id)

# Footprints: unique per stable plot, all keys known to FIA.
if (file.exists(footprints_path)) {
  fp <- st_read(footprints_path, quiet = TRUE)
  add_check("footprints_unique_per_plot",
            if (anyDuplicated(fp$stable_plot_id) == 0) "pass" else "FAIL",
            anyDuplicated(fp$stable_plot_id), 0)
  add_check("footprints_keys_in_fia",
            if (all(fp$stable_plot_id %in% fia_ids)) "pass" else "FAIL",
            sum(!fp$stable_plot_id %in% fia_ids), 0)
  add_check("footprints_have_one_coordinate_pair",
            if ("coordinate_pair_count" %in% names(fp) &&
                  all(fp$coordinate_pair_count == 1L)) "pass" else "FAIL",
            if ("coordinate_pair_count" %in% names(fp)) {
              sum(fp$coordinate_pair_count != 1L)
            } else {
              NA
            },
            0)
} else {
  add_check("footprints_present", "missing -- run script 01")
}

# MTBS events: unique per plot x fire_year, fractions in [0, 1].
if (file.exists(mtbs_path)) {
  m <- as.data.table(read_parquet(mtbs_path))
  add_check("mtbs_unique_plot_year",
            if (anyDuplicated(m[, .(stable_plot_id, fire_year)]) == 0) "pass" else "FAIL",
            anyDuplicated(m[, .(stable_plot_id, fire_year)]), 0)
  bad_frac <- m[frac_high_severity < 0 | frac_high_severity > 1, .N]
  add_check("mtbs_frac_high_severity_in_range",
            if (bad_frac == 0) "pass" else "FAIL", bad_frac, 0)
  required_mask_cols <- c(
    "n_pixels_total", "n_pixels_valid", "n_pixels_masked", "frac_pixels_masked"
  )
  add_check("mtbs_mask_accounting_present",
            if (all(required_mask_cols %in% names(m))) "pass" else "FAIL",
            paste(setdiff(required_mask_cols, names(m)), collapse = ","), "all present")
  if (all(required_mask_cols %in% names(m))) {
    bad_mask <- m[
      frac_pixels_masked < 0 | frac_pixels_masked > 1 |
        n_pixels_total != n_pixels_valid + n_pixels_masked,
      .N
    ]
    add_check("mtbs_mask_accounting_consistent",
              if (bad_mask == 0) "pass" else "FAIL", bad_mask, 0)
  }
  add_check("mtbs_has_no_mean_of_categorical_codes",
            if (!"mean_severity_class" %in% names(m)) "pass" else "FAIL",
            "mean_severity_class" %in% names(m), FALSE)
} else {
  add_check("mtbs_events_present", "missing -- run script 02 (needs MTBS rasters)")
}

# IDS events: unique per plot x year x agent.
if (file.exists(ids_path)) {
  d <- as.data.table(read_parquet(ids_path))
  ids_required <- c(
    "dca_code", "dca_common_name", "overlap_acres",
    "footprint_overlap_fraction", "n_source_polygons",
    "source_polygon_acres_sum", "linkage_method"
  )
  add_check("ids_overlap_contract_present",
            if (all(ids_required %in% names(d))) "pass" else "FAIL",
            paste(setdiff(ids_required, names(d)), collapse = ","), "all present")
  key <- d[, .(stable_plot_id, survey_year, dca_code)]
  add_check("ids_unique_plot_year_agent",
            if (anyDuplicated(key) == 0) "pass" else "FAIL", anyDuplicated(key), 0)
  add_check("ids_has_bark_beetle_flag",
            if ("is_bark_beetle" %in% names(d)) "pass" else "FAIL", NA, "column present")
  if (all(c("overlap_acres", "footprint_overlap_fraction") %in% names(d))) {
    bad_overlap <- d[
      is.na(overlap_acres) | overlap_acres < 0 |
        is.na(footprint_overlap_fraction) |
        footprint_overlap_fraction < 0 | footprint_overlap_fraction > 1,
      .N
    ]
    add_check("ids_overlap_values_valid",
              if (bad_overlap == 0) "pass" else "FAIL", bad_overlap, 0)
  }
  add_check("ids_raw_dca_code_preserved",
            if ("dca_code" %in% names(d) && all(!is.na(d$dca_code))) "pass" else "FAIL",
            if ("dca_code" %in% names(d)) sum(is.na(d$dca_code)) else NA,
            0)
} else {
  add_check("ids_events_present", "missing -- run script 03")
}

# Linked events (script 04): required schema present, keys join back to FIA.
if (file.exists(linked_path)) {
  L <- as.data.table(read_parquet(linked_path))
  need <- c(
    "stable_plot_id", "source", "year", "source_event_code",
    "source_event_label", "event_type", "is_high_severity_fire",
    "is_bark_beetle", "linkage_method", "dca_code",
    "overlap_acres", "footprint_overlap_fraction", "frac_high_severity"
  )
  add_check("linked_has_required_columns",
            if (all(need %in% names(L))) "pass" else "FAIL",
            paste(setdiff(need, names(L)), collapse = ","), "all present")
  add_check("linked_keys_in_fia",
            if (all(L$stable_plot_id %in% fia_ids)) "pass" else "FAIL",
            sum(!L$stable_plot_id %in% fia_ids), 0)
  add_check("linked_sources_valid",
            if (all(L$source %in% c("mtbs", "ids"))) "pass" else "FAIL",
            paste(setdiff(unique(L$source), c("mtbs", "ids")), collapse = ","), "mtbs/ids")
  if (all(c("stable_plot_id", "source", "year", "source_event_code") %in% names(L))) {
    linked_key <- L[, .(stable_plot_id, source, year, source_event_code)]
    add_check("linked_unique_source_event_key",
              if (anyDuplicated(linked_key) == 0) "pass" else "FAIL",
              anyDuplicated(linked_key), 0)
  }
  if (all(c("source", "source_event_code", "dca_code") %in% names(L))) {
    bad_ids_code <- L[
      source == "ids" &
        (is.na(dca_code) | source_event_code != as.character(dca_code)),
      .N
    ]
    add_check("linked_ids_code_roundtrip",
              if (bad_ids_code == 0) "pass" else "FAIL", bad_ids_code, 0)
  }
  deprecated <- intersect(c("detail", "magnitude", "intersecting_acres"), names(L))
  add_check("linked_avoids_ambiguous_fields",
            if (length(deprecated) == 0) "pass" else "FAIL",
            paste(deprecated, collapse = ","), "none")
  current_year <- as.integer(format(Sys.Date(), "%Y"))
  bad_year <- L[
    is.na(year) |
      (source == "mtbs" & (year < 1984L | year > current_year + 1L)) |
      (source == "ids" & (year < 1900L | year > current_year + 1L)),
    .N
  ]
  add_check("linked_event_years_plausible",
            if (bad_year == 0) "pass" else "FAIL", bad_year, 0)
} else {
  add_check("linked_events_present", "missing -- run script 04")
}

checks_dt <- rbindlist(checks, fill = TRUE)
out_csv <- file.path(qa_dir, "disturbance_linkage_validation_checks.csv")
fwrite(checks_dt, out_csv)
cat("Wrote", out_csv, "\n\n")
print(checks_dt)
