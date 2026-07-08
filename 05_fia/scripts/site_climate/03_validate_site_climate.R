# ==============================================================================
# 03_validate_site_climate.R
# Validate the FIA site-climate extraction products.
#
# This QA script does not modify the climate products. It checks:
#   1. all_site_locations.csv has one valid coordinate row per site_id.
#   2. site_pixel_map.parquet is internally consistent, when present.
#   3. site_climate.parquet has expected year/month/variable coverage and
#      plausible value ranges, when present.
#
# Usage:
#   Rscript 05_fia/scripts/site_climate/03_validate_site_climate.R
#   Rscript 05_fia/scripts/site_climate/03_validate_site_climate.R \
#     --pixel-map-file=/path/to/site_pixel_map.parquet \
#     --climate-file=/path/to/site_climate.parquet \
#     --output-prefix=external_copy_
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(fs)
  library(here)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
args <- commandArgs(trailingOnly = TRUE)

arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) == 0) {
    return(default)
  }
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

site_dir <- here("05_fia/data/processed/site_climate")
default_site_file <- file.path(site_dir, "all_site_locations.csv")
site_file <- arg_value("site-file", default_site_file)
pixel_map_file <- arg_value("pixel-map-file", file.path(site_dir, "site_pixel_map.parquet"))
climate_file <- arg_value("climate-file", file.path(site_dir, "site_climate.parquet"))
output_prefix <- arg_value("output-prefix", "")
pixel_map_label <- pixel_map_file
climate_label <- climate_file
qa_dir <- here("05_fia/qa/outputs")
dir_create(qa_dir)

site_vars <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
start_year <- 1958L

append_check <- function(checks, check_name, status, value, expected, severity = "error") {
  rbind(
    checks,
    data.table(
      check_name = check_name,
      status = status,
      severity = severity,
      value = as.character(value),
      expected = as.character(expected)
    ),
    fill = TRUE
  )
}

checks <- data.table()

if (!file.exists(site_file)) {
  stop(sprintf("Missing site list: %s", site_file))
}

sites <- fread(site_file)
required_site_cols <- c("site_id", "latitude", "longitude", "source")
checks <- append_check(
  checks,
  "site_list_required_columns",
  if (all(required_site_cols %in% names(sites))) "pass" else "fail",
  paste(names(sites), collapse = ","),
  paste(required_site_cols, collapse = ",")
)

checks <- append_check(
  checks,
  "site_list_unique_site_id",
  if (anyDuplicated(sites$site_id) == 0) "pass" else "fail",
  anyDuplicated(sites$site_id),
  0
)

bad_coords <- sites[
  is.na(latitude) | is.na(longitude) |
    latitude < -90 | latitude > 90 |
    longitude < -180 | longitude > 180
]
checks <- append_check(
  checks,
  "site_list_valid_coordinates",
  if (nrow(bad_coords) == 0) "pass" else "fail",
  nrow(bad_coords),
  0
)

site_summary <- data.table(
  metric = c(
    "n_sites",
    "n_duplicate_site_ids",
    "n_invalid_coordinate_rows",
    "min_latitude",
    "max_latitude",
    "min_longitude",
    "max_longitude"
  ),
  value = c(
    nrow(sites),
    sum(duplicated(sites$site_id)),
    nrow(bad_coords),
    min(sites$latitude, na.rm = TRUE),
    max(sites$latitude, na.rm = TRUE),
    min(sites$longitude, na.rm = TRUE),
    max(sites$longitude, na.rm = TRUE)
  )
)

using_default_site_file <- normalizePath(site_file, winslash = "/", mustWork = FALSE) ==
  normalizePath(default_site_file, winslash = "/", mustWork = FALSE)

cond_dir <- here(config$processed$fia$cond$output_dir)
if (!using_default_site_file) {
  checks <- append_check(
    checks,
    "site_list_matches_valid_cond_stable_plot_ids",
    "skip",
    "external site file",
    "default FIA site list only",
    severity = "info"
  )
} else if (dir_exists(cond_dir)) {
  cond_locations <- open_dataset(cond_dir, partitioning = "state") |>
    select(stable_plot_id, STATECD, LAT, LON) |>
    distinct() |>
    collect() |>
    as.data.table()

  cond_valid_locations <- cond_locations[
    !is.na(LAT) & !is.na(LON) & LAT != 0 & LON != 0
  ]
  coord_variation <- cond_valid_locations[, .(
    n_coordinate_rows = .N,
    n_latitudes = uniqueN(LAT),
    n_longitudes = uniqueN(LON)
  ), by = stable_plot_id]
  n_valid_stable_plot_ids <- uniqueN(cond_valid_locations$stable_plot_id)
  n_stable_plot_ids_with_coordinate_variation <- nrow(
    coord_variation[n_latitudes > 1 | n_longitudes > 1]
  )

  checks <- append_check(
    checks,
    "site_list_matches_valid_cond_stable_plot_ids",
    if (nrow(sites) == n_valid_stable_plot_ids) "pass" else "fail",
    nrow(sites),
    n_valid_stable_plot_ids
  )

  site_summary <- rbind(
    site_summary,
    data.table(
      metric = c(
        "n_valid_stable_plot_ids_in_cond",
        "n_cond_stable_plot_ids_with_coordinate_variation"
      ),
      value = c(
        n_valid_stable_plot_ids,
        n_stable_plot_ids_with_coordinate_variation
      )
    ),
    fill = TRUE
  )
} else {
  checks <- append_check(
    checks,
    "cond_source_present_for_site_list_comparison",
    "warn",
    "missing",
    cond_dir,
    severity = "warning"
  )
}

pixel_summary <- data.table()
if (file.exists(pixel_map_file)) {
  pixel_map <- as.data.table(read_parquet(pixel_map_file))
  checks <- append_check(
    checks,
    "pixel_map_required_columns",
    if (all(c("site_id", "pixel_id", "x", "y", "coverage_fraction") %in% names(pixel_map))) "pass" else "fail",
    paste(names(pixel_map), collapse = ","),
    "site_id,pixel_id,x,y,coverage_fraction"
  )
  checks <- append_check(
    checks,
    "pixel_map_matches_site_count",
    if (uniqueN(pixel_map$site_id) == nrow(sites)) "pass" else "fail",
    uniqueN(pixel_map$site_id),
    nrow(sites)
  )
  pixel_summary <- data.table(
    metric = c("n_pixel_map_rows", "n_site_ids", "n_unique_pixels", "mean_sites_per_pixel"),
    value = c(
      nrow(pixel_map),
      uniqueN(pixel_map$site_id),
      uniqueN(pixel_map$pixel_id),
      nrow(pixel_map) / uniqueN(pixel_map$pixel_id)
    )
  )
} else {
  checks <- append_check(
    checks,
    "pixel_map_present",
    "warn",
    "missing",
    pixel_map_label,
    severity = "warning"
  )
}

climate_summary <- data.table()
climate_value_ranges <- data.table()
if (file.exists(climate_file)) {
  climate_ds <- open_dataset(climate_file)
  climate_meta <- climate_ds |>
    summarise(
      n_rows = n(),
      n_sites = n_distinct(site_id),
      min_year = min(year, na.rm = TRUE),
      max_year = max(year, na.rm = TRUE),
      n_variables = n_distinct(variable)
    ) |>
    collect() |>
    as.data.table()

  variable_counts <- climate_ds |>
    group_by(variable) |>
    summarise(
      n_rows = n(),
      min_year = min(year, na.rm = TRUE),
      max_year = max(year, na.rm = TRUE),
      n_months = n_distinct(month),
      min_value = min(value, na.rm = TRUE),
      max_value = max(value, na.rm = TRUE),
      .groups = "drop"
    ) |>
    collect() |>
    as.data.table()

  observed_vars <- sort(variable_counts$variable)
  expected_end_year <- max(variable_counts$max_year, na.rm = TRUE)
  expected_rows <- nrow(sites) * length(site_vars) * 12L *
    (expected_end_year - start_year + 1L)

  checks <- append_check(
    checks,
    "climate_variables_expected",
    if (setequal(observed_vars, site_vars)) "pass" else "fail",
    paste(observed_vars, collapse = ","),
    paste(site_vars, collapse = ",")
  )
  checks <- append_check(
    checks,
    "climate_site_count_matches_site_list",
    if (climate_meta$n_sites == nrow(sites)) "pass" else "fail",
    climate_meta$n_sites,
    nrow(sites)
  )
  checks <- append_check(
    checks,
    "climate_row_count_complete_through_max_year",
    if (climate_meta$n_rows == expected_rows) "pass" else "fail",
    climate_meta$n_rows,
    expected_rows
  )
  checks <- append_check(
    checks,
    "climate_months_expected",
    if (all(variable_counts$n_months == 12L)) "pass" else "fail",
    paste(variable_counts$n_months, collapse = ","),
    "12 for every variable"
  )

  climate_summary <- data.table(
    metric = c(
      "n_rows",
      "n_sites",
      "min_year",
      "max_year",
      "n_variables",
      "expected_complete_rows_through_max_year"
    ),
    value = c(
      climate_meta$n_rows,
      climate_meta$n_sites,
      climate_meta$min_year,
      climate_meta$max_year,
      climate_meta$n_variables,
      expected_rows
    )
  )
  climate_value_ranges <- variable_counts
} else {
  checks <- append_check(
    checks,
    "site_climate_present",
    "warn",
    "missing",
    climate_label,
    severity = "warning"
  )
}

fwrite(checks, file.path(qa_dir, paste0(output_prefix, "site_climate_validation_checks.csv")))
fwrite(site_summary, file.path(qa_dir, paste0(output_prefix, "site_climate_site_list_summary.csv")))
if (nrow(pixel_summary) > 0) {
  fwrite(pixel_summary, file.path(qa_dir, paste0(output_prefix, "site_climate_pixel_map_summary.csv")))
}
if (nrow(climate_summary) > 0) {
  fwrite(climate_summary, file.path(qa_dir, paste0(output_prefix, "site_climate_output_summary.csv")))
  fwrite(climate_value_ranges, file.path(qa_dir, paste0(output_prefix, "site_climate_value_ranges.csv")))
}

cat("Site Climate Validation\n")
cat("=======================\n\n")
print(checks)

if (any(checks$status == "fail" & checks$severity == "error")) {
  stop("Site-climate validation failed one or more required checks.")
}

cat("\nDone.\n")
