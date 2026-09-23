#!/usr/bin/env Rscript

# Refuse to build site-CWD model inputs from a cache that does not satisfy the
# one declared analysis window. Known sites without TerraClimate values remain
# visible as exclusions; partial years, duplicate keys, and stale windows fail.

suppressPackageStartupMessages({
  library(arrow)
  library(DBI)
  library(digest)
  library(duckdb)
  library(here)
  library(terra)
})

repo_root <- normalizePath(here(), winslash = "/", mustWork = TRUE)
setwd(repo_root)

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (!length(hit)) return(default)
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

config_path <- file.path("09_analysis", "config", "analysis_window.csv")
locations_path <- file.path(
  "09_analysis", "data", "intermediate", "model_site_locations.csv"
)
history_path <- file.path(
  "09_analysis", "data", "intermediate", "history_measurement_dates.parquet"
)
cache_dir <- arg_value(
  "cache-dir",
  file.path("09_analysis", "data", "cache", "terraclimate_site_cwd")
)
pixel_map_path <- file.path(cache_dir, "site_pixel_map.parquet")
climate_path <- file.path(cache_dir, "site_climate.parquet")
manifest_path <- file.path(cache_dir, "extraction_manifest.csv")
qa_path <- arg_value(
  "qa-output",
  file.path(
    "09_analysis", "qa", "outputs", "05_site_cwd_extraction",
    "site_cwd_cache_contract.csv"
  )
)

required_files <- c(
  config_path, locations_path, history_path, pixel_map_path, climate_path,
  manifest_path
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files)) {
  stop(
    "Missing site-CWD validation input(s): ",
    paste(missing_files, collapse = ", ")
  )
}

window <- read.csv(config_path, stringsAsFactors = FALSE)
required_config <- c(
  "climate_variable", "climate_start_year", "climate_end_year",
  "last_eligible_history_month", "climate_backend", "climate_source_id"
)
if (nrow(window) != 1L || !all(required_config %in% names(window))) {
  stop(
    "analysis_window.csv must contain exactly one row and the documented columns."
  )
}

climate_variable <- window$climate_variable[[1L]]
start_year <- as.integer(window$climate_start_year[[1L]])
end_year <- as.integer(window$climate_end_year[[1L]])
last_eligible_month <- as.Date(window$last_eligible_history_month[[1L]])
climate_backend <- window$climate_backend[[1L]]
climate_source_id <- window$climate_source_id[[1L]]
expected_source_id <- paste0(
  "https://tds-proxy.nkn.uidaho.edu/thredds/ncss/grid/",
  "TERRACLIMATE_ALL/data/TerraClimate_def_{year}.nc"
)
if (
  is.na(start_year) || is.na(end_year) || start_year > end_year ||
  is.na(last_eligible_month) ||
  format(last_eligible_month, "%Y-%m-%d") != sprintf("%d-12-01", end_year) ||
  !grepl("^[A-Za-z0-9_]+$", climate_variable) ||
  !identical(climate_backend, "local-ncss") ||
  !identical(climate_source_id, expected_source_id)
) {
  stop("Invalid analysis-window values.")
}

# Recompute the TerraClimate cell from current coordinates so a same-ID
# coordinate change cannot silently reuse an old pixel map.
location_points <- read.csv(locations_path, stringsAsFactors = FALSE)
pixel_points <- as.data.frame(read_parquet(pixel_map_path))
pixel_row <- match(location_points$site_id, pixel_points$site_id)
tc_grid <- rast(
  xmin = -180, xmax = 180,
  ymin = -90, ymax = 90,
  resolution = 1 / 24,
  crs = "+proj=longlat +datum=WGS84 +no_defs"
)
expected_pixel <- cellFromXY(
  tc_grid,
  as.matrix(location_points[, c("longitude", "latitude")])
)
pixel_mapping_mismatches <- sum(
  is.na(pixel_row) |
    is.na(expected_pixel) |
    pixel_points$pixel_id[pixel_row] != expected_pixel,
  na.rm = TRUE
)
expected_months <- 12L * (end_year - start_year + 1L)
manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
required_manifest <- c(
  "input_sha256", "backend", "variables", "start_year", "end_year",
  "annual_checkpoints", "checkpoint_directory"
)
if (nrow(manifest) != 1L || !all(required_manifest %in% names(manifest))) {
  stop("extraction_manifest.csv is missing required provenance fields.")
}
manifest_source_recorded <- "source_id" %in% names(manifest) &&
  !is.na(manifest$source_id[[1L]]) && nzchar(manifest$source_id[[1L]])
manifest_source_id <- if (manifest_source_recorded) {
  manifest$source_id[[1L]]
} else {
  NA_character_
}
location_sha256 <- digest(locations_path, algo = "sha256", file = TRUE)

con <- dbConnect(duckdb())
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

sql_path <- function(path) gsub("'", "''", path, fixed = TRUE)
sql_value <- function(value) gsub("'", "''", value, fixed = TRUE)

locations <- dbGetQuery(con, sprintf(
  paste(
    "SELECT count(*) AS rows, count(DISTINCT site_id) AS ids",
    "FROM read_csv_auto('%s', header = true)"
  ),
  sql_path(locations_path)
))
pixels <- dbGetQuery(con, sprintf(
  paste(
    "SELECT count(*) AS rows, count(DISTINCT site_id) AS ids,",
    "count(DISTINCT pixel_id) AS pixel_ids FROM read_parquet('%s')"
  ),
  sql_path(pixel_map_path)
))
climate <- dbGetQuery(con, sprintf(
  paste(
    "SELECT count(*) AS rows, count(DISTINCT site_id) AS ids,",
    "min(year) AS first_year, max(year) AS last_year,",
    "count(DISTINCT variable) AS variables,",
    "count(*) - count(DISTINCT (site_id, year, month, variable)) AS duplicate_keys",
    "FROM read_parquet('%s')"
  ),
  sql_path(climate_path)
))
site_months <- dbGetQuery(con, sprintf(
  paste(
    "WITH per_site AS (",
    " SELECT site_id, count(*) AS months",
    " FROM read_parquet('%s') WHERE variable = '%s' GROUP BY site_id",
    ") SELECT min(months) AS min_months, max(months) AS max_months,",
    "count(*) FILTER (WHERE months <> %d) AS sites_with_wrong_month_count",
    "FROM per_site"
  ),
  sql_path(climate_path), sql_value(climate_variable), expected_months
))
identity <- dbGetQuery(con, sprintf(
  paste(
    "WITH locations AS (",
    " SELECT CAST(site_id AS VARCHAR) AS site_id",
    " FROM read_csv_auto('%s', header = true)",
    "), pixels AS (",
    " SELECT DISTINCT CAST(site_id AS VARCHAR) AS site_id FROM read_parquet('%s')",
    "), climate AS (",
    " SELECT DISTINCT CAST(site_id AS VARCHAR) AS site_id",
    " FROM read_parquet('%s') WHERE variable = '%s'",
    ") SELECT",
    " count(*) FILTER (WHERE p.site_id IS NULL) AS locations_absent_from_pixel_map,",
    " count(*) FILTER (WHERE c.site_id IS NULL) AS locations_absent_from_climate",
    "FROM locations l LEFT JOIN pixels p USING (site_id)",
    "LEFT JOIN climate c USING (site_id)"
  ),
  sql_path(locations_path), sql_path(pixel_map_path),
  sql_path(climate_path), sql_value(climate_variable)
))
histories <- dbGetQuery(con, sprintf(
  paste(
    "WITH histories AS ( SELECT",
    " CASE WHEN first_measurement_date = date_trunc('month', first_measurement_date)",
    "  THEN CAST(date_trunc('month', first_measurement_date) AS DATE)",
    "  ELSE CAST(date_trunc('month', first_measurement_date) + INTERVAL 1 MONTH AS DATE)",
    " END AS first_included_month,",
    " CAST(date_trunc('month', last_measurement_date) AS DATE) AS last_included_month",
    " FROM read_parquet('%s') )",
    "SELECT count(*) AS histories, min(first_included_month) AS first_month,",
    "max(last_included_month) AS last_month,",
    "count(*) FILTER (WHERE last_included_month <= DATE '%s') AS eligible_histories,",
    "count(*) FILTER (WHERE last_included_month > DATE '%s') AS outside_window_histories",
    "FROM histories"
  ),
  sql_path(history_path), last_eligible_month, last_eligible_month
))
observed_variables <- dbGetQuery(con, sprintf(
  paste(
    "SELECT string_agg(DISTINCT variable, ',' ORDER BY variable) AS value",
    "FROM read_parquet('%s')"
  ),
  sql_path(climate_path)
))$value[[1L]]

problems <- character()
if (!identical(manifest$input_sha256[[1L]], location_sha256)) {
  problems <- c(problems, "manifest input hash differs from model locations")
}
if (!identical(manifest$backend[[1L]], climate_backend)) {
  problems <- c(problems, "manifest backend differs from the declared backend")
}
if (manifest_source_recorded &&
    !identical(manifest_source_id, climate_source_id)) {
  problems <- c(problems, "manifest source differs from the declared source")
}
if (!identical(manifest$variables[[1L]], climate_variable)) {
  problems <- c(problems, "manifest variable differs from the declared variable")
}
if (
  as.integer(manifest$start_year[[1L]]) != start_year ||
  as.integer(manifest$end_year[[1L]]) != end_year ||
  as.integer(manifest$annual_checkpoints[[1L]]) != end_year - start_year + 1L
) {
  problems <- c(problems, "manifest years differ from the declared window")
}
if (!identical(manifest$checkpoint_directory[[1L]], "_local_annual")) {
  problems <- c(problems, "manifest checkpoint directory is not local NCSS")
}
if (locations$rows != locations$ids) {
  problems <- c(problems, "duplicate model location IDs")
}
if (pixels$rows != pixels$ids) problems <- c(problems, "duplicate pixel-map site IDs")
if (identity$locations_absent_from_pixel_map != 0) {
  problems <- c(problems, "model locations absent from the pixel map")
}
if (pixel_mapping_mismatches != 0) {
  problems <- c(problems, "pixel map does not match current coordinates")
}
if (climate$duplicate_keys != 0) problems <- c(problems, "duplicate climate keys")
if (!identical(observed_variables, climate_variable)) {
  problems <- c(problems, "climate variable set differs from the declared variable")
}
if (
  is.na(climate$first_year) || is.na(climate$last_year) ||
  climate$first_year != start_year || climate$last_year != end_year
) {
  problems <- c(problems, "climate years differ from the declared window")
}
if (
  is.na(site_months$min_months) || is.na(site_months$max_months) ||
  site_months$min_months != expected_months ||
  site_months$max_months != expected_months ||
  site_months$sites_with_wrong_month_count != 0
) {
  problems <- c(problems, "represented sites do not have every declared month")
}
if (as.Date(histories$first_month) < as.Date(sprintf("%d-01-01", start_year))) {
  problems <- c(problems, "a modeled history starts before the climate window")
}

dir.create(dirname(qa_path), recursive = TRUE, showWarnings = FALSE)
result <- data.frame(
  contract_status = if (length(problems)) "fail" else "pass",
  climate_variable = climate_variable,
  configured_backend = climate_backend,
  configured_source_id = climate_source_id,
  manifest_backend = manifest$backend[[1L]],
  manifest_source_id = manifest_source_id,
  manifest_source_recorded = manifest_source_recorded,
  manifest_input_sha256 = manifest$input_sha256[[1L]],
  configured_start_year = start_year,
  configured_end_year = end_year,
  expected_months_per_represented_site = expected_months,
  observed_first_year = climate$first_year,
  observed_last_year = climate$last_year,
  min_months_per_represented_site = site_months$min_months,
  max_months_per_represented_site = site_months$max_months,
  model_location_ids = locations$ids,
  pixel_map_site_ids = pixels$ids,
  climate_site_ids = climate$ids,
  locations_absent_from_pixel_map = identity$locations_absent_from_pixel_map,
  locations_absent_from_climate = identity$locations_absent_from_climate,
  pixel_mapping_mismatches = pixel_mapping_mismatches,
  eligible_histories = histories$eligible_histories,
  outside_window_histories = histories$outside_window_histories,
  problems = paste(problems, collapse = ";"),
  stringsAsFactors = FALSE
)
write.csv(result, qa_path, row.names = FALSE)

if (length(problems)) {
  stop(
    "Site-CWD cache does not satisfy 09_analysis/config/analysis_window.csv: ",
    paste(problems, collapse = "; "), ". See ", qa_path, "."
  )
}

cat(
  "Site-CWD cache contract passed: ", climate$ids, " represented sites, ",
  expected_months, " months each (", start_year, "-", end_year, "); ",
  identity$locations_absent_from_climate, " sites have no climate values and ",
  histories$outside_window_histories, " histories end after the analysis window.\n",
  sep = ""
)
