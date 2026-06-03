# ==============================================================================
# 03_download_bien_ranges.R
# Download and cache BIEN range polygons for species with available BIEN maps.
#
# BIEN::BIEN_ranges_species(..., match_names_only = FALSE) writes shapefile
# sidecars to the current working directory. This script runs each request in an
# isolated temporary directory, reads the written shapefile, and caches it as a
# per-species GeoPackage.
#
# Usage:
#   Rscript 06_species_niches/scripts/03_download_bien_ranges.R
#   Rscript 06_species_niches/scripts/03_download_bien_ranges.R --limit=10
#   Rscript 06_species_niches/scripts/03_download_bien_ranges.R --force
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(tibble)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

args <- commandArgs(trailingOnly = TRUE)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

get_arg <- function(flag, default = NULL) {
  hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", flag, "="), "", hit[[1]])
}

has_flag <- function(flag) flag %in% args

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)
force <- has_flag("--force")

if (!requireNamespace("BIEN", quietly = TRUE)) {
  stop("Package 'BIEN' is required. Install it with install.packages('BIEN').")
}

config <- load_config()
niche_config <- config$processed$species_niches
processed_dir <- here(niche_config$output_dir)
smoke_data_dir <- here("06_species_niches/data/smoke")
raw_range_dir <- here(niche_config$range_dir)
qa_dir <- if (is_smoke_run) here("06_species_niches/qa/smoke") else here("06_species_niches/qa/outputs")

dir_create(processed_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(raw_range_dir)
dir_create(qa_dir)

availability_path <- file.path(processed_dir, niche_config$files$bien_range_availability)
# Smoke runs prefer smoke availability from script 02 when present, otherwise
# they use production availability so this script remains runnable on its own.
if (is_smoke_run) {
  smoke_availability_path <- file.path(smoke_data_dir, sprintf("bien_range_availability_limit_%d.parquet", limit_arg))
  if (file.exists(smoke_availability_path)) {
    availability_path <- smoke_availability_path
  }
}

polygons_path <- file.path(
  if (is_smoke_run) smoke_data_dir else processed_dir,
  niche_config$files$species_range_polygons
)
summary_path <- file.path(qa_dir, "bien_range_polygon_summary.csv")
failed_path <- file.path(qa_dir, "bien_range_polygon_failures.csv")

if (is_smoke_run) {
  polygons_path <- file.path(smoke_data_dir, sprintf("species_range_polygons_limit_%d.gpkg", limit_arg))
  summary_path <- file.path(qa_dir, sprintf("bien_range_polygon_summary_limit_%d.csv", limit_arg))
  failed_path <- file.path(qa_dir, sprintf("bien_range_polygon_failures_limit_%d.csv", limit_arg))
}

if (!file.exists(availability_path)) {
  stop(glue("BIEN availability table not found: {availability_path}"))
}

safe_file_stem <- function(x) {
  x <- gsub("[^A-Za-z0-9_\\-]+", "_", as.character(x))
  x <- gsub("_+", "_", x)
  gsub("^_|_$", "", x)
}

cache_path_for_species <- function(species_key, bien_query_name) {
  file.path(
    raw_range_dir,
    paste0(safe_file_stem(species_key), "__", safe_file_stem(bien_query_name), ".gpkg")
  )
}

read_cached_range <- function(path) {
  if (!file.exists(path)) return(NULL)
  tryCatch(
    suppressWarnings(st_read(path, quiet = TRUE)),
    error = function(e) NULL
  )
}

find_written_range_file <- function(dir) {
  shp_files <- list.files(dir, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)
  gpkg_files <- list.files(dir, pattern = "\\.gpkg$", full.names = TRUE, recursive = TRUE)
  candidates <- c(shp_files, gpkg_files)
  if (length(candidates) == 0) return(NA_character_)
  candidates[which.max(file.info(candidates)$size)]
}

download_one_range <- function(species_key, bien_query_name, cache_path, force = FALSE) {
  cached <- read_cached_range(cache_path)
  if (!is.null(cached) && nrow(cached) > 0 && !force) {
    cached$download_status <- "cached"
    cached$download_error <- NA_character_
    return(cached)
  }

  tmp_dir <- tempfile("bien_range_")
  dir_create(tmp_dir)
  old_dir <- getwd()
  on.exit({
    setwd(old_dir)
    unlink(tmp_dir, recursive = TRUE, force = TRUE)
  }, add = TRUE)

  setwd(tmp_dir)

  result <- tryCatch(
    BIEN::BIEN_ranges_species(
      species = bien_query_name,
      match_names_only = FALSE
    ),
    error = function(e) e
  )

  if (inherits(result, "error")) {
    return(data.table(
      species_key = species_key,
      bien_query_name = bien_query_name,
      download_status = "error",
      download_error = conditionMessage(result)
    ))
  }

  range_file <- find_written_range_file(tmp_dir)
  if (is.na(range_file)) {
    return(data.table(
      species_key = species_key,
      bien_query_name = bien_query_name,
      download_status = "empty",
      download_error = "BIEN returned no readable range file"
    ))
  }

  range_sf <- tryCatch(
    st_read(range_file, quiet = TRUE),
    error = function(e) e
  )

  if (inherits(range_sf, "error") || nrow(range_sf) == 0) {
    return(data.table(
      species_key = species_key,
      bien_query_name = bien_query_name,
      download_status = "read_error",
      download_error = if (inherits(range_sf, "error")) conditionMessage(range_sf) else "Empty range geometry"
    ))
  }

  if (is.na(st_crs(range_sf))) {
    st_crs(range_sf) <- 4326
  } else {
    range_sf <- st_transform(range_sf, 4326)
  }

  range_sf <- st_make_valid(range_sf)
  range_sf$species_key <- species_key
  range_sf$bien_query_name <- bien_query_name
  range_sf$download_status <- "downloaded"
  range_sf$download_error <- NA_character_

  keep_cols <- intersect(
    c("species", "gid", "species_key", "bien_query_name", "download_status", "download_error"),
    names(range_sf)
  )
  range_sf <- range_sf[, keep_cols]

  st_write(range_sf, cache_path, delete_dsn = TRUE, quiet = TRUE)
  range_sf
}

combine_sf_fill <- function(sf_list) {
  attrs <- lapply(sf_list, st_drop_geometry)
  geoms <- lapply(sf_list, st_geometry)
  attrs_dt <- rbindlist(lapply(attrs, as.data.table), fill = TRUE)
  geometry <- do.call(c, geoms)
  st_sf(attrs_dt, geometry = geometry, crs = st_crs(sf_list[[1]]))
}

cat("BIEN Range Polygon Download\n")
cat("===========================\n\n")

availability <- as.data.table(read_parquet(availability_path))
to_download <- availability[bien_range_available == TRUE]
setorder(to_download, source_code_system, scientific_name, species_key)

if (!is.na(limit_arg)) {
  to_download <- head(to_download, limit_arg)
}

to_download[, cache_path := mapply(
  cache_path_for_species,
  species_key,
  bien_query_name,
  USE.NAMES = FALSE
)]

cached_ok <- vapply(to_download$cache_path, function(p) {
  x <- read_cached_range(p)
  !is.null(x) && nrow(x) > 0
}, logical(1))

cat(glue("Species with BIEN ranges: {format(nrow(to_download), big.mark = ',')}"), "\n")
cat(glue("Already cached:          {format(sum(cached_ok), big.mark = ',')}"), "\n")
cat(glue("Need BIEN download:      {format(sum(!cached_ok | force), big.mark = ',')}"), "\n\n")

range_objects <- list()
failures <- list()

for (i in seq_len(nrow(to_download))) {
  row <- to_download[i]
  status_hint <- if (file.exists(row$cache_path) && !force) "cache" else "download"
  cat(glue("[{i}/{nrow(to_download)}] {row$bien_query_name} ({row$species_key}) [{status_hint}]"), "\n")

  one <- download_one_range(
    species_key = row$species_key,
    bien_query_name = row$bien_query_name,
    cache_path = row$cache_path,
    force = force
  )

  if (inherits(one, "sf")) {
    range_objects[[length(range_objects) + 1]] <- one
  } else {
    failures[[length(failures) + 1]] <- one
  }

  if (status_hint == "download") Sys.sleep(0.15)
}

if (length(range_objects) == 0) {
  stop("No range polygons were downloaded or read from cache.")
}

ranges <- combine_sf_fill(range_objects)
ranges <- st_make_valid(ranges)
ranges$geometry_is_empty <- st_is_empty(ranges)
ranges$geometry_is_valid <- st_is_valid(ranges)

area_crs <- config$params$area_crs %||% "EPSG:5070"
ranges_area <- st_transform(ranges, area_crs)
ranges$range_area_km2_qa <- as.numeric(st_area(ranges_area)) / 1e6

polygon_summary <- as.data.table(st_drop_geometry(ranges))[, .(
  n_polygon_parts = .N,
  n_empty = sum(geometry_is_empty, na.rm = TRUE),
  n_invalid = sum(!geometry_is_valid, na.rm = TRUE),
  range_area_km2_qa = sum(range_area_km2_qa, na.rm = TRUE),
  download_status = paste(sort(unique(download_status)), collapse = ";")
), by = .(species_key, bien_query_name)]
setorder(polygon_summary, species_key)

failures_dt <- if (length(failures) > 0) {
  rbindlist(failures, fill = TRUE)
} else {
  data.table(
    species_key = character(),
    bien_query_name = character(),
    download_status = character(),
    download_error = character()
  )
}

st_write(ranges, polygons_path, delete_dsn = TRUE, quiet = TRUE)
fwrite(polygon_summary, summary_path)
fwrite(failures_dt, failed_path)

cat("\nDone.\n")
cat(glue("Species with polygons: {uniqueN(ranges$species_key)}"), "\n")
cat(glue("Polygon parts:         {nrow(ranges)}"), "\n")
cat(glue("Failures:              {nrow(failures_dt)}"), "\n")
cat(glue("Combined polygons:     {polygons_path}"), "\n")
cat(glue("QA summary:            {summary_path}"), "\n")
cat(glue("Failures file:         {failed_path}"), "\n")
