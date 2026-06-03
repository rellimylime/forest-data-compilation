# ==============================================================================
# 04_extract_terraclimate_from_ranges.R
# Extract TerraClimate climatologies across BIEN species range polygons
#
# This script overlays each available BIEN range polygon with TerraClimate in
# Google Earth Engine and writes monthly species-level climate summaries.
#
# Grain of the output:
#   one row per species_key x calendar month x TerraClimate variable x metric
#
# Default climate period:
#   1981-2010 monthly climatology
#
# Usage examples:
#   Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R
#   Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --limit 25
#   Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --force --batch-size 10
#
# Prerequisites:
#   - local/user_config.yaml contains the Google Earth Engine project
#   - 06_species_niches/scripts/03_download_bien_ranges.R has completed
# ==============================================================================

library(here)
library(yaml)
library(sf)
library(arrow)
library(dplyr)
library(data.table)
library(fs)
library(glue)
library(jsonlite)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/gee_utils.R"))

# ------------------------------------------------------------------------------
# Command line options
# ------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", flag, "="), "", hit[[1]])
}

has_flag <- function(flag) flag %in% args

limit_arg              <- get_arg("--limit", NA_character_)
batch_size             <- as.integer(get_arg("--batch-size", "20"))
start_year             <- as.integer(get_arg("--start-year", "1981"))
end_year               <- as.integer(get_arg("--end-year", "2010"))
tile_scale             <- as.integer(get_arg("--tile-scale", "4"))
simplify_tolerance_deg <- as.numeric(get_arg("--simplify-tolerance", "0"))
max_retries            <- as.integer(get_arg("--max-retries", "3"))
retry_wait_seconds     <- as.integer(get_arg("--retry-wait", "30"))
force                  <- has_flag("--force")
mean_only              <- has_flag("--mean-only")

variables <- strsplit(
  get_arg("--variables", "tmmx,tmmn,pr,def,pet,aet"),
  ",",
  fixed = TRUE
)[[1]]
variables <- trimws(variables)

if (!is.na(limit_arg)) {
  limit_arg <- as.integer(limit_arg)
}
is_smoke_run <- !is.na(limit_arg)

if (start_year > end_year) {
  stop("--start-year must be less than or equal to --end-year.")
}

if (batch_size < 1) {
  stop("--batch-size must be at least 1.")
}

if (max_retries < 1) {
  stop("--max-retries must be at least 1.")
}

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
tc_config <- config$raw$terraclimate
niche_config <- config$processed$species_niches

processed_dir <- here(niche_config$output_dir)
smoke_data_dir <- here("06_species_niches/data/smoke")
qa_dir <- if (is_smoke_run) here("06_species_niches/qa/smoke") else here("06_species_niches/qa/outputs")
climate_period <- sprintf("%d-%d", start_year, end_year)

# Keep batch caches separate across test runs and extraction settings. This
# prevents a small --limit smoke test from being mistaken for a completed full
# extraction later.
run_tag <- paste(
  climate_period,
  paste(variables, collapse = "-"),
  if (mean_only) "mean" else "mean-p10-p50-p90",
  if (!is.na(limit_arg)) paste0("limit-", limit_arg) else "full",
  sep = "__"
)
run_tag <- gsub("[^A-Za-z0-9_.-]+", "_", run_tag)
batch_dir <- file.path(
  if (is_smoke_run) smoke_data_dir else processed_dir,
  "_range_climate_batches",
  run_tag
)

range_path <- file.path(
  processed_dir,
  niche_config$files$species_range_polygons
)
availability_path <- file.path(
  processed_dir,
  niche_config$files$bien_range_availability
)
# Smoke runs prefer smoke polygons/availability from scripts 02 and 03 when
# present, otherwise they fall back to production inputs for standalone testing.
if (is_smoke_run) {
  smoke_range_path <- file.path(smoke_data_dir, sprintf("species_range_polygons_limit_%d.gpkg", limit_arg))
  smoke_availability_path <- file.path(smoke_data_dir, sprintf("bien_range_availability_limit_%d.parquet", limit_arg))
  if (file.exists(smoke_range_path)) range_path <- smoke_range_path
  if (file.exists(smoke_availability_path)) availability_path <- smoke_availability_path
}

out_file <- file.path(
  if (is_smoke_run) smoke_data_dir else processed_dir,
  niche_config$files$species_range_climate
)
qa_summary_file <- file.path(qa_dir, "species_range_climate_summary.csv")
failure_file <- file.path(qa_dir, "species_range_climate_failures.csv")

if (is_smoke_run) {
  out_file <- file.path(
    smoke_data_dir,
    sprintf("species_range_climate_limit_%d.parquet", limit_arg)
  )
  qa_summary_file <- file.path(
    qa_dir,
    sprintf("species_range_climate_summary_limit_%d.csv", limit_arg)
  )
  failure_file <- file.path(
    qa_dir,
    sprintf("species_range_climate_failures_limit_%d.csv", limit_arg)
  )
}

dir_create(processed_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)
dir_create(batch_dir)

if (!file_exists(range_path)) {
  stop(glue("Range polygon file not found: {range_path}"))
}
if (!file_exists(availability_path)) {
  stop(glue("BIEN availability file not found: {availability_path}"))
}

scale_factors <- vapply(tc_config$variables, function(v) v$scale, numeric(1))
missing_vars <- setdiff(variables, names(scale_factors))
if (length(missing_vars) > 0) {
  stop(glue("Variables missing from config scale factors: {paste(missing_vars, collapse = ', ')}"))
}

cat("TerraClimate range extraction\n")
cat("==============================\n\n")
cat(glue("Climate period: {climate_period}"), "\n")
cat(glue("Variables: {paste(variables, collapse = ', ')}"), "\n")
cat(glue("Reducer: {if (mean_only) 'mean' else 'mean + p10/p50/p90'}"), "\n")
cat(glue("Batch size: {batch_size} polygons"), "\n\n")
cat(glue("Retries per batch: {max_retries}"), "\n")
cat(glue("Retry wait: {retry_wait_seconds} seconds"), "\n\n")

range_climate_failures <- list()

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------

make_monthly_climatology_stack <- function(ee, gee_asset, variables,
                                           start_year, end_year) {
  # Build one image with bands named like tmmx_m01, pr_m08, etc.
  # Each band is the mean of all years in the selected baseline period for that
  # calendar month.
  base_collection <- ee$ImageCollection(gee_asset)$
    filter(ee$Filter$calendarRange(as.integer(start_year), as.integer(end_year), "year"))$
    select(variables)

  stacked_img <- NULL

  for (month in 1:12) {
    monthly_img <- base_collection$
      filter(ee$Filter$calendarRange(as.integer(month), as.integer(month), "month"))$
      mean()$
      rename(sprintf("%s_m%02d", variables, month))

    if (is.null(stacked_img)) {
      stacked_img <- monthly_img
    } else {
      stacked_img <- stacked_img$addBands(monthly_img)
    }
  }

  stacked_img
}

make_range_reducer <- function(ee, mean_only = FALSE) {
  # The percentile reducer summarizes climate variation across grid cells inside
  # a species range. If GEE percentile output becomes too heavy for a test run,
  # use --mean-only to extract the simpler range mean first.
  if (mean_only) {
    return(ee$Reducer$mean())
  }

  ee$Reducer$mean()$combine(
    reducer2 = ee$Reducer$percentile(list(10L, 50L, 90L)),
    sharedInputs = TRUE
  )
}

sf_batch_to_ee <- function(sf_obj, ee, id_col = "species_key",
                           simplify_tolerance_deg = 0) {
  # Passing a GeoJSON dictionary to Earth Engine is much faster and less fragile
  # than constructing every polygon ring through reticulate by hand.
  keep <- sf_obj[, id_col, drop = FALSE]
  keep <- st_transform(keep, 4326)
  keep[[id_col]] <- as.character(keep[[id_col]])

  if (simplify_tolerance_deg > 0) {
    keep <- st_simplify(
      keep,
      dTolerance = simplify_tolerance_deg,
      preserveTopology = TRUE
    )
  }

  keep <- st_make_valid(keep)

  tmp_geojson <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp_geojson), add = TRUE)

  st_write(
    keep,
    tmp_geojson,
    driver = "GeoJSON",
    delete_dsn = TRUE,
    quiet = TRUE
  )

  geojson <- jsonlite::read_json(tmp_geojson, simplifyVector = FALSE)
  ee$FeatureCollection(geojson)
}

extract_range_batch <- function(image, polygons_sf, ee, reducer, scale,
                                tile_scale, simplify_tolerance_deg) {
  fc <- sf_batch_to_ee(
    polygons_sf,
    ee,
    id_col = "species_key",
    simplify_tolerance_deg = simplify_tolerance_deg
  )

  reduced <- image$reduceRegions(
    collection = fc,
    reducer = reducer,
    scale = scale,
    tileScale = tile_scale
  )

  result <- reduced$getInfo()
  if (length(result$features) == 0) {
    return(tibble())
  }

  rows <- lapply(result$features, function(feature) {
    as_tibble(feature$properties)
  })

  bind_rows(rows)
}

extract_range_batch_with_retries <- function(image, polygons_sf, ee, reducer,
                                             scale, tile_scale,
                                             simplify_tolerance_deg,
                                             max_retries,
                                             retry_wait_seconds) {
  last_error <- NULL

  for (attempt in seq_len(max_retries)) {
    result <- tryCatch(
      extract_range_batch(
        image = image,
        polygons_sf = polygons_sf,
        ee = ee,
        reducer = reducer,
        scale = scale,
        tile_scale = tile_scale,
        simplify_tolerance_deg = simplify_tolerance_deg
      ),
      error = function(e) {
        last_error <<- e
        NULL
      }
    )

    if (!is.null(result)) {
      return(result)
    }

    if (attempt < max_retries) {
      cat(glue("GEE error on attempt {attempt}/{max_retries}; waiting {retry_wait_seconds}s... "))
      Sys.sleep(retry_wait_seconds)
    }
  }

  stop(last_error)
}

extract_range_batch_adaptive <- function(image, polygons_sf, ee, reducer,
                                         scale, tile_scale,
                                         simplify_tolerance_deg,
                                         max_retries,
                                         retry_wait_seconds,
                                         depth = 0) {
  # Earth Engine occasionally fails on a whole FeatureCollection even when the
  # individual polygons are fine. If a top-level batch fails, split it into
  # smaller pieces and recombine the successful results.
  result <- tryCatch(
    extract_range_batch_with_retries(
      image = image,
      polygons_sf = polygons_sf,
      ee = ee,
      reducer = reducer,
      scale = scale,
      tile_scale = tile_scale,
      simplify_tolerance_deg = simplify_tolerance_deg,
      max_retries = max_retries,
      retry_wait_seconds = retry_wait_seconds
    ),
    error = function(e) e
  )

  if (!inherits(result, "error")) {
    return(result)
  }

  if (nrow(polygons_sf) <= 1) {
    failed_row <- sf::st_drop_geometry(polygons_sf[1, ])
    failure_record <- tibble(
      species_key = as.character(failed_row$species_key[[1]]),
      bien_query_name = if ("bien_query_name" %in% names(failed_row)) {
        as.character(failed_row$bien_query_name[[1]])
      } else {
        NA_character_
      },
      failure_stage = "terraclimate_range_extraction",
      failure_reason = conditionMessage(result),
      climate_period = climate_period,
      variables = paste(variables, collapse = ","),
      simplify_tolerance_deg = simplify_tolerance_deg,
      failure_time = as.character(Sys.time())
    )

    range_climate_failures[[length(range_climate_failures) + 1]] <<- failure_record
    cat(glue(
      "\n  Skipping failed single species {failure_record$species_key} ",
      "({failure_record$bien_query_name}); logged for QA. "
    ))

    return(tibble())
  }

  mid <- floor(nrow(polygons_sf) / 2)
  indent <- paste(rep("  ", depth + 1), collapse = "")
  cat(glue("\n{indent}Splitting failed batch into {mid} + {nrow(polygons_sf) - mid} polygons... "))

  left <- extract_range_batch_adaptive(
    image = image,
    polygons_sf = polygons_sf[seq_len(mid), ],
    ee = ee,
    reducer = reducer,
    scale = scale,
    tile_scale = tile_scale,
    simplify_tolerance_deg = simplify_tolerance_deg,
    max_retries = max_retries,
    retry_wait_seconds = retry_wait_seconds,
    depth = depth + 1
  )

  right <- extract_range_batch_adaptive(
    image = image,
    polygons_sf = polygons_sf[(mid + 1):nrow(polygons_sf), ],
    ee = ee,
    reducer = reducer,
    scale = scale,
    tile_scale = tile_scale,
    simplify_tolerance_deg = simplify_tolerance_deg,
    max_retries = max_retries,
    retry_wait_seconds = retry_wait_seconds,
    depth = depth + 1
  )

  bind_rows(left, right)
}

reshape_extraction <- function(df, variables, scale_factors, climate_period) {
  if (nrow(df) == 0) return(tibble())

  value_cols <- setdiff(names(df), "species_key")
  df$species_key <- as.character(df$species_key)
  long <- as_tibble(data.table::melt(
    as.data.table(df),
    id.vars = "species_key",
    measure.vars = value_cols,
    variable.name = "band_metric",
    value.name = "value",
    variable.factor = FALSE
  ))

  # Expected names:
  #   mean-only reducer: tmmx_m01
  #   combined reducer:  tmmx_m01_mean, tmmx_m01_p10, tmmx_m01_p50, ...
  matched <- regexec(
    "^([A-Za-z0-9]+)_m([0-9]{2})(?:_(mean|p[0-9]+))?$",
    long$band_metric
  )
  parts <- regmatches(long$band_metric, matched)

  parsed <- lapply(parts, function(x) {
    if (length(x) == 0) {
      return(c(variable = NA_character_, month = NA_character_, metric = NA_character_))
    }
    metric <- if (length(x) >= 4 && !is.na(x[[4]]) && nzchar(x[[4]])) x[[4]] else "mean"
    c(variable = x[[2]], month = x[[3]], metric = metric)
  })
  parsed <- as_tibble(do.call(rbind, parsed))

  long <- bind_cols(long, parsed) |>
    filter(!is.na(variable), variable %in% variables) |>
    mutate(
      month = as.integer(month),
      value = as.numeric(value),
      value = value * scale_factors[variable],
      climate_period = climate_period,
      climate_source = "TerraClimate",
      range_source = "BIEN"
    ) |>
    select(
      species_key, month, variable, metric, value,
      climate_period, climate_source, range_source
    )

  # Add mean temperature as a derived variable so downstream models can use one
  # interpretable thermal axis while retaining tmmx and tmmn if needed.
  if (all(c("tmmx", "tmmn") %in% variables)) {
    tmean <- long |>
      filter(variable %in% c("tmmx", "tmmn")) |>
      group_by(species_key, month, metric, climate_period, climate_source, range_source) |>
      summarize(
        value = if (all(c("tmmx", "tmmn") %in% variable)) mean(value, na.rm = TRUE) else NA_real_,
        .groups = "drop"
      ) |>
      filter(!is.na(value)) |>
      mutate(variable = "tmean") |>
      select(
        species_key, month, variable, metric, value,
        climate_period, climate_source, range_source
      )

    long <- bind_rows(long, tmean)
  }

  long
}

write_output_metadata <- function(path) {
  metadata_script <- here("scripts/utils/parquet_metadata.R")
  if (file.exists(metadata_script)) {
    source(metadata_script)
    write_parquet_metadata(path, sample_size = Inf)
  }
}

# ------------------------------------------------------------------------------
# Load species ranges and metadata
# ------------------------------------------------------------------------------

cat("[1/5] Loading BIEN range polygons...\n")

ranges <- st_read(range_path, quiet = TRUE)
ranges <- ranges |>
  mutate(species_key = as.character(species_key))

if (!is.na(limit_arg)) {
  ranges <- ranges[seq_len(min(limit_arg, nrow(ranges))), ]
}

availability <- read_parquet(availability_path) |>
  mutate(species_key = as.character(species_key))

metadata_cols <- intersect(
  c(
    "species_key", "source_code_system", "source_species_code",
    "scientific_name", "common_name", "community_layers", "bien_query_name"
  ),
  names(availability)
)
species_metadata <- availability |>
  select(all_of(metadata_cols)) |>
  distinct(species_key, .keep_all = TRUE)

cat(glue("  Loaded {format(nrow(ranges), big.mark = ',')} range polygons"), "\n\n")

# ------------------------------------------------------------------------------
# Build the TerraClimate image and reducer in Earth Engine
# ------------------------------------------------------------------------------

cat("[2/5] Initializing Google Earth Engine...\n")
ee <- init_gee()

cat("[3/5] Building monthly climatology image...\n")
climate_image <- make_monthly_climatology_stack(
  ee = ee,
  gee_asset = tc_config$gee_asset,
  variables = variables,
  start_year = start_year,
  end_year = end_year
)
range_reducer <- make_range_reducer(ee, mean_only = mean_only)

# ------------------------------------------------------------------------------
# Extract range summaries in cacheable batches
# ------------------------------------------------------------------------------

cat("[4/5] Extracting range climate summaries...\n")

n_batches <- ceiling(nrow(ranges) / batch_size)
batch_files <- character(n_batches)

for (batch_id in seq_len(n_batches)) {
  start_idx <- (batch_id - 1) * batch_size + 1
  end_idx <- min(batch_id * batch_size, nrow(ranges))
  batch_file <- file.path(batch_dir, sprintf("range_climate_batch_%04d.parquet", batch_id))
  batch_files[[batch_id]] <- batch_file

  if (file.exists(batch_file) && !force) {
    cat(glue("  Batch {batch_id}/{n_batches} exists, skipping"), "\n")
    next
  }

  cat(glue("  Batch {batch_id}/{n_batches} ({start_idx}-{end_idx})... "))
  t0 <- Sys.time()

  batch_sf <- ranges[start_idx:end_idx, ]

  raw_batch <- extract_range_batch_adaptive(
    image = climate_image,
    polygons_sf = batch_sf,
    ee = ee,
    reducer = range_reducer,
    scale = tc_config$gee_scale,
    tile_scale = tile_scale,
    simplify_tolerance_deg = simplify_tolerance_deg,
    max_retries = max_retries,
    retry_wait_seconds = retry_wait_seconds
  )

  batch_long <- reshape_extraction(
    raw_batch,
    variables = variables,
    scale_factors = scale_factors,
    climate_period = climate_period
  )

  write_parquet(as_tibble(batch_long), batch_file, compression = "snappy")

  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  cat(glue("saved {format(nrow(batch_long), big.mark = ',')} rows ({round(elapsed, 1)} min)\n"))
}

# ------------------------------------------------------------------------------
# Consolidate batches to the final analysis table
# ------------------------------------------------------------------------------

cat("\n[5/5] Consolidating batches...\n")

existing_batches <- batch_files[file.exists(batch_files)]
if (length(existing_batches) == 0) {
  stop("No batch outputs were written; cannot build final species range climate table.")
}

species_range_climate <- open_dataset(existing_batches, format = "parquet") |>
  collect() |>
  left_join(species_metadata, by = "species_key") |>
  select(
    species_key,
    any_of(c(
      "source_code_system", "source_species_code", "scientific_name",
      "common_name", "community_layers", "bien_query_name"
    )),
    month, variable, metric, value,
    climate_period, climate_source, range_source
  ) |>
  arrange(species_key, month, variable, metric)

write_parquet(as_tibble(species_range_climate), out_file, compression = "snappy")
write_output_metadata(out_file)

qa_summary <- species_range_climate |>
  group_by(variable, metric, climate_period) |>
  summarize(
    n_species = n_distinct(species_key),
    n_rows = n(),
    n_missing_values = sum(is.na(value)),
    value_min = min(value, na.rm = TRUE),
    value_median = median(value, na.rm = TRUE),
    value_max = max(value, na.rm = TRUE),
    .groups = "drop"
  )

write.csv(qa_summary, qa_summary_file, row.names = FALSE)

if (length(range_climate_failures) > 0) {
  failures <- bind_rows(range_climate_failures)
  if (file.exists(failure_file)) {
    existing_failures <- read.csv(failure_file, stringsAsFactors = FALSE)
    failures <- bind_rows(existing_failures, failures) |>
      distinct(species_key, climate_period, variables, failure_reason, .keep_all = TRUE)
  }
  write.csv(failures, failure_file, row.names = FALSE)
} else if (!file.exists(failure_file)) {
  write.csv(
    tibble(
      species_key = character(),
      bien_query_name = character(),
      failure_stage = character(),
      failure_reason = character(),
      climate_period = character(),
      variables = character(),
      simplify_tolerance_deg = numeric(),
      failure_time = character()
    ),
    failure_file,
    row.names = FALSE
  )
}

cat("\nDone.\n")
cat(glue("Range climate parquet: {out_file}"), "\n")
cat(glue("QA summary:            {qa_summary_file}"), "\n")
cat(glue("Failures:              {failure_file}"), "\n")
cat(glue("Rows:                  {format(nrow(species_range_climate), big.mark = ',')}"), "\n")
cat(glue("Species:               {format(n_distinct(species_range_climate$species_key), big.mark = ',')}"), "\n")
