#!/usr/bin/env Rscript

# Collect a deterministic, read-only snapshot for cross-machine comparison.
# All outputs are written beneath --output-dir; repository files are not changed.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(digest)
  library(dplyr)
})

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (!length(hit)) return(default)
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

repo <- normalizePath(arg_value("repo", getwd()), winslash = "/", mustWork = TRUE)
output_dir <- arg_value("output-dir", file.path(tempdir(), "forest_consistency_snapshot"))
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

repo_file <- function(...) file.path(repo, ...)
sha_file <- function(path) digest(path, algo = "sha256", file = TRUE)

canonical_value <- function(x) {
  if (inherits(x, "Date")) {
    out <- ifelse(is.na(x), "<NA>", format(x, "%Y-%m-%d"))
  } else if (inherits(x, "POSIXt")) {
    out <- ifelse(
      is.na(x), "<NA>",
      format(x, "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")
    )
  } else if (inherits(x, "integer64")) {
    out <- ifelse(is.na(x), "<NA>", as.character(x))
  } else if (is.double(x)) {
    out <- rep.int("<NA>", length(x))
    out[is.nan(x)] <- "<NaN>"
    out[is.infinite(x) & x > 0] <- "<Inf>"
    out[is.infinite(x) & x < 0] <- "<-Inf>"
    finite <- is.finite(x)
    out[finite] <- sprintf("%.17g", x[finite])
  } else if (is.integer(x)) {
    out <- ifelse(is.na(x), "<NA>", as.character(x))
  } else if (is.logical(x)) {
    out <- ifelse(is.na(x), "<NA>", ifelse(x, "TRUE", "FALSE"))
  } else {
    out <- ifelse(is.na(x), "<NA>", enc2utf8(as.character(x)))
  }
  paste0(nchar(out, type = "bytes"), ":", out)
}

canonical_sha256 <- function(data, key, label) {
  d <- copy(data)
  setorderv(d, key, na.last = TRUE)
  tmp <- file.path(output_dir, paste0(".", label, ".canonical.tmp"))
  con <- file(tmp, open = "wb")
  on.exit({
    try(close(con), silent = TRUE)
    unlink(tmp)
  }, add = TRUE)

  starts <- seq.int(1L, nrow(d), by = 10000L)
  for (start in starts) {
    end <- min(start + 9999L, nrow(d))
    fields <- lapply(d[start:end], canonical_value)
    lines <- do.call(paste, c(fields, sep = "	"))
    writeBin(charToRaw(paste0(lines, collapse = "\n")), con)
    writeBin(charToRaw("\n"), con)
  }
  close(con)
  con <- NULL
  sha_file(tmp)
}

schema_table <- function(tab, name) {
  fields <- tab$schema$fields
  data.table(
    input = name,
    position = seq_along(fields),
    column = vapply(fields, function(x) x$name, character(1)),
    type = vapply(fields, function(x) x$type$ToString(), character(1))
  )
}

product_specs <- list(
  stable_condition_intervals = list(
    path = "09_analysis/data/processed/stable_condition_intervals.parquet",
    key = "stable_condition_interval_key"
  ),
  history_cumulative_mortality = list(
    path = "09_analysis/data/processed/history_cumulative_mortality.parquet",
    key = "history_id"
  ),
  history_site_cwd = list(
    path = "09_analysis/data/processed/history_site_cwd.parquet",
    key = "history_id"
  ),
  lifestage_model_data = list(
    path = "09_analysis/data/processed/lifestage_model_data.parquet",
    key = c("history_id", "layer")
  ),
  pooled_model_data = list(
    path = "09_analysis/data/processed/pooled_model_data.parquet",
    key = "history_id"
  )
)

profiles <- list()
schemas <- list()
tables <- list()
for (name in names(product_specs)) {
  spec <- product_specs[[name]]
  path <- repo_file(spec$path)
  if (!file.exists(path)) {
    profiles[[name]] <- data.table(
      input = name, path = spec$path, exists = FALSE
    )
    next
  }

  tab <- read_parquet(path, as_data_frame = FALSE)
  d <- as.data.table(tab)
  tables[[name]] <- d
  schemas[[name]] <- schema_table(tab, name)
  profiles[[name]] <- data.table(
    input = name,
    path = spec$path,
    exists = TRUE,
    bytes = file.info(path)$size,
    byte_sha256 = sha_file(path),
    rows = nrow(d),
    columns = ncol(d),
    declared_key = paste(spec$key, collapse = "+"),
    duplicate_key_rows = nrow(d) - uniqueN(d, by = spec$key),
    canonical_sha256 = canonical_sha256(d, spec$key, name)
  )
}
fwrite(rbindlist(profiles, fill = TRUE), file.path(output_dir, "product_profiles.csv"))
fwrite(rbindlist(schemas, fill = TRUE), file.path(output_dir, "product_schemas.csv"))

locations_path <- repo_file(
  "09_analysis", "data", "intermediate", "model_site_locations.csv"
)
cache_dir <- repo_file(
  "09_analysis", "data", "cache", "terraclimate_site_cwd"
)
pixel_path <- file.path(cache_dir, "site_pixel_map.parquet")
climate_path <- file.path(cache_dir, "site_climate.parquet")
window_path <- arg_value(
  "analysis-window",
  repo_file("09_analysis", "config", "analysis_window.csv")
)
required <- c(locations_path, pixel_path, climate_path)
if (any(!file.exists(required))) {
  stop(
    "Missing consistency input(s): ",
    paste(required[!file.exists(required)], collapse = ", ")
  )
}

locations <- fread(locations_path)
pixel_tab <- read_parquet(pixel_path, as_data_frame = FALSE)
pixels <- as.data.table(pixel_tab)
if (file.exists(window_path)) {
  window <- fread(window_path)
} else {
  climate_variable_arg <- arg_value("climate-variable")
  start_year_arg <- arg_value("start-year")
  end_year_arg <- arg_value("end-year")
  if (any(vapply(
    list(climate_variable_arg, start_year_arg, end_year_arg),
    is.null,
    logical(1)
  ))) {
    stop(
      "The analysis-window file is absent. Supply --climate-variable, ",
      "--start-year, and --end-year explicitly."
    )
  }
  window <- data.table(
    climate_variable = climate_variable_arg,
    climate_start_year = as.integer(start_year_arg),
    climate_end_year = as.integer(end_year_arg)
  )
}
if (nrow(window) != 1L) stop("analysis_window.csv must have exactly one row.")

identity_profiles <- rbindlist(list(
  data.table(
    input = "model_site_locations",
    rows = nrow(locations),
    unique_ids = uniqueN(locations$site_id),
    byte_sha256 = sha_file(locations_path),
    canonical_sha256 = canonical_sha256(
      locations, "site_id", "model_site_locations"
    )
  ),
  data.table(
    input = "site_pixel_map",
    rows = nrow(pixels),
    unique_ids = uniqueN(pixels$site_id),
    byte_sha256 = sha_file(pixel_path),
    canonical_sha256 = canonical_sha256(
      pixels, "site_id", "site_pixel_map"
    )
  )
))
fwrite(identity_profiles, file.path(output_dir, "identity_profiles.csv"))
fwrite(schema_table(pixel_tab, "site_pixel_map"), file.path(output_dir, "pixel_schema.csv"))

if (anyDuplicated(locations$site_id) || anyDuplicated(pixels$site_id)) {
  stop("Duplicate site IDs in locations or pixel map.")
}
if (!setequal(as.character(locations$site_id), as.character(pixels$site_id))) {
  stop("Location and pixel-map site IDs differ.")
}

climate_variable <- as.character(window$climate_variable[[1L]])
start_year <- as.integer(window$climate_start_year[[1L]])
end_year <- as.integer(window$climate_end_year[[1L]])
years <- seq.int(start_year, end_year)
pixel_lookup <- pixels[, .(site_id = as.character(site_id), pixel_id)]
setkey(pixel_lookup, site_id)
climate_dataset <- open_dataset(climate_path)
annual_profiles <- vector("list", length(years))

for (i in seq_along(years)) {
  year_value <- years[[i]]
  site_values <- climate_dataset |>
    filter(variable == climate_variable, year == year_value) |>
    select(site_id, year, month, value) |>
    collect() |>
    as.data.table()
  site_values[, site_id := as.character(site_id)]

  if (anyDuplicated(site_values, by = c("site_id", "year", "month"))) {
    stop("Duplicate site-month keys in climate year ", year_value)
  }

  joined <- pixel_lookup[site_values, on = "site_id", nomatch = 0L]
  if (nrow(joined) != nrow(site_values)) {
    stop("Climate sites missing from pixel map in year ", year_value)
  }

  consistency <- joined[, .(distinct_values = uniqueN(value)),
                        by = .(pixel_id, year, month)]
  if (any(consistency$distinct_values != 1L)) {
    stop("Sites sharing a pixel disagree in year ", year_value)
  }

  pixel_values <- joined[, .(value = value[[1L]], site_rows = .N),
                         by = .(pixel_id, year, month)]
  setorder(pixel_values, pixel_id, year, month)
  lines <- paste(
    sprintf("%.0f", pixel_values$pixel_id),
    pixel_values$year,
    pixel_values$month,
    sprintf("%.17g", pixel_values$value),
    sep = ","
  )
  annual_sha <- digest(
    paste0(paste(lines, collapse = "\n"), "\n"),
    algo = "sha256",
    serialize = FALSE
  )
  annual_profiles[[i]] <- data.table(
    year = year_value,
    site_month_rows = nrow(site_values),
    pixel_month_rows = nrow(pixel_values),
    pixels = uniqueN(pixel_values$pixel_id),
    months = uniqueN(pixel_values$month),
    minimum = min(pixel_values$value),
    maximum = max(pixel_values$value),
    canonical_sha256 = annual_sha
  )
  message("Fingerprinted climate year ", year_value)
}

annual_profiles <- rbindlist(annual_profiles)
fwrite(annual_profiles, file.path(output_dir, "climate_year_profiles.csv"))
snapshot_lines <- paste(
  annual_profiles$year,
  annual_profiles$canonical_sha256,
  sep = ","
)
climate_snapshot_sha256 <- digest(
  paste0(paste(snapshot_lines, collapse = "\n"), "\n"),
  algo = "sha256",
  serialize = FALSE
)

climate_ids <- climate_dataset |>
  filter(variable == climate_variable) |>
  select(site_id) |>
  distinct() |>
  collect() |>
  as.data.table()
missing_sites <- locations[!as.character(site_id) %in% as.character(climate_ids$site_id)]
fwrite(missing_sites, file.path(output_dir, "climate_missing_sites.csv"))

climate_summary <- data.table(
  variable = climate_variable,
  start_year = start_year,
  end_year = end_year,
  years = length(years),
  location_sites = uniqueN(locations$site_id),
  mapped_sites = uniqueN(pixels$site_id),
  mapped_pixels = uniqueN(pixels$pixel_id),
  represented_sites = uniqueN(climate_ids$site_id),
  missing_sites = nrow(missing_sites),
  expected_months_per_site = length(years) * 12L,
  climate_snapshot_sha256 = climate_snapshot_sha256
)
fwrite(climate_summary, file.path(output_dir, "climate_summary.csv"))

predictors <- c(
  "fire_cumulative_mortality_pct",
  "insect_cumulative_mortality_pct",
  "disease_cumulative_mortality_pct",
  "cumulative_site_CWD_mm",
  "full_survey_period_years"
)
stage <- tables$lifestage_model_data
community <- tables$pooled_model_data
if (is.null(stage) || is.null(community)) {
  stop("Model products are required for membership checks.")
}
groups <- list(
  saplings = stage[layer == "saplings"],
  trees = stage[layer == "trees"],
  community = community
)
sample_parts <- list()
common_parts <- list()
for (response in c("temperature", "precipitation", "CWD")) {
  outcome <- paste0("delta_", response)
  needed <- c(outcome, predictors, "stable_plot_id", "history_id")
  for (group in names(groups)) {
    d <- groups[[group]]
    included <- d$cumulative_site_CWD_complete %in% TRUE &
      complete.cases(d[, ..needed])
    sample_parts[[paste(response, group)]] <- data.table(
      response = response,
      group = group,
      available = nrow(d),
      complete = sum(included),
      excluded = sum(!included),
      stable_plots = uniqueN(d[included, stable_plot_id])
    )
  }

  stage_complete <- stage[
    layer %in% c("saplings", "trees") &
      cumulative_site_CWD_complete %in% TRUE &
      complete.cases(stage[, ..needed])
  ]
  layer_counts <- unique(
    stage_complete[, .(history_id, layer)]
  )[, .(required_layers = uniqueN(layer)), by = history_id]
  common_parts[[response]] <- data.table(
    response = response,
    common_sapling_adult_histories = sum(layer_counts$required_layers == 2L)
  )
}
fwrite(rbindlist(sample_parts), file.path(output_dir, "model_sample_counts.csv"))
fwrite(rbindlist(common_parts), file.path(output_dir, "common_history_counts.csv"))

manifest_path <- file.path(cache_dir, "extraction_manifest.csv")
if (file.exists(manifest_path)) {
  file.copy(
    manifest_path,
    file.path(output_dir, "extraction_manifest.csv"),
    overwrite = TRUE
  )
}

package_names <- c(
  "arrow", "data.table", "digest", "dplyr", "duckdb", "renv",
  "sandwich", "lmtest", "ggplot2", "ggeffects", "sjPlot", "rmarkdown"
)
environment <- data.table(
  component = c("R", package_names, "Pandoc"),
  version = c(
    R.version.string,
    vapply(package_names, function(package) {
      if (requireNamespace(package, quietly = TRUE)) {
        as.character(packageVersion(package))
      } else {
        "not installed"
      }
    }, character(1)),
    if (requireNamespace("rmarkdown", quietly = TRUE) &&
        rmarkdown::pandoc_available()) {
      as.character(rmarkdown::pandoc_version())
    } else {
      "not installed"
    }
  )
)
fwrite(environment, file.path(output_dir, "environment_versions.csv"))

git_output <- function(arguments) {
  paste(system2("git", arguments, stdout = TRUE, stderr = TRUE), collapse = "\n")
}
metadata <- data.table(
  field = c(
    "git_branch", "git_commit", "git_status_short",
    "repository", "collected_at_utc"
  ),
  value = c(
    git_output(c("-C", repo, "branch", "--show-current")),
    git_output(c("-C", repo, "rev-parse", "HEAD")),
    git_output(c("-C", repo, "status", "--short")),
    repo,
    format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )
)
fwrite(metadata, file.path(output_dir, "collection_metadata.csv"))

summary_parts <- list(
  data.table(
    item = c(
      "model_site_locations", "site_pixel_map", "climate_snapshot"
    ),
    canonical_sha256 = c(
      identity_profiles[input == "model_site_locations", canonical_sha256],
      identity_profiles[input == "site_pixel_map", canonical_sha256],
      climate_snapshot_sha256
    )
  ),
  rbindlist(profiles, fill = TRUE)[
    exists %in% TRUE,
    .(item = input, canonical_sha256)
  ]
)
fwrite(rbindlist(summary_parts), file.path(output_dir, "canonical_summary.csv"))

cat("Consistency snapshot written to: ",
    normalizePath(output_dir, winslash = "/", mustWork = TRUE), "\n", sep = "")
