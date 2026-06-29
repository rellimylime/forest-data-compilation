# ==============================================================================
# 05_build_species_climate_niches.R
# Build compact species-level climate niche indicators from BIEN range overlays.
#
# Input grain:
#   species_key x month x TerraClimate variable x range metric
#
# Output grain:
#   one row per species_key
#
# The goal is to keep downstream community metrics interpretable: eight scalar
# species traits are easier to model than a 12-month climate vector. These
# values describe climate across the mapped species range, not conditions at an
# individual FIA plot.
#
# Usage:
#   Rscript 06_species_niches/scripts/05_build_species_climate_niches.R
#   Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --range-scope=us_study_area
#   Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --limit=25
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(tibble)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

# ------------------------------------------------------------------------------
# Command line options
# ------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", flag, "="), "", hit[[1]])
}

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)
range_scope <- get_arg("--range-scope", "global")

allowed_range_scopes <- c("global", "us_study_area")
if (!range_scope %in% allowed_range_scopes) {
  stop(glue("--range-scope must be one of: {paste(allowed_range_scopes, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
niche_config <- config$processed$species_niches

processed_dir <- here(niche_config$output_dir)
smoke_data_dir <- here("06_species_niches/data/smoke")
qa_dir <- if (is_smoke_run) here("06_species_niches/qa/smoke") else here("06_species_niches/qa/outputs")

range_climate_path <- file.path(processed_dir, niche_config$files$species_range_climate)
out_path <- file.path(processed_dir, niche_config$files$species_climate_niches)

if (range_scope != "global") {
  range_climate_path <- file.path(
    processed_dir,
    sprintf("species_range_climate_%s.parquet", range_scope)
  )
  out_path <- file.path(
    processed_dir,
    sprintf("species_climate_niches_%s.parquet", range_scope)
  )
}

# Smoke runs prefer smoke input from script 04 when present, otherwise they use
# production range climate and limit the species after reading.
if (is_smoke_run) {
  smoke_suffix <- if (range_scope == "global") {
    sprintf("limit_%d", limit_arg)
  } else {
    sprintf("%s_limit_%d", range_scope, limit_arg)
  }

  smoke_range_climate_path <- file.path(
    smoke_data_dir,
    sprintf("species_range_climate_%s.parquet", smoke_suffix)
  )
  if (file.exists(smoke_range_climate_path)) {
    range_climate_path <- smoke_range_climate_path
  }

  out_path <- file.path(
    smoke_data_dir,
    sprintf("species_climate_niches_%s.parquet", smoke_suffix)
  )
}

qa_summary_path <- file.path(
  qa_dir,
  if (is_smoke_run) {
    sprintf("species_climate_niches_summary_%s.csv", smoke_suffix)
  } else if (range_scope != "global") {
    sprintf("species_climate_niches_summary_%s.csv", range_scope)
  } else {
    "species_climate_niches_summary.csv"
  }
)
qa_rankings_path <- file.path(
  qa_dir,
  if (is_smoke_run) {
    sprintf("species_climate_niches_rankings_%s.csv", smoke_suffix)
  } else if (range_scope != "global") {
    sprintf("species_climate_niches_rankings_%s.csv", range_scope)
  } else {
    "species_climate_niches_rankings.csv"
  }
)
qa_missing_path <- file.path(
  qa_dir,
  if (is_smoke_run) {
    sprintf("species_climate_niches_missing_%s.csv", smoke_suffix)
  } else if (range_scope != "global") {
    sprintf("species_climate_niches_missing_%s.csv", range_scope)
  } else {
    "species_climate_niches_missing.csv"
  }
)

dir_create(processed_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)

if (!file.exists(range_climate_path)) {
  stop(glue("Species range climate table not found: {range_climate_path}"))
}

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------

write_parquet_safely <- function(df, path, compression = "snappy") {
  # Write through a temporary file so interrupted reruns do not corrupt the
  # canonical output.
  dir_create(dirname(path))
  tmp_path <- tempfile(
    pattern = paste0(path_file(path), "_tmp_"),
    tmpdir = dirname(path),
    fileext = ".parquet"
  )
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)

  write_parquet(df, tmp_path, compression = compression)
  file_copy(tmp_path, path, overwrite = TRUE)
}

write_csv_safely <- function(df, path) {
  dir_create(dirname(path))
  tmp_path <- tempfile(
    pattern = paste0(path_file(path), "_tmp_"),
    tmpdir = dirname(path),
    fileext = ".csv"
  )
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)

  write.csv(df, tmp_path, row.names = FALSE)
  file_copy(tmp_path, path, overwrite = TRUE)
}

count_present <- function(.SD) {
  rowSums(!is.na(as.data.frame(.SD)))
}

# ------------------------------------------------------------------------------
# Load range-climate data
# ------------------------------------------------------------------------------

cat("Species Climate Niche Build\n")
cat("===========================\n\n")
cat(glue("Range scope: {range_scope}"), "\n")
cat(glue("Input:  {range_climate_path}"), "\n")
cat(glue("Output: {out_path}"), "\n\n")

range_climate <- as.data.table(read_parquet(range_climate_path))

if (is_smoke_run && !grepl(sprintf("_limit_%d\\.parquet$", limit_arg), range_climate_path)) {
  keep_species <- unique(range_climate$species_key)[seq_len(min(limit_arg, uniqueN(range_climate$species_key)))]
  range_climate <- range_climate[species_key %in% keep_species]
}

required_variables <- c("tmean", "def", "pr")
missing_variables <- setdiff(required_variables, unique(range_climate$variable))
if (length(missing_variables) > 0) {
  stop(glue("Missing required range-climate variable(s): {paste(missing_variables, collapse = ', ')}"))
}

# Use the spatial mean across each BIEN range scope as the canonical species
# niche value. The p10/p50/p90 spatial summaries remain available in the
# range-climate table for sensitivity checks.
monthly_mean <- range_climate[
  metric == "mean" & variable %in% required_variables
]

metadata_cols <- intersect(
  c(
    "species_key", "source_code_system", "source_species_code",
    "scientific_name", "common_name", "community_layers",
    "bien_query_name", "niche_taxon_name", "niche_taxon_key",
    "climate_period", "climate_source", "range_source",
    "range_scope"
  ),
  names(monthly_mean)
)

# One row per species and month, with tmean/def/pr as columns.
monthly_wide <- dcast(
  monthly_mean,
  ... ~ variable,
  value.var = "value"
)

# ------------------------------------------------------------------------------
# Build the eight headline indicators
# ------------------------------------------------------------------------------

cat("[1/3] Computing compact species indicators...\n")

niches <- monthly_wide[, .(
  # Temperature indicators are means or extremes of the 12 monthly range means.
  tmean_annual_mean = mean(tmean),
  tmean_warmest_month_mean = max(tmean),
  tmean_coldest_month_mean = min(tmean),
  temp_seasonality_mean = max(tmean) - min(tmean),

  # Annual water totals are sums of the 12 monthly range means.
  cwd_annual_sum = sum(def),
  cwd_max_month_mean = max(def),
  pr_annual_sum = sum(pr),
  pr_driest_month_mean = min(pr),

  # QA counts: all should be 12 unless a species has incomplete climate data.
  n_months_tmean = sum(!is.na(tmean)),
  n_months_cwd = sum(!is.na(def)),
  n_months_pr = sum(!is.na(pr))
), by = metadata_cols]

indicator_cols <- c(
  "tmean_annual_mean",
  "tmean_warmest_month_mean",
  "tmean_coldest_month_mean",
  "temp_seasonality_mean",
  "cwd_annual_sum",
  "cwd_max_month_mean",
  "pr_annual_sum",
  "pr_driest_month_mean"
)

niches[, n_indicators_present := count_present(.SD), .SDcols = indicator_cols]
niches[, range_scope := range_scope]
niches[, niche_method := paste0(
  "bien_range_terraclimate_1981_2010_",
  range_scope,
  "_compact_indicators"
)]

setcolorder(niches, c(
  intersect(
    c(
      "species_key", "source_code_system", "source_species_code",
      "scientific_name", "common_name", "community_layers", "bien_query_name",
      "niche_taxon_name", "niche_taxon_key"
    ),
    names(niches)
  ),
  indicator_cols,
  "n_indicators_present",
  "n_months_tmean",
  "n_months_cwd",
  "n_months_pr",
  intersect(c("climate_period", "climate_source", "range_source", "range_scope"), names(niches)),
  "niche_method"
))
setorder(niches, source_code_system, scientific_name, species_key)

write_parquet_safely(as_tibble(niches), out_path, compression = "snappy")

# ------------------------------------------------------------------------------
# QA outputs
# ------------------------------------------------------------------------------

cat("[2/3] Writing QA summaries and rankings...\n")

qa_summary <- rbindlist(lapply(indicator_cols, function(col) {
  values <- niches[[col]]
  data.table(
    indicator = col,
    n_species = sum(!is.na(values)),
    n_missing = sum(is.na(values)),
    value_min = min(values, na.rm = TRUE),
    value_p10 = as.numeric(quantile(values, 0.10, na.rm = TRUE)),
    value_median = median(values, na.rm = TRUE),
    value_p90 = as.numeric(quantile(values, 0.90, na.rm = TRUE)),
    value_max = max(values, na.rm = TRUE)
  )
}), fill = TRUE)

ranking_one_indicator <- function(dt, indicator, n = 20) {
  common_cols <- intersect(
    c("species_key", "scientific_name", "common_name", "community_layers"),
    names(dt)
  )

  high <- dt[!is.na(get(indicator))][order(-get(indicator))]
  high <- head(high, n)
  high <- high[, c(common_cols, indicator), with = FALSE]
  high[, `:=`(indicator = indicator, rank_type = "highest", rank = seq_len(.N))]
  setnames(high, indicator, "value")

  low <- dt[!is.na(get(indicator))][order(get(indicator))]
  low <- head(low, n)
  low <- low[, c(common_cols, indicator), with = FALSE]
  low[, `:=`(indicator = indicator, rank_type = "lowest", rank = seq_len(.N))]
  setnames(low, indicator, "value")

  rbindlist(list(high, low), fill = TRUE)
}

qa_rankings <- rbindlist(
  lapply(indicator_cols, function(col) ranking_one_indicator(niches, col)),
  fill = TRUE
)
setcolorder(qa_rankings, c(
  "indicator", "rank_type", "rank", "value",
  setdiff(names(qa_rankings), c("indicator", "rank_type", "rank", "value"))
))

qa_missing <- niches[
  n_indicators_present < length(indicator_cols) |
    n_months_tmean < 12 |
    n_months_cwd < 12 |
    n_months_pr < 12
]

write_csv_safely(qa_summary, qa_summary_path)
write_csv_safely(qa_rankings, qa_rankings_path)
write_csv_safely(qa_missing, qa_missing_path)

cat("[3/3] Complete.\n\n")
cat(glue("Species niches: {out_path}"), "\n")
cat(glue("QA summary:     {qa_summary_path}"), "\n")
cat(glue("QA rankings:    {qa_rankings_path}"), "\n")
cat(glue("QA missing:     {qa_missing_path}"), "\n")
cat(glue("Species:        {format(nrow(niches), big.mark = ',')}"), "\n")
cat(glue("Indicators:     {length(indicator_cols)}"), "\n")
