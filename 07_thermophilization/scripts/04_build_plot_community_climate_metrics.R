# ==============================================================================
# 04_build_plot_community_climate_metrics.R
# Build condition-level climate-affinity metrics for FIA community layers.
#
# This generalizes the seedling recruitment CWM workflow to additional FIA
# community layers. It computes both weighted means and weighted medians of
# species climate niche indicators for each FIA condition.
#
# Supported layers:
#   seedlings  -> plot_seedling_species.parquet, default weight seedlings_tpa
#   saplings   -> plot_sapling_species.parquet,  default weight abundance_for_cwm
#   trees      -> plot_tree_species.parquet,     default weight abundance_for_cwm
#
# Output grain:
#   one row per community_layer x stable_plot_id x PLT_CN x INVYR x CONDID
#
# Usage:
#   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=seedlings
#   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=saplings
#   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=trees
#   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=trees --weight=ba_per_acre
#   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=seedlings --limit=100
#
# Documentation:
#   07_thermophilization/README.md#script-04-inputs-and-outputs
#   07_thermophilization/README.md#plot_community_climate_layerparquet
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

# ------------------------------------------------------------------------------
# Command line options
# ------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  # Support both --flag=value and --flag value.
  eq_hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(eq_hit) > 0) {
    return(sub(paste0("^", flag, "="), "", eq_hit[[1]]))
  }

  flag_pos <- which(args == flag)
  if (length(flag_pos) > 0 && flag_pos[[1]] < length(args)) {
    return(args[[flag_pos[[1]] + 1]])
  }

  default
}

layer <- get_arg("--layer", "seedlings")
allowed_layers <- c("seedlings", "saplings", "trees")
if (!layer %in% allowed_layers) {
  stop(glue("--layer must be one of: {paste(allowed_layers, collapse = ', ')}"))
}

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)

range_scope <- get_arg("--range-scope", "us_study_area_with_global_fallback")
allowed_range_scopes <- c("global", "us_study_area", "us_study_area_with_global_fallback")
if (!range_scope %in% allowed_range_scopes) {
  stop(glue("--range-scope must be one of: {paste(allowed_range_scopes, collapse = ', ')}"))
}

default_weight <- switch(
  layer,
  seedlings = "seedlings_tpa",
  saplings = "abundance_for_cwm",
  trees = "abundance_for_cwm"
)
weight_col <- get_arg("--weight", default_weight)

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
fia_summary_dir <- here(config$processed$fia$summaries$output_dir)
niche_dir <- here(config$processed$species_niches$output_dir)
thermo_dir <- here(config$processed$thermophilization$output_dir)

smoke_data_dir <- here("07_thermophilization/data/smoke")
qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}

community_filename <- switch(
  layer,
  seedlings = "plot_seedling_species.parquet",
  saplings = "plot_sapling_species.parquet",
  trees = "plot_tree_species.parquet"
)
community_path <- file.path(fia_summary_dir, community_filename)

study_area_niche_path <- file.path(niche_dir, "species_climate_niches_us_study_area.parquet")
global_niche_path <- file.path(niche_dir, config$processed$species_niches$files$species_climate_niches)
availability_path <- file.path(niche_dir, config$processed$species_niches$files$bien_range_availability)

niche_path <- switch(
  range_scope,
  global = global_niche_path,
  us_study_area = study_area_niche_path,
  us_study_area_with_global_fallback = study_area_niche_path
)

out_filename <- switch(
  layer,
  seedlings = config$processed$thermophilization$files$plot_community_climate_seedlings,
  saplings = config$processed$thermophilization$files$plot_community_climate_saplings,
  trees = config$processed$thermophilization$files$plot_community_climate_trees
)
if (is.null(out_filename) || !nzchar(out_filename)) {
  out_filename <- sprintf("plot_community_climate_%s.parquet", layer)
}

out_file <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("plot_community_climate_%s_%s_limit_%d.parquet", layer, range_scope, limit_arg)
  } else {
    out_filename
  }
)

qa_suffix <- if (is_smoke_run) {
  sprintf("%s_%s_limit_%d", layer, range_scope, limit_arg)
} else {
  layer
}

summary_file <- file.path(qa_dir, sprintf("plot_community_climate_summary_%s.csv", qa_suffix))
missing_species_file <- file.path(qa_dir, sprintf("plot_community_climate_missing_species_%s.csv", qa_suffix))
coverage_file <- file.path(qa_dir, sprintf("plot_community_climate_coverage_by_state_%s.csv", qa_suffix))

dir_create(thermo_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)

required_files <- c(community_path, niche_path)
if (range_scope == "us_study_area_with_global_fallback") {
  required_files <- c(required_files, global_niche_path, availability_path)
}
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(glue("Required input file(s) not found: {paste(missing_files, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

write_parquet_safely <- function(df, path, compression = "snappy") {
  # Write through a temporary file so interrupted reruns keep the last complete output.
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

  fwrite(df, tmp_path)
  file_copy(tmp_path, path, overwrite = TRUE)
}

sum_or_na <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  sum(x, na.rm = TRUE)
}

first_nonmissing <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)
  x[[1]]
}

first_nonmissing_character <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

weighted_mean_or_na <- function(x, w) {
  ok <- !is.na(x) & !is.na(w) & w > 0
  if (!any(ok)) return(NA_real_)
  stats::weighted.mean(x[ok], w[ok])
}

weighted_median_or_na <- function(x, w) {
  ok <- !is.na(x) & !is.na(w) & w > 0
  if (!any(ok)) return(NA_real_)

  x <- x[ok]
  w <- w[ok]
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]

  cutoff <- sum(w) / 2
  x[which(cumsum(w) >= cutoff)[1]]
}

weighted_median_table <- function(dt, key_col, value_col, out_col) {
  # Compute weighted medians for one indicator using one sorted grouped pass.
  sub <- dt[
    !is.na(get(value_col)) & !is.na(community_weight) & community_weight > 0,
    .(
      metric_condition_key = get(key_col),
      indicator_value = as.numeric(get(value_col)),
      community_weight = as.numeric(community_weight)
    )
  ]

  if (nrow(sub) == 0) {
    out <- data.table(metric_condition_key = character(), value = numeric())
    setnames(out, "value", out_col)
    return(out)
  }

  setorder(sub, metric_condition_key, indicator_value)
  sub[, total_weight := sum(community_weight), by = metric_condition_key]
  sub[, cumulative_weight := cumsum(community_weight), by = metric_condition_key]
  out <- sub[
    cumulative_weight >= total_weight / 2,
    .SD[1],
    by = metric_condition_key
  ][, .(metric_condition_key, value = indicator_value)]
  setnames(out, "value", out_col)
  out
}

# ------------------------------------------------------------------------------
# Load inputs
# ------------------------------------------------------------------------------

cat("Plot Community Climate Metrics Build\n")
cat("====================================\n\n")
cat(glue("Community layer:      {layer}"), "\n")
cat(glue("Community input:      {community_path}"), "\n")
cat(glue("Species niche input:  {niche_path}"), "\n")
if (range_scope == "us_study_area_with_global_fallback") {
  cat(glue("Global fallback:      {global_niche_path}"), "\n")
}
cat(glue("Range scope:          {range_scope}"), "\n")
cat(glue("Weighting:            {weight_col}"), "\n")
cat(glue("Output:               {out_file}"), "\n\n")

community <- as.data.table(read_parquet(community_path))
niches <- as.data.table(read_parquet(niche_path))

if (range_scope == "us_study_area_with_global_fallback") {
  availability <- as.data.table(read_parquet(availability_path))
  current_bien_available_keys <- availability[
    as.logical(bien_range_available) == TRUE,
    unique(species_key)
  ]

  global_niches <- as.data.table(read_parquet(global_niche_path))
  global_fallback <- global_niches[
    species_key %in% current_bien_available_keys &
      !species_key %in% niches$species_key
  ]

  niches[, `:=`(
    niche_scope_used = "us_study_area",
    niche_fallback_reason = NA_character_
  )]
  global_fallback[, `:=`(
    niche_scope_used = "global_fallback",
    niche_fallback_reason = "no_study_area_niche"
  )]
  niches <- rbindlist(list(niches, global_fallback), fill = TRUE, use.names = TRUE)
} else {
  niches[, `:=`(
    niche_scope_used = range_scope,
    niche_fallback_reason = NA_character_
  )]
}

if (!"SPCD" %in% names(community)) {
  stop("Community table must contain SPCD.")
}
if (!"species_key" %in% names(community)) {
  community[, species_key := paste0("fia_spcd:", as.integer(SPCD))]
}
if (!weight_col %in% names(community) && weight_col != "presence") {
  stop(glue("Weight column not found in {community_filename}: {weight_col}"))
}

if (is_smoke_run) {
  # Limit by full condition so each smoke row keeps complete community composition.
  community[, condition_key := paste(PLT_CN, INVYR, CONDID, sep = "|")]
  keep_conditions <- unique(community$condition_key)[seq_len(min(limit_arg, uniqueN(community$condition_key)))]
  community <- community[condition_key %in% keep_conditions]
}

# ------------------------------------------------------------------------------
# Collapse to one species row per condition
# ------------------------------------------------------------------------------

metadata_cols <- intersect(
  c(
    "stable_plot_id", "PLT_CN", "INVYR", "CONDID", "state", "STATECD",
    "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN", "LAT", "LON", "ELEV",
    "FORTYPCD", "forest_type_label", "forest_type_group",
    "COND_STATUS_CD", "CONDPROP_UNADJ", "pct_forested",
    "is_forested_condition", "has_fire_condition", "has_crown_fire_condition",
    "has_insect_condition", "has_disease_condition", "has_wind_condition",
    "has_drought_condition", "has_human_dist_condition", "has_cutting_treatment"
  ),
  names(community)
)

species_cols <- intersect(
  c("SPCD", "species_key", "SCIENTIFIC_NAME", "COMMON_NAME", "GENUS", "SPECIES"),
  names(community)
)

condition_subplot_counts <- community[
  ,
  .(n_subplots_with_layer = uniqueN(SUBP)),
  by = metadata_cols
]

if (weight_col == "presence") {
  community[, community_weight := 1]
} else {
  community[, community_weight := as.numeric(get(weight_col))]
}
community[is.na(community_weight) | community_weight < 0, community_weight := 0]

community_species <- community[
  ,
  .(
    community_weight = sum_or_na(community_weight),
    n_source_rows = .N,
    ba_per_acre = if ("ba_per_acre" %in% names(.SD)) sum_or_na(ba_per_acre) else NA_real_,
    n_trees_tpa = if ("n_trees_tpa" %in% names(.SD)) sum_or_na(n_trees_tpa) else NA_real_,
    treecount_total = if ("treecount_total" %in% names(.SD)) sum_or_na(treecount_total) else NA_real_,
    seedlings_tpa = if ("seedlings_tpa" %in% names(.SD)) sum_or_na(seedlings_tpa) else NA_real_
  ),
  by = c(metadata_cols, species_cols)
]

# ------------------------------------------------------------------------------
# Join species climate niches
# ------------------------------------------------------------------------------

indicator_map <- c(
  temp = "tmean_annual_mean",
  heat = "tmean_warmest_month_mean",
  cold = "tmean_coldest_month_mean",
  temp_seasonality = "temp_seasonality_mean",
  cwd = "cwd_annual_sum",
  peak_cwd = "cwd_max_month_mean",
  pr = "pr_annual_sum",
  dry_month_pr = "pr_driest_month_mean"
)

missing_indicator_cols <- setdiff(unname(indicator_map), names(niches))
if (length(missing_indicator_cols) > 0) {
  stop(glue("Niche table is missing indicator(s): {paste(missing_indicator_cols, collapse = ', ')}"))
}

niche_keep_cols <- intersect(
  c(
    "species_key", "source_code_system", "source_species_code",
    "scientific_name", "common_name", "community_layers",
    "climate_period", "climate_source", "range_source", "range_scope",
    "niche_taxon_name", "niche_taxon_key",
    "niche_scope_used", "niche_fallback_reason", "niche_method",
    unname(indicator_map)
  ),
  names(niches)
)

fia_niches <- niches[source_code_system == "fia_spcd", ..niche_keep_cols]
joined <- merge(community_species, fia_niches, by = "species_key", all.x = TRUE)
joined[, has_niche := !is.na(tmean_annual_mean)]
joined[, metric_condition_key := paste(stable_plot_id, PLT_CN, INVYR, CONDID, sep = "|")]

# ------------------------------------------------------------------------------
# Aggregate to condition-level weighted means and medians
# ------------------------------------------------------------------------------

condition_cols <- metadata_cols
condition_group_cols <- c("metric_condition_key", condition_cols)

metrics <- joined[
  ,
  .(
    n_species_total = uniqueN(SPCD),
    n_species_with_niche = uniqueN(SPCD[has_niche]),
    n_source_rows = sum(n_source_rows, na.rm = TRUE),
    community_weight_total = sum(community_weight, na.rm = TRUE),
    community_weight_with_niche = sum(ifelse(has_niche, community_weight, 0.0), na.rm = TRUE),
    community_weight_with_study_area_niche = sum(ifelse(has_niche & niche_scope_used == "us_study_area", community_weight, 0.0), na.rm = TRUE),
    community_weight_with_global_fallback_niche = sum(ifelse(has_niche & niche_scope_used == "global_fallback", community_weight, 0.0), na.rm = TRUE),

    mean_temp = weighted_mean_or_na(tmean_annual_mean, community_weight),
    mean_heat = weighted_mean_or_na(tmean_warmest_month_mean, community_weight),
    mean_cold = weighted_mean_or_na(tmean_coldest_month_mean, community_weight),
    mean_temp_seasonality = weighted_mean_or_na(temp_seasonality_mean, community_weight),
    mean_cwd = weighted_mean_or_na(cwd_annual_sum, community_weight),
    mean_peak_cwd = weighted_mean_or_na(cwd_max_month_mean, community_weight),
    mean_pr = weighted_mean_or_na(pr_annual_sum, community_weight),
    mean_dry_month_pr = weighted_mean_or_na(pr_driest_month_mean, community_weight),

    climate_period = first_nonmissing_character(climate_period),
    climate_source = first_nonmissing_character(climate_source),
    range_source = first_nonmissing_character(range_source),
    niche_scopes_used = paste(sort(unique(niche_scope_used[has_niche])), collapse = ";"),
    niche_method = first_nonmissing_character(niche_method)
  ),
  by = condition_group_cols
]

median_tables <- lapply(names(indicator_map), function(short_name) {
  weighted_median_table(
    joined,
    key_col = "metric_condition_key",
    value_col = indicator_map[[short_name]],
    out_col = paste0("median_", short_name)
  )
})
for (median_dt in median_tables) {
  metrics <- merge(metrics, median_dt, by = "metric_condition_key", all.x = TRUE, sort = FALSE)
}

metrics <- merge(metrics, condition_subplot_counts, by = condition_cols, all.x = TRUE)

metrics[, `:=`(
  community_layer = layer,
  weight_column = weight_col,
  range_scope = range_scope,
  frac_weight_with_niche = fifelse(
    community_weight_total > 0,
    community_weight_with_niche / community_weight_total,
    NA_real_
  ),
  frac_species_with_niche = fifelse(
    n_species_total > 0,
    n_species_with_niche / n_species_total,
    NA_real_
  ),
  frac_weight_with_study_area_niche = fifelse(
    community_weight_total > 0,
    community_weight_with_study_area_niche / community_weight_total,
    NA_real_
  ),
  frac_weight_with_global_fallback_niche = fifelse(
    community_weight_total > 0,
    community_weight_with_global_fallback_niche / community_weight_total,
    NA_real_
  )
)]

setcolorder(
  metrics,
  c(
    "community_layer", condition_cols, "weight_column", "range_scope",
    "niche_scopes_used", "climate_period", "climate_source", "range_source",
    "niche_method",
    setdiff(names(metrics), c(
      "community_layer", condition_cols, "weight_column", "range_scope",
      "niche_scopes_used", "climate_period", "climate_source", "range_source",
      "niche_method"
    ))
  )
)
metrics[, metric_condition_key := NULL]
setorder(metrics, stable_plot_id, INVYR, PLT_CN, CONDID)

# ------------------------------------------------------------------------------
# QA outputs
# ------------------------------------------------------------------------------

summary <- data.table(
  metric = c(
    "n_condition_rows",
    "n_condition_rows_with_mean_temp",
    "n_community_species_rows",
    "n_unique_species",
    "n_unique_species_missing_niche",
    "median_frac_weight_with_niche",
    "p10_frac_weight_with_niche",
    "n_condition_rows_below_95pct_niche_coverage",
    "n_condition_rows_using_global_fallback",
    "median_frac_weight_with_global_fallback_niche"
  ),
  value = c(
    nrow(metrics),
    sum(!is.na(metrics$mean_temp)),
    nrow(community_species),
    uniqueN(community_species$SPCD),
    uniqueN(joined[has_niche == FALSE, SPCD]),
    stats::median(metrics$frac_weight_with_niche, na.rm = TRUE),
    as.numeric(stats::quantile(metrics$frac_weight_with_niche, 0.10, na.rm = TRUE)),
    sum(metrics$frac_weight_with_niche < 0.95, na.rm = TRUE),
    sum(metrics$community_weight_with_global_fallback_niche > 0, na.rm = TRUE),
    stats::median(metrics$frac_weight_with_global_fallback_niche, na.rm = TRUE)
  )
)
summary[, `:=`(
  community_layer = layer,
  range_scope = range_scope,
  weight_column = weight_col,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]

coverage_by_state <- metrics[
  ,
  .(
    n_condition_rows = .N,
    n_condition_rows_with_mean_temp = sum(!is.na(mean_temp)),
    median_frac_weight_with_niche = stats::median(frac_weight_with_niche, na.rm = TRUE),
    p10_frac_weight_with_niche = as.numeric(stats::quantile(frac_weight_with_niche, 0.10, na.rm = TRUE)),
    n_condition_rows_below_95pct_niche_coverage = sum(frac_weight_with_niche < 0.95, na.rm = TRUE),
    n_condition_rows_using_global_fallback = sum(community_weight_with_global_fallback_niche > 0, na.rm = TRUE)
  ),
  by = state
][order(state)]

missing_species <- joined[
  has_niche == FALSE,
  .(
    scientific_name = first_nonmissing(SCIENTIFIC_NAME),
    common_name = first_nonmissing(COMMON_NAME),
    n_condition_rows = uniqueN(paste(PLT_CN, INVYR, CONDID, sep = "|")),
    community_weight_total = sum(community_weight, na.rm = TRUE),
    n_source_rows = sum(n_source_rows, na.rm = TRUE)
  ),
  by = .(species_key, SPCD)
][order(-community_weight_total, species_key)]

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

write_parquet_safely(metrics, out_file)
write_csv_safely(summary, summary_file)
write_csv_safely(coverage_by_state, coverage_file)
write_csv_safely(missing_species, missing_species_file)

cat("\nDone.\n")
cat(glue("Community climate parquet: {out_file}"), "\n")
cat(glue("QA summary:                {summary_file}"), "\n")
cat(glue("QA by state:               {coverage_file}"), "\n")
cat(glue("Missing species:           {missing_species_file}"), "\n\n")
print(summary)
