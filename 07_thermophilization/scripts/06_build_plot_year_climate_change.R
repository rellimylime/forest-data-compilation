# ==============================================================================
# 06_build_plot_year_climate_change.R
# Build repeated-survey climate-affinity change metrics.
#
# This script compares consecutive FIA survey years for each stable plot and
# community layer. It calculates change and annualized rate of change in
# community-weighted species climate-affinity metrics, then joins disturbance
# proportions for the current survey year.
#
# Supported layers:
#   seedlings, saplings, trees
#
# Output grain:
#   one row per community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN
#
# Usage:
#   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=seedlings
#   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=saplings
#   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=trees
#   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=seedlings --limit=1000
#
# Documentation:
#   07_thermophilization/README.md#script-06-inputs-and-outputs
#   07_thermophilization/README.md#plot_year_climate_change_layerparquet
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

min_niche_coverage <- as.numeric(get_arg("--min-niche-coverage", "0.95"))
if (is.na(min_niche_coverage) || min_niche_coverage < 0 || min_niche_coverage > 1) {
  stop("--min-niche-coverage must be between 0 and 1.")
}

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
thermo_config <- config$processed$thermophilization
thermo_dir <- here(thermo_config$output_dir)

smoke_data_dir <- here("07_thermophilization/data/smoke")
qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}

plot_year_filename <- switch(
  layer,
  seedlings = thermo_config$files$plot_year_community_cwm_seedlings,
  saplings = thermo_config$files$plot_year_community_cwm_saplings,
  trees = thermo_config$files$plot_year_community_cwm_trees
)
if (is.null(plot_year_filename) || !nzchar(plot_year_filename)) {
  plot_year_filename <- sprintf("plot_year_community_cwm_%s.parquet", layer)
}

change_filename <- switch(
  layer,
  seedlings = thermo_config$files$plot_year_climate_change_seedlings,
  saplings = thermo_config$files$plot_year_climate_change_saplings,
  trees = thermo_config$files$plot_year_climate_change_trees
)
if (is.null(change_filename) || !nzchar(change_filename)) {
  change_filename <- sprintf("plot_year_climate_change_%s.parquet", layer)
}
input_path <- file.path(thermo_dir, plot_year_filename)
severity_path <- file.path(thermo_dir, thermo_config$files$plot_disturbance_severity)
out_path <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("plot_year_climate_change_%s_limit_%d.parquet", layer, limit_arg)
  } else {
    change_filename
  }
)

qa_suffix <- if (is_smoke_run) sprintf("%s_limit_%d", layer, limit_arg) else layer
summary_path <- file.path(qa_dir, sprintf("plot_year_climate_change_summary_%s.csv", qa_suffix))
by_class_path <- file.path(qa_dir, sprintf("plot_year_climate_change_by_disturbance_%s.csv", qa_suffix))

dir_create(if (is_smoke_run) smoke_data_dir else thermo_dir)
dir_create(qa_dir)

required_files <- c(input_path, severity_path)
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

count_section <- function(dt, column, section_name) {
  out <- dt[, .N, by = .(category = get(column))]
  out[, section := section_name]
  setcolorder(out, c("section", "category", "N"))
  out[order(section, -N, category)]
}

# ------------------------------------------------------------------------------
# Load inputs
# ------------------------------------------------------------------------------

cat("Plot-Year Climate Change Build\n")
cat("==============================\n\n")
cat(glue("Community layer input: {input_path}"), "\n")
cat(glue("Disturbance input:     {severity_path}"), "\n")
cat(glue("Output:                {out_path}"), "\n\n")

plot_year <- as.data.table(read_parquet(input_path))
severity <- as.data.table(read_parquet(severity_path))

if (is_smoke_run) {
  keep_plots <- unique(plot_year$stable_plot_id)[seq_len(min(limit_arg, uniqueN(plot_year$stable_plot_id)))]
  plot_year <- plot_year[stable_plot_id %in% keep_plots]
}

required_plot_cols <- c(
  "community_layer", "stable_plot_id", "PLT_CN", "INVYR", "state",
  "STATECD", "UNITCD", "COUNTYCD", "PLOT", "LAT", "LON", "ELEV",
  "pct_forested", "n_species_total", "plot_community_weight_total",
  "frac_weight_with_niche"
)
missing_plot_cols <- setdiff(required_plot_cols, names(plot_year))
if (length(missing_plot_cols) > 0) {
  stop(glue("Plot-year CWM table is missing required column(s): {paste(missing_plot_cols, collapse = ', ')}"))
}

metric_cols <- c(
  "cwm_temp", "cwm_heat", "cwm_cold", "cwm_temp_seasonality",
  "cwm_cwd", "cwm_peak_cwd", "cwm_pr", "cwm_dry_month_pr",
  "median_temp", "median_heat", "median_cold", "median_temp_seasonality",
  "median_cwd", "median_peak_cwd", "median_pr", "median_dry_month_pr"
)
missing_metric_cols <- setdiff(metric_cols, names(plot_year))
if (length(missing_metric_cols) > 0) {
  stop(glue("Plot-year CWM table is missing metric column(s): {paste(missing_metric_cols, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Build consecutive survey intervals
# ------------------------------------------------------------------------------

setorder(plot_year, stable_plot_id, INVYR, PLT_CN)

shift_cols <- c(
  "PLT_CN", "INVYR", "pct_forested", "n_species_total",
  "plot_community_weight_total", "frac_weight_with_niche", metric_cols
)
for (col in shift_cols) {
  plot_year[, paste0("previous_", col) := shift(get(col), type = "lag"), by = stable_plot_id]
}

plot_year[, years_between_surveys := INVYR - previous_INVYR]
changes <- plot_year[!is.na(previous_PLT_CN) & years_between_surveys > 0]

if (nrow(changes) == 0) {
  stop("No repeated survey intervals were found for the selected layer.")
}

for (metric in metric_cols) {
  delta_col <- paste0("delta_", metric)
  rate_col <- paste0("rate_", metric, "_per_year")
  previous_col <- paste0("previous_", metric)
  changes[, (delta_col) := get(metric) - get(previous_col)]
  changes[, (rate_col) := get(delta_col) / years_between_surveys]
}

changes[, `:=`(
  current_PLT_CN = PLT_CN,
  current_INVYR = INVYR,
  current_pct_forested = pct_forested,
  current_n_species_total = n_species_total,
  current_plot_community_weight_total = plot_community_weight_total,
  current_frac_weight_with_niche = frac_weight_with_niche,
  previous_frac_weight_with_niche = previous_frac_weight_with_niche,
  meets_niche_coverage_threshold = (
    !is.na(frac_weight_with_niche) &
      !is.na(previous_frac_weight_with_niche) &
      frac_weight_with_niche >= min_niche_coverage &
      previous_frac_weight_with_niche >= min_niche_coverage
  ),
  min_niche_coverage_threshold = min_niche_coverage
)]

# ------------------------------------------------------------------------------
# Join current-year disturbance proportions
# ------------------------------------------------------------------------------

severity_keep_cols <- intersect(
  c(
    "stable_plot_id", "PLT_CN", "INVYR", "region_east_west",
    "is_forest_dominated_plot", "n_conditions", "total_condition_prop",
    "forested_condition_prop", "prop_any_recorded_disturbance",
    "prop_any_natural_disturbance", "prop_fire", "prop_crown_fire",
    "prop_insect", "prop_disease", "prop_weather", "prop_wind",
    "prop_drought", "prop_other_weather", "prop_other_natural",
    "prop_human_or_harvest", "prop_any_treatment",
    "prop_cutting_treatment", "forested_prop_fire",
    "forested_prop_crown_fire", "forested_prop_insect",
    "forested_prop_disease", "forested_prop_weather",
    "disturbance_year_latest", "disturbance_year_earliest",
    "has_continuous_disturbance_year", "dominant_disturbance_class",
    "dominant_disturbance_prop", "any_fire", "any_crown_fire",
    "any_insect", "any_disease", "any_weather",
    "any_natural_disturbance", "any_human_or_harvest",
    "has_mixed_natural_disturbance", "fire_severity_class",
    "plot_disturbance_extent_class", "condition_prop_quality_flag"
  ),
  names(severity)
)
severity <- severity[, ..severity_keep_cols]

changes <- merge(
  changes,
  severity,
  by.x = c("stable_plot_id", "current_PLT_CN", "current_INVYR"),
  by.y = c("stable_plot_id", "PLT_CN", "INVYR"),
  all.x = TRUE,
  sort = FALSE
)

changes[, disturbance_interval_role := fifelse(
  any_natural_disturbance == TRUE,
  "current_visit_records_disturbance",
  "no_current_visit_natural_disturbance"
)]
changes[, disturbance_within_interval := fifelse(
  !is.na(disturbance_year_latest) &
    !is.na(previous_INVYR) &
    disturbance_year_latest > previous_INVYR &
    disturbance_year_latest <= current_INVYR,
  TRUE,
  FALSE,
  na = FALSE
)]

# ------------------------------------------------------------------------------
# Select and order final columns
# ------------------------------------------------------------------------------

identity_cols <- c(
  "community_layer", "stable_plot_id", "state", "STATECD", "UNITCD",
  "COUNTYCD", "PLOT", "previous_PLT_CN", "current_PLT_CN",
  "previous_INVYR", "current_INVYR", "years_between_surveys",
  "LAT", "LON", "ELEV", "region_east_west"
)

context_cols <- c(
  "previous_pct_forested", "current_pct_forested",
  "previous_n_species_total", "current_n_species_total",
  "previous_plot_community_weight_total", "current_plot_community_weight_total",
  "previous_frac_weight_with_niche", "current_frac_weight_with_niche",
  "meets_niche_coverage_threshold", "min_niche_coverage_threshold"
)

delta_cols <- unlist(lapply(metric_cols, function(metric) {
  c(paste0("previous_", metric), metric, paste0("delta_", metric), paste0("rate_", metric, "_per_year"))
}))

disturbance_cols <- intersect(
  c(
    "disturbance_interval_role", "disturbance_within_interval",
    "dominant_disturbance_class", "dominant_disturbance_prop",
    "plot_disturbance_extent_class", "fire_severity_class",
    "prop_any_natural_disturbance", "prop_fire", "prop_crown_fire",
    "prop_insect", "prop_disease", "prop_weather", "prop_human_or_harvest",
    "disturbance_year_latest", "disturbance_year_earliest",
    "has_continuous_disturbance_year", "condition_prop_quality_flag"
  ),
  names(changes)
)

final_cols <- c(
  identity_cols,
  context_cols,
  delta_cols,
  disturbance_cols
)
final_cols <- intersect(final_cols, names(changes))
changes <- changes[, ..final_cols]
setorder(changes, community_layer, stable_plot_id, current_INVYR, current_PLT_CN)

# ------------------------------------------------------------------------------
# QA outputs
# ------------------------------------------------------------------------------

summary <- data.table(
  metric = c(
    "n_change_rows",
    "n_stable_plots_with_change",
    "median_years_between_surveys",
    "n_rows_meeting_niche_coverage_threshold",
    "n_rows_with_current_visit_natural_disturbance",
    "n_rows_with_disturbance_year_within_interval",
    "median_delta_cwm_temp",
    "median_rate_cwm_temp_per_year",
    "median_delta_cwm_cwd",
    "median_rate_cwm_cwd_per_year"
  ),
  value = c(
    nrow(changes),
    uniqueN(changes$stable_plot_id),
    stats::median(changes$years_between_surveys, na.rm = TRUE),
    sum(changes$meets_niche_coverage_threshold, na.rm = TRUE),
    sum(changes$disturbance_interval_role == "current_visit_records_disturbance", na.rm = TRUE),
    sum(changes$disturbance_within_interval, na.rm = TRUE),
    stats::median(changes$delta_cwm_temp, na.rm = TRUE),
    stats::median(changes$rate_cwm_temp_per_year, na.rm = TRUE),
    stats::median(changes$delta_cwm_cwd, na.rm = TRUE),
    stats::median(changes$rate_cwm_cwd_per_year, na.rm = TRUE)
  )
)
summary[, `:=`(
  community_layer = layer,
  min_niche_coverage = min_niche_coverage,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]

by_class <- rbindlist(list(
  count_section(changes, "dominant_disturbance_class", "dominant_disturbance_class"),
  count_section(changes, "plot_disturbance_extent_class", "plot_disturbance_extent_class"),
  count_section(changes, "fire_severity_class", "fire_severity_class"),
  count_section(changes, "disturbance_interval_role", "disturbance_interval_role")
), fill = TRUE)
by_class[, `:=`(
  community_layer = layer,
  min_niche_coverage = min_niche_coverage,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]
setcolorder(by_class, c("community_layer", "section", "category", "N", "min_niche_coverage", "smoke_limit"))

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

write_parquet_safely(changes, out_path)
write_csv_safely(summary, summary_path)
write_csv_safely(by_class, by_class_path)

cat("\nDone.\n")
cat(glue("Change parquet: {out_path}"), "\n")
cat(glue("QA summary:     {summary_path}"), "\n")
cat(glue("QA by class:    {by_class_path}"), "\n\n")
print(summary)
