#!/usr/bin/env Rscript

# ==============================================================================
# 04_build_visit_interval_change.R
# Change in community climate affinity between consecutive FIA surveys.
#
# One row per remeasurement interval: each survey of a stable plot is compared
# with the survey immediately before it. A plot measured in 2002, 2012, and 2022
# yields two rows (2002->2012 and 2012->2022). The companion product
# 05_build_first_last_change.R instead compares only the earliest and latest
# survey, giving one row per plot. Choosing between the two designs is an open
# analysis decision; both are built so the choice stays open.
#
# Input is the forest-only plot-visit CWM, so nonforest conditions are already
# excluded and forest conditions are area-weighted within each visit.
#
# Supported layers:
#   seedlings, saplings, trees
#
# Output grain:
#   one row per community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN
#
# Usage:
#   Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=seedlings
#   Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=saplings
#   Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=trees
#   Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=seedlings --limit=1000
#
# Documentation:
#   07_thermophilization/README.md
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
fia_config <- config$processed$fia
thermo_dir <- here(thermo_config$output_dir)
summary_dir <- here(fia_config$summaries$output_dir)

smoke_data_dir <- here("07_thermophilization/data/smoke")
qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}

input_key <- paste0("forest_plot_visit_cwm_", layer)
input_filename <- thermo_config$files[[input_key]]
if (is.null(input_filename) || !nzchar(input_filename)) {
  input_filename <- sprintf("forest_plot_visit_cwm_%s.parquet", layer)
}

change_key <- paste0("forest_visit_interval_change_", layer)
change_filename <- thermo_config$files[[change_key]]
if (is.null(change_filename) || !nzchar(change_filename)) {
  change_filename <- sprintf("forest_visit_interval_change_%s.parquet", layer)
}

input_path <- file.path(thermo_dir, input_filename)
severity_path <- file.path(thermo_dir, thermo_config$files$plot_disturbance_severity)
context_path <- file.path(summary_dir, fia_config$summaries$files$plot_visit_context)
out_path <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("forest_visit_interval_change_%s_limit_%d.parquet", layer, limit_arg)
  } else {
    change_filename
  }
)

qa_suffix <- if (is_smoke_run) sprintf("%s_limit_%d", layer, limit_arg) else layer
summary_path <- file.path(qa_dir, sprintf("forest_visit_interval_change_summary_%s.csv", qa_suffix))
by_class_path <- file.path(qa_dir, sprintf("forest_visit_interval_change_by_disturbance_%s.csv", qa_suffix))
linkage_path <- file.path(qa_dir, sprintf("forest_visit_interval_change_linkage_%s.csv", qa_suffix))

dir_create(if (is_smoke_run) smoke_data_dir else thermo_dir)
dir_create(qa_dir)

required_files <- c(input_path, severity_path, context_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(glue(
    "Required input file(s) not found: {paste(missing_files, collapse = ', ')}\n",
    "Run: Rscript 07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R"
  ))
}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

count_section <- function(dt, column, section_name) {
  out <- dt[, .N, by = .(category = get(column))]
  out[, section := section_name]
  setcolorder(out, c("section", "category", "N"))
  out[order(section, -N, category)]
}

# ------------------------------------------------------------------------------
# Load inputs
# ------------------------------------------------------------------------------

cat("Forest Visit Interval Change Build\n")
cat("==================================\n\n")
cat(glue("Community layer input: {input_path}"), "\n")
cat(glue("Disturbance input:     {severity_path}"), "\n")
cat(glue("Visit context input:   {context_path}"), "\n")
cat(glue("Output:                {out_path}"), "\n\n")

visits <- as.data.table(read_parquet(input_path))
severity <- as.data.table(read_parquet(severity_path))

if (is_smoke_run) {
  keep_plots <- unique(visits$stable_plot_id)[seq_len(min(limit_arg, uniqueN(visits$stable_plot_id)))]
  visits <- visits[stable_plot_id %in% keep_plots]
}

required_plot_cols <- c(
  "community_layer", "stable_plot_id", "PLT_CN", "INVYR", "PREV_PLT_CN",
  "state", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "LAT", "LON", "ELEV",
  "forested_plot_proportion", "n_forested_conditions_with_layer",
  "forested_condition_weight_with_layer", "frac_weight_with_niche"
)
missing_plot_cols <- setdiff(required_plot_cols, names(visits))
if (length(missing_plot_cols) > 0) {
  stop(glue(
    "Forest plot-visit CWM is missing required column(s): ",
    "{paste(missing_plot_cols, collapse = ', ')}"
  ))
}

metric_cols <- c(
  paste0("mean_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  )),
  paste0("median_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  ))
)
missing_metric_cols <- setdiff(metric_cols, names(visits))
if (length(missing_metric_cols) > 0) {
  stop(glue("Forest plot-visit CWM is missing metric column(s): {paste(missing_metric_cols, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Order visits by measurement date
# ------------------------------------------------------------------------------
# Interval direction decides the sign of every change value. Order on the
# recorded measurement date rather than INVYR alone, so this product and the
# first/last product agree on which survey came first.

context <- as.data.table(read_parquet(context_path))
context <- context[, .(
  PLT_CN, measurement_date_lower, measurement_date_upper, measurement_date_precision
)]
if (anyDuplicated(context$PLT_CN)) stop("Visit context must be unique by PLT_CN.")

visits <- merge(visits, context, by = "PLT_CN", all.x = TRUE, sort = FALSE)
n_undated_visits <- visits[is.na(measurement_date_lower), uniqueN(PLT_CN)]
if (n_undated_visits > 0L) {
  stop(
    format(n_undated_visits, big.mark = ","),
    " plot visit(s) have no measurement date in ", context_path, ".\n",
    "Survey intervals cannot be ordered. Rebuild the visit context: ",
    "Rscript 05_fia/scripts/07_build_plot_visit_context.R"
  )
}

setorder(visits, stable_plot_id, measurement_date_lower, measurement_date_upper, INVYR, PLT_CN)

# ------------------------------------------------------------------------------
# Build consecutive survey intervals
# ------------------------------------------------------------------------------

shift_cols <- c(
  "PLT_CN", "INVYR", "measurement_date_lower",
  "forested_plot_proportion", "n_forested_conditions_with_layer",
  "forested_condition_weight_with_layer", "frac_weight_with_niche", metric_cols
)
for (col in shift_cols) {
  visits[, paste0("previous_", col) := shift(get(col), type = "lag"), by = stable_plot_id]
}

visits[, years_between_surveys := INVYR - previous_INVYR]
visits[, days_between_measurements :=
  as.integer(measurement_date_lower) - as.integer(previous_measurement_date_lower)]

# ------------------------------------------------------------------------------
# Repeated-visit linkage contract
# ------------------------------------------------------------------------------
# A change interval is only valid when FIA's official remeasurement link matches
# the chronologically previous CWM-bearing visit on the same physical plot, i.e.
#   current PREV_PLT_CN == previous (lagged) PLT_CN
# Chronological adjacency alone is NOT a remeasurement link: plots that were
# replaced or re-established at a reused location carry a null or different
# PREV_PLT_CN, and must not be turned into a spurious change interval.
visits[, link_status := fcase(
  is.na(previous_PLT_CN),                     "no_prior_visit_in_layer",
  is.na(PREV_PLT_CN),                         "null_official_link",
  PREV_PLT_CN == previous_PLT_CN,             "official_link_match",
  default =                                   "official_link_mismatch"
)]

# Diagnostics: count each linkage outcome before filtering.
link_diag <- data.table(
  layer = layer,
  n_plot_visit_rows = nrow(visits),
  n_rows_with_prior_visit_in_layer = visits[!is.na(previous_PLT_CN), .N],
  n_official_link_match = visits[link_status == "official_link_match", .N],
  n_null_official_link = visits[link_status == "null_official_link", .N],
  n_official_link_mismatch = visits[link_status == "official_link_mismatch", .N],
  # Prior visit outside the analysis window: FIA records a previous plot but it is
  # not present as a CWM-bearing row for this layer, so no interval can be built.
  n_prior_visit_outside_layer = visits[
    is.na(previous_PLT_CN) & !is.na(PREV_PLT_CN), .N]
)
cat("Repeated-visit linkage (", layer, "):\n", sep = "")
cat(sprintf("  official_link_match      : %d\n", link_diag$n_official_link_match))
cat(sprintf("  null_official_link       : %d (excluded)\n", link_diag$n_null_official_link))
cat(sprintf("  official_link_mismatch   : %d (excluded)\n", link_diag$n_official_link_mismatch))
cat(sprintf("  prior_visit_outside_layer: %d\n", link_diag$n_prior_visit_outside_layer))

# Keep only official, forward-in-time remeasurement intervals.
changes <- visits[
  link_status == "official_link_match" & years_between_surveys > 0
]

if (nrow(changes) == 0) {
  stop("No official-link survey intervals were found for the selected layer.")
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
  current_forested_plot_proportion = forested_plot_proportion,
  current_n_forested_conditions_with_layer = n_forested_conditions_with_layer,
  current_forested_condition_weight_with_layer = forested_condition_weight_with_layer,
  current_frac_weight_with_niche = frac_weight_with_niche,
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
    "fire_disturbance_year_latest", "fire_disturbance_year_earliest",
    "insect_disturbance_year_latest", "insect_disturbance_year_earliest",
    "has_continuous_disturbance_year", "dominant_disturbance_class",
    "dominant_disturbance_prop", "any_fire", "any_crown_fire",
    "any_insect", "any_disease", "any_weather",
    "any_natural_disturbance", "any_human_or_harvest",
    "has_mixed_natural_disturbance", "is_high_severity_fire",
    "high_severity_fire_column", "high_severity_fire_threshold",
    "condition_prop_quality_flag"
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

# Type-specific versions of disturbance_within_interval. The any-type flag
# above can be TRUE because of a disease/weather/etc. event even when no fire
# or insect event occurred, so a fire- or insect-specific pre/post-survey
# question needs its own bracketing check against the type-specific year.
changes[, fire_within_interval := fifelse(
  !is.na(fire_disturbance_year_latest) &
    !is.na(previous_INVYR) &
    fire_disturbance_year_latest > previous_INVYR &
    fire_disturbance_year_latest <= current_INVYR,
  TRUE,
  FALSE,
  na = FALSE
)]
changes[, insect_within_interval := fifelse(
  !is.na(insect_disturbance_year_latest) &
    !is.na(previous_INVYR) &
    insect_disturbance_year_latest > previous_INVYR &
    insect_disturbance_year_latest <= current_INVYR,
  TRUE,
  FALSE,
  na = FALSE
)]

# ------------------------------------------------------------------------------
# Select and order final columns
# ------------------------------------------------------------------------------

identity_cols <- c(
  "community_layer", "stable_plot_id", "state", "STATECD", "UNITCD",
  "COUNTYCD", "PLOT", "previous_PLT_CN", "current_PLT_CN", "PREV_PLT_CN",
  "link_status",
  "previous_INVYR", "current_INVYR", "years_between_surveys",
  "days_between_measurements",
  "LAT", "LON", "ELEV", "region_east_west"
)

context_cols <- c(
  "previous_forested_plot_proportion", "current_forested_plot_proportion",
  "previous_n_forested_conditions_with_layer",
  "current_n_forested_conditions_with_layer",
  "previous_forested_condition_weight_with_layer",
  "current_forested_condition_weight_with_layer",
  "previous_frac_weight_with_niche", "current_frac_weight_with_niche",
  "meets_niche_coverage_threshold", "min_niche_coverage_threshold"
)

delta_cols <- unlist(lapply(metric_cols, function(metric) {
  c(paste0("previous_", metric), metric, paste0("delta_", metric), paste0("rate_", metric, "_per_year"))
}))

disturbance_cols <- intersect(
  c(
    "disturbance_interval_role", "disturbance_within_interval",
    "fire_within_interval", "insect_within_interval",
    "dominant_disturbance_class", "dominant_disturbance_prop",
    "is_high_severity_fire", "high_severity_fire_column", "high_severity_fire_threshold",
    "prop_any_natural_disturbance", "prop_fire", "prop_crown_fire",
    "prop_insect", "prop_disease", "prop_weather", "prop_human_or_harvest",
    "disturbance_year_latest", "disturbance_year_earliest",
    "fire_disturbance_year_latest", "fire_disturbance_year_earliest",
    "insect_disturbance_year_latest", "insect_disturbance_year_earliest",
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
    "n_rows_with_fire_within_interval",
    "n_stable_plots_with_fire_within_interval",
    "n_rows_with_insect_within_interval",
    "n_stable_plots_with_insect_within_interval",
    "median_delta_mean_temp",
    "median_rate_mean_temp_per_year",
    "median_delta_mean_cwd",
    "median_rate_mean_cwd_per_year"
  ),
  value = c(
    nrow(changes),
    uniqueN(changes$stable_plot_id),
    stats::median(changes$years_between_surveys, na.rm = TRUE),
    sum(changes$meets_niche_coverage_threshold, na.rm = TRUE),
    sum(changes$disturbance_interval_role == "current_visit_records_disturbance", na.rm = TRUE),
    sum(changes$disturbance_within_interval, na.rm = TRUE),
    sum(changes$fire_within_interval, na.rm = TRUE),
    uniqueN(changes[fire_within_interval == TRUE]$stable_plot_id),
    sum(changes$insect_within_interval, na.rm = TRUE),
    uniqueN(changes[insect_within_interval == TRUE]$stable_plot_id),
    stats::median(changes$delta_mean_temp, na.rm = TRUE),
    stats::median(changes$rate_mean_temp_per_year, na.rm = TRUE),
    stats::median(changes$delta_mean_cwd, na.rm = TRUE),
    stats::median(changes$rate_mean_cwd_per_year, na.rm = TRUE)
  )
)
summary[, `:=`(
  community_layer = layer,
  min_niche_coverage = min_niche_coverage,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]

by_class <- rbindlist(list(
  count_section(changes, "dominant_disturbance_class", "dominant_disturbance_class"),
  count_section(changes, "is_high_severity_fire", "is_high_severity_fire"),
  count_section(changes, "fire_within_interval", "fire_within_interval"),
  count_section(changes, "insect_within_interval", "insect_within_interval"),
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

write_parquet_atomic(changes, out_path, compression = "snappy")
fwrite(summary, summary_path)
fwrite(by_class, by_class_path)
fwrite(link_diag, linkage_path)

cat("\nDone.\n")
cat(glue("Change parquet: {out_path}"), "\n")
cat(glue("QA summary:     {summary_path}"), "\n")
cat(glue("QA by class:    {by_class_path}"), "\n")
cat(glue("QA linkage:     {linkage_path}"), "\n\n")
print(summary)
