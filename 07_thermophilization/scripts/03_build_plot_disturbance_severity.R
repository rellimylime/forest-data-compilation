# ==============================================================================
# 03_build_plot_disturbance_severity.R
# Build plot-visit disturbance proportions from FIA condition-level classes.
#
# This script aggregates condition-level FIA disturbance classifications to one
# row per plot visit. The output is meant to support analyses that need
# plot-level disturbance intensity, such as proportion of plot area affected by
# fire, insects, disease, or weather.
#
# Input:
#   05_fia/data/processed/summaries/plot_disturbance_classification.parquet
#
# Output grain:
#   one row per stable_plot_id x PLT_CN x INVYR
#
# Usage:
#   Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R
#   Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R --limit=1000
#
# Documentation:
#   07_thermophilization/README.md#script-03-inputs-and-outputs
#   07_thermophilization/README.md#plot_disturbance_severityparquet
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

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
fia_summary_dir <- here(config$processed$fia$summaries$output_dir)
thermo_config <- config$processed$thermophilization
thermo_dir <- here(thermo_config$output_dir)

smoke_data_dir <- here("07_thermophilization/data/smoke")
qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}

disturbance_path <- file.path(
  fia_summary_dir,
  "plot_disturbance_classification.parquet"
)

out_filename <- thermo_config$files$plot_disturbance_severity
if (is.null(out_filename) || !nzchar(out_filename)) {
  out_filename <- "plot_disturbance_severity.parquet"
}
out_path <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("plot_disturbance_severity_limit_%d.parquet", limit_arg)
  } else {
    out_filename
  }
)

qa_suffix <- if (is_smoke_run) sprintf("_limit_%d", limit_arg) else ""
summary_path <- file.path(
  qa_dir,
  paste0("plot_disturbance_severity_summary", qa_suffix, ".csv")
)
dominant_path <- file.path(
  qa_dir,
  paste0("plot_disturbance_severity_by_class", qa_suffix, ".csv")
)

dir_create(if (is_smoke_run) smoke_data_dir else thermo_dir)
dir_create(qa_dir)

if (!file.exists(disturbance_path)) {
  stop(glue("Required input file not found: {disturbance_path}"))
}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

write_parquet_safely <- function(df, path, compression = "snappy") {
  # Write through a temporary file so an interrupted run cannot corrupt the last
  # complete product.
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

first_nonmissing <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)
  x[[1]]
}

weighted_latest_year <- function(year, weight) {
  year <- as.integer(year)
  year <- year[!is.na(year) & !is.na(weight) & weight > 0]
  if (length(year) == 0) return(NA_integer_)
  max(year)
}

safe_prop <- function(num, den) {
  fifelse(!is.na(den) & den > 0, num / den, NA_real_)
}

dominant_from_props <- function(dt) {
  # Pick the largest natural-disturbance proportion for each plot visit.
  prop_cols <- c(
    prop_fire = "prop_fire",
    prop_insect = "prop_insect",
    prop_disease = "prop_disease",
    prop_weather = "prop_weather",
    prop_other_natural = "prop_other_natural"
  )

  prop_matrix <- as.matrix(dt[, ..prop_cols])
  all_missing <- rowSums(!is.na(prop_matrix)) == 0
  prop_matrix_for_max <- prop_matrix
  prop_matrix_for_max[is.na(prop_matrix_for_max)] <- 0

  max_prop <- apply(prop_matrix_for_max, 1, max)
  max_index <- max.col(prop_matrix_for_max, ties.method = "first")
  dominant <- names(prop_cols)[max_index]
  dominant[max_prop <= 0 | all_missing] <- "none"
  dominant <- sub("^prop_", "", dominant)

  data.table(
    dominant_disturbance_class = dominant,
    dominant_disturbance_prop = fifelse(all_missing, NA_real_, max_prop)
  )
}

# ------------------------------------------------------------------------------
# Load condition-level disturbance classifications
# ------------------------------------------------------------------------------

cat("Plot Disturbance Severity Build\n")
cat("===============================\n\n")
cat(glue("Input:  {disturbance_path}"), "\n")
cat(glue("Output: {out_path}"), "\n\n")

disturbance <- as.data.table(read_parquet(disturbance_path))

if (is_smoke_run) {
  disturbance <- disturbance[seq_len(min(.N, limit_arg))]
}

required_cols <- c(
  "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
  "region_east_west", "CONDID", "CONDPROP_UNADJ", "pct_forested",
  "is_forested_condition", "is_forest_dominated_plot",
  "has_fire_condition", "has_crown_fire_condition", "has_insect_condition",
  "has_disease_condition", "has_wind_condition", "has_drought_condition",
  "has_other_weather_condition", "has_animal_condition",
  "has_vegetation_condition", "has_geologic_condition",
  "has_any_recorded_disturbance", "has_any_treatment",
  "has_cutting_treatment", "is_human_or_harvest",
  "disturbance_year_latest", "disturbance_year_earliest",
  "time_since_disturbance", "has_continuous_disturbance_year"
)

missing_cols <- setdiff(required_cols, names(disturbance))
if (length(missing_cols) > 0) {
  stop(glue(
    "Disturbance classification is missing required column(s): ",
    "{paste(missing_cols, collapse = ', ')}"
  ))
}

# ------------------------------------------------------------------------------
# Prepare condition weights and disturbance groups
# ------------------------------------------------------------------------------

disturbance[, condition_weight := fifelse(
  !is.na(CONDPROP_UNADJ) & CONDPROP_UNADJ > 0,
  CONDPROP_UNADJ,
  NA_real_
)]

disturbance[, forested_condition_weight := fifelse(
  is_forested_condition == TRUE,
  condition_weight,
  0
)]

disturbance[, has_weather_condition :=
  has_wind_condition | has_drought_condition | has_other_weather_condition]
disturbance[, has_other_natural_condition :=
  has_animal_condition | has_vegetation_condition | has_geologic_condition]
disturbance[, has_any_natural_disturbance :=
  has_fire_condition | has_insect_condition | has_disease_condition |
    has_weather_condition | has_other_natural_condition]

# ------------------------------------------------------------------------------
# Aggregate condition proportions to plot visits
# ------------------------------------------------------------------------------

plot_keys <- c("stable_plot_id", "PLT_CN", "INVYR")

severity <- disturbance[, .(
  STATECD = first_nonmissing(STATECD),
  UNITCD = first_nonmissing(UNITCD),
  COUNTYCD = first_nonmissing(COUNTYCD),
  PLOT = first_nonmissing(PLOT),
  region_east_west = first_nonmissing(region_east_west),
  pct_forested = first_nonmissing(pct_forested),
  is_forest_dominated_plot = first_nonmissing(is_forest_dominated_plot),

  n_conditions = .N,
  total_condition_prop = sum(condition_weight, na.rm = TRUE),
  forested_condition_prop = sum(forested_condition_weight, na.rm = TRUE),

  prop_any_recorded_disturbance = sum(condition_weight[has_any_recorded_disturbance == TRUE], na.rm = TRUE),
  prop_any_natural_disturbance = sum(condition_weight[has_any_natural_disturbance == TRUE], na.rm = TRUE),
  prop_fire = sum(condition_weight[has_fire_condition == TRUE], na.rm = TRUE),
  prop_crown_fire = sum(condition_weight[has_crown_fire_condition == TRUE], na.rm = TRUE),
  prop_insect = sum(condition_weight[has_insect_condition == TRUE], na.rm = TRUE),
  prop_disease = sum(condition_weight[has_disease_condition == TRUE], na.rm = TRUE),
  prop_weather = sum(condition_weight[has_weather_condition == TRUE], na.rm = TRUE),
  prop_wind = sum(condition_weight[has_wind_condition == TRUE], na.rm = TRUE),
  prop_drought = sum(condition_weight[has_drought_condition == TRUE], na.rm = TRUE),
  prop_other_weather = sum(condition_weight[has_other_weather_condition == TRUE], na.rm = TRUE),
  prop_other_natural = sum(condition_weight[has_other_natural_condition == TRUE], na.rm = TRUE),
  prop_human_or_harvest = sum(condition_weight[is_human_or_harvest == TRUE], na.rm = TRUE),
  prop_any_treatment = sum(condition_weight[has_any_treatment == TRUE], na.rm = TRUE),
  prop_cutting_treatment = sum(condition_weight[has_cutting_treatment == TRUE], na.rm = TRUE),

  forested_prop_fire = sum(forested_condition_weight[has_fire_condition == TRUE], na.rm = TRUE),
  forested_prop_crown_fire = sum(forested_condition_weight[has_crown_fire_condition == TRUE], na.rm = TRUE),
  forested_prop_insect = sum(forested_condition_weight[has_insect_condition == TRUE], na.rm = TRUE),
  forested_prop_disease = sum(forested_condition_weight[has_disease_condition == TRUE], na.rm = TRUE),
  forested_prop_weather = sum(forested_condition_weight[has_weather_condition == TRUE], na.rm = TRUE),

  disturbance_year_latest = weighted_latest_year(disturbance_year_latest, condition_weight),
  disturbance_year_earliest = {
    years <- as.integer(disturbance_year_earliest)
    years <- years[!is.na(years) & !is.na(condition_weight) & condition_weight > 0]
    if (length(years) == 0) NA_integer_ else min(years)
  },
  has_continuous_disturbance_year = any(has_continuous_disturbance_year == TRUE, na.rm = TRUE)
), by = plot_keys]

# Convert raw condition-proportion sums to proportions of the mapped plot visit.
prop_cols <- grep("^prop_", names(severity), value = TRUE)
for (col in prop_cols) {
  severity[, (col) := safe_prop(get(col), total_condition_prop)]
}

# Convert forested disturbance sums to proportions of forested condition area.
forested_prop_cols <- grep("^forested_prop_", names(severity), value = TRUE)
for (col in forested_prop_cols) {
  severity[, (col) := safe_prop(get(col), forested_condition_prop)]
}

dominant <- dominant_from_props(severity)
severity[, `:=`(
  dominant_disturbance_class = dominant$dominant_disturbance_class,
  dominant_disturbance_prop = dominant$dominant_disturbance_prop,
  any_fire = prop_fire > 0,
  any_crown_fire = prop_crown_fire > 0,
  any_insect = prop_insect > 0,
  any_disease = prop_disease > 0,
  any_weather = prop_weather > 0,
  any_natural_disturbance = prop_any_natural_disturbance > 0,
  any_human_or_harvest = prop_human_or_harvest > 0,
  has_mixed_natural_disturbance = rowSums(.SD > 0, na.rm = TRUE) > 1
), .SDcols = c("prop_fire", "prop_insect", "prop_disease", "prop_weather", "prop_other_natural")]

# Fire classes are FIA-only proxies: crown fire is the strongest severity signal.
severity[, fire_severity_class := fcase(
  prop_crown_fire > 0, "crown_fire",
  prop_fire > 0, "non_crown_or_unspecified_fire",
  default = "none"
)]

severity[, plot_disturbance_extent_class := fcase(
  prop_any_natural_disturbance >= 0.95, "nearly_complete",
  prop_any_natural_disturbance >= 0.50, "majority",
  prop_any_natural_disturbance > 0, "partial",
  default = "none"
)]

severity[, condition_prop_quality_flag := fcase(
  is.na(total_condition_prop) | total_condition_prop <= 0, "missing_or_zero_total",
  abs(total_condition_prop - 1) > 0.05, "total_not_near_one",
  default = "ok"
)]

setorder(severity, stable_plot_id, INVYR, PLT_CN)

# ------------------------------------------------------------------------------
# QA summaries
# ------------------------------------------------------------------------------

summary <- data.table(
  metric = c(
    "n_plot_visits",
    "n_stable_plots",
    "n_plot_visits_with_any_natural_disturbance",
    "n_plot_visits_with_fire",
    "n_plot_visits_with_crown_fire",
    "n_plot_visits_with_insect",
    "n_plot_visits_with_disease",
    "n_plot_visits_with_weather",
    "n_plot_visits_with_mixed_natural_disturbance",
    "n_condition_prop_quality_warnings",
    "median_prop_any_natural_disturbance_among_disturbed"
  ),
  value = c(
    nrow(severity),
    uniqueN(severity$stable_plot_id),
    sum(severity$any_natural_disturbance, na.rm = TRUE),
    sum(severity$any_fire, na.rm = TRUE),
    sum(severity$any_crown_fire, na.rm = TRUE),
    sum(severity$any_insect, na.rm = TRUE),
    sum(severity$any_disease, na.rm = TRUE),
    sum(severity$any_weather, na.rm = TRUE),
    sum(severity$has_mixed_natural_disturbance, na.rm = TRUE),
    sum(severity$condition_prop_quality_flag != "ok", na.rm = TRUE),
    median(severity[any_natural_disturbance == TRUE]$prop_any_natural_disturbance, na.rm = TRUE)
  ),
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)

count_section <- function(dt, column, section_name) {
  out <- dt[, .N, by = .(category = get(column))]
  out[, section := section_name]
  setcolorder(out, c("section", "category", "N"))
  out
}

by_class <- rbindlist(list(
  count_section(severity, "dominant_disturbance_class", "dominant_disturbance_class"),
  count_section(severity, "fire_severity_class", "fire_severity_class"),
  count_section(severity, "plot_disturbance_extent_class", "plot_disturbance_extent_class"),
  count_section(severity, "condition_prop_quality_flag", "condition_prop_quality_flag")
), fill = TRUE)
setorder(by_class, section, -N, category)

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

write_parquet_safely(severity, out_path)
write_csv_safely(summary, summary_path)
write_csv_safely(by_class, dominant_path)

metadata_script <- here("scripts/utils/parquet_metadata.R")
if (file.exists(metadata_script) && !is_smoke_run) {
  source(metadata_script)
  write_parquet_metadata(out_path, sample_size = Inf)
}

cat("\nDone.\n")
cat(glue("Disturbance severity parquet: {out_path}"), "\n")
cat(glue("QA summary:                  {summary_path}"), "\n")
cat(glue("QA by class:                 {dominant_path}"), "\n")
cat(glue("Plot visits:                 {format(nrow(severity), big.mark = ',')}"), "\n")
cat(glue(
  "With natural disturbance:    ",
  "{format(sum(severity$any_natural_disturbance, na.rm = TRUE), big.mark = ',')}"
), "\n")
