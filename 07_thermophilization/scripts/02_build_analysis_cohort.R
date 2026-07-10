# ==============================================================================
# 02_build_analysis_cohort.R
# Build the condition-level cohort used for thermophilization comparisons.
#
# This script joins three existing products:
#
#   1. recruitment climate CWMs from script 01;
#   2. condition-level disturbance/control classifications from FIA; and
#   3. plot-visit exclusion flags for nonforest, human disturbance, and harvest.
#
# Output grain:
#   one row per stable_plot_id x PLT_CN x INVYR x CONDID
#
# The output contains only FIA-forested natural-disturbance candidates and
# untreated, undisturbed control candidates with a usable recruitment CWM.
# Whole-plot exclusions are retained as sensitivity flags but do not remove a
# clean condition solely because another condition on that visit differs.
#
# Usage:
#   Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R
#   Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R --limit=1000
#   Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R --min-niche-coverage=0.95
#
# Documentation:
#   07_thermophilization/README.md#script-02-inputs-and-outputs
#   07_thermophilization/README.md#plot_recruitment_analysis_cohortparquet
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

min_niche_coverage <- as.numeric(get_arg("--min-niche-coverage", "0.95"))
if (
  is.na(min_niche_coverage) ||
    min_niche_coverage < 0 ||
    min_niche_coverage > 1
) {
  stop("--min-niche-coverage must be between 0 and 1.")
}

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

cwm_path <- file.path(
  thermo_dir,
  thermo_config$files$plot_recruitment_cwm
)
disturbance_path <- file.path(
  fia_summary_dir,
  "plot_disturbance_classification.parquet"
)
exclusion_path <- file.path(
  fia_summary_dir,
  "plot_exclusion_flags.parquet"
)

cohort_filename <- thermo_config$files$analysis_cohort
if (is.null(cohort_filename) || !nzchar(cohort_filename)) {
  cohort_filename <- "plot_recruitment_analysis_cohort.parquet"
}
out_path <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("plot_recruitment_analysis_cohort_limit_%d.parquet", limit_arg)
  } else {
    cohort_filename
  }
)

qa_suffix <- if (is_smoke_run) sprintf("_limit_%d", limit_arg) else ""
attrition_path <- file.path(
  qa_dir,
  paste0("analysis_cohort_attrition", qa_suffix, ".csv")
)
summary_path <- file.path(
  qa_dir,
  paste0("analysis_cohort_summary", qa_suffix, ".csv")
)

dir_create(thermo_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)

required_files <- c(cwm_path, disturbance_path, exclusion_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(glue("Required input file(s) not found: {paste(missing_files, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

write_parquet_safely <- function(df, path, compression = "snappy") {
  # Write through a temporary file so an interrupted run cannot corrupt the
  # last complete cohort.
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

assert_unique <- function(dt, keys, table_name) {
  duplicate_count <- dt[, .N, by = keys][N > 1, .N]
  if (duplicate_count > 0) {
    stop(glue(
      "{table_name} has {format(duplicate_count, big.mark = ',')} duplicated ",
      "key combination(s): {paste(keys, collapse = ' x ')}"
    ))
  }
}

add_attrition_step <- function(steps, step, description, before, after) {
  steps[[length(steps) + 1]] <- data.table(
    step = step,
    description = description,
    n_before = nrow(before),
    n_removed = nrow(before) - nrow(after),
    n_remaining = nrow(after),
    pct_remaining_from_start = NA_real_
  )
  steps
}

# ------------------------------------------------------------------------------
# Load and join inputs
# ------------------------------------------------------------------------------

cat("Thermophilization Analysis Cohort Build\n")
cat("=======================================\n\n")
cat(glue("Recruitment CWM:       {cwm_path}"), "\n")
cat(glue("Disturbance classes:   {disturbance_path}"), "\n")
cat(glue("Plot exclusion flags:  {exclusion_path}"), "\n")
cat(glue("Niche coverage flag:   {min_niche_coverage}"), "\n")
cat(glue("Output:                {out_path}"), "\n\n")

condition_keys <- c("PLT_CN", "INVYR", "CONDID")
plot_visit_keys <- c("PLT_CN", "INVYR")

cwm <- as.data.table(read_parquet(cwm_path))
disturbance <- as.data.table(read_parquet(disturbance_path))
exclusions <- as.data.table(read_parquet(exclusion_path))

if (is_smoke_run) {
  cwm <- head(cwm, limit_arg)
}

assert_unique(cwm, condition_keys, "Recruitment CWM table")
assert_unique(disturbance, condition_keys, "Disturbance classification table")
assert_unique(exclusions, plot_visit_keys, "Plot exclusion table")

# The disturbance product is the authority for condition metadata and
# disturbance definitions. Remove overlapping non-key columns from the CWM
# before joining so the result has one clear version of each field.
overlapping_cwm_cols <- setdiff(
  intersect(names(cwm), names(disturbance)),
  condition_keys
)
cwm[, (overlapping_cwm_cols) := NULL]

disturbance[, has_disturbance_classification := TRUE]
joined <- merge(
  cwm,
  disturbance,
  by = condition_keys,
  all.x = TRUE,
  sort = FALSE
)

# Exclusion flags are defined at the whole plot-visit level. They catch issues
# such as incidental harvest mortality that condition fields alone may miss.
exclusion_cols <- c(
  plot_visit_keys,
  "exclude_harvest_agent",
  "exclude_nonforest",
  "exclude_human_dist",
  "exclude_harvest",
  "exclude_any"
)
missing_exclusion_cols <- setdiff(exclusion_cols, names(exclusions))
if (length(missing_exclusion_cols) > 0) {
  stop(glue(
    "Plot exclusion table is missing column(s): ",
    "{paste(missing_exclusion_cols, collapse = ', ')}"
  ))
}

exclusions <- exclusions[, ..exclusion_cols]
exclusions[, has_plot_exclusion_flags := TRUE]
joined <- merge(
  joined,
  exclusions,
  by = plot_visit_keys,
  all.x = TRUE,
  sort = FALSE
)

# ------------------------------------------------------------------------------
# Define eligibility and document attrition
# ------------------------------------------------------------------------------

joined[, has_usable_cwm :=
  !is.na(cwm_temp) &
    !is.na(frac_weight_with_niche) &
    frac_weight_with_niche > 0]
joined[, passes_plot_exclusions :=
  has_plot_exclusion_flags == TRUE &
    !is.na(exclude_any) &
    !exclude_any]
joined[, is_analysis_group :=
  disturbed_vs_control %in% c("control", "disturbed")]
joined[, meets_niche_coverage_threshold :=
  !is.na(frac_weight_with_niche) &
    frac_weight_with_niche >= min_niche_coverage]
joined[, niche_coverage_threshold := min_niche_coverage]
joined[, has_plot_level_exclusion_warning :=
  has_plot_exclusion_flags == TRUE & exclude_any == TRUE]
joined[, has_plot_level_harvest_agent_warning :=
  has_plot_exclusion_flags == TRUE & exclude_harvest_agent == TRUE]

# A single primary reason makes exclusions easy to count and explain. The order
# reflects the sequential attrition table below.
joined[, analysis_exclusion_reason := fcase(
  is.na(has_disturbance_classification), "missing_disturbance_classification",
  !has_usable_cwm, "no_usable_recruitment_cwm",
  !is_forested_analysis_condition, "not_forested_analysis_condition",
  !is_analysis_group, "not_natural_disturbance_or_control",
  default = NA_character_
)]
joined[, analysis_eligible := is.na(analysis_exclusion_reason)]

attrition_steps <- list()
current <- joined
start_n <- nrow(current)

next_rows <- current[!is.na(has_disturbance_classification)]
attrition_steps <- add_attrition_step(
  attrition_steps, 1L, "Matched condition-level disturbance classification",
  current, next_rows
)
current <- next_rows

next_rows <- current[has_usable_cwm == TRUE]
attrition_steps <- add_attrition_step(
  attrition_steps, 2L, "Retained rows with a usable recruitment CWM",
  current, next_rows
)
current <- next_rows

next_rows <- current[is_forested_analysis_condition == TRUE]
attrition_steps <- add_attrition_step(
  attrition_steps, 3L, "Retained forested analysis conditions",
  current, next_rows
)
current <- next_rows

next_rows <- current[is_analysis_group == TRUE]
attrition_steps <- add_attrition_step(
  attrition_steps, 4L, "Retained natural-disturbance and control candidates",
  current, next_rows
)
cohort <- next_rows

attrition <- rbindlist(attrition_steps)
attrition[, pct_remaining_from_start := if (start_n > 0) {
  round(100 * n_remaining / start_n, 2)
} else {
  NA_real_
}]

if (nrow(cohort) == 0) {
  stop("No rows remain after applying the analysis cohort filters.")
}

assert_unique(cohort, condition_keys, "Final analysis cohort")
setorder(cohort, stable_plot_id, INVYR, PLT_CN, CONDID)

# Minimal QA summary: population size, analysis groups, coverage, and fallback
# use. Detailed row-level information remains in the cohort itself.
count_categories <- function(dt, column, section_name) {
  out <- dt[, .(value = .N), by = .(category = get(column))]
  out[, section := section_name]
  setcolorder(out, c("section", "category", "value"))
  out
}

summary <- rbindlist(list(
  data.table(
    section = "overall",
    category = c(
      "condition_rows",
      "stable_plots",
      "rows_meeting_niche_coverage_threshold",
      "rows_below_niche_coverage_threshold",
      "rows_using_global_fallback",
      "rows_with_plot_level_exclusion_warning",
      "rows_with_plot_level_harvest_agent_warning",
      "rows_on_less_than_50pct_forested_plots"
    ),
    value = c(
      nrow(cohort),
      uniqueN(cohort$stable_plot_id),
      sum(cohort$meets_niche_coverage_threshold, na.rm = TRUE),
      sum(!cohort$meets_niche_coverage_threshold, na.rm = TRUE),
      sum(cohort$cwm_weight_with_global_fallback_niche > 0, na.rm = TRUE),
      sum(cohort$has_plot_level_exclusion_warning, na.rm = TRUE),
      sum(cohort$has_plot_level_harvest_agent_warning, na.rm = TRUE),
      sum(!cohort$is_forest_dominated_plot, na.rm = TRUE)
    )
  ),
  count_categories(cohort, "disturbed_vs_control", "analysis_group"),
  count_categories(cohort, "disturbance_class", "disturbance_class"),
  count_categories(cohort, "region_east_west", "region")
), fill = TRUE)
summary[, `:=`(
  min_niche_coverage = min_niche_coverage,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]
setorder(summary, section, -value, category)

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

write_parquet_safely(cohort, out_path)
write_csv_safely(attrition, attrition_path)
write_csv_safely(summary, summary_path)

metadata_script <- here("scripts/utils/parquet_metadata.R")
if (file.exists(metadata_script) && !is_smoke_run) {
  source(metadata_script)
  write_parquet_metadata(out_path, sample_size = Inf)
}

cat("Done.\n")
cat(glue("Analysis cohort: {out_path}"), "\n")
cat(glue("Attrition QA:    {attrition_path}"), "\n")
cat(glue("Summary QA:      {summary_path}"), "\n")
cat(glue("Rows retained:   {format(nrow(cohort), big.mark = ',')}"), "\n")
cat(glue(
  "Below {min_niche_coverage * 100}% niche coverage: ",
  "{format(sum(!cohort$meets_niche_coverage_threshold), big.mark = ',')}"
), "\n")
