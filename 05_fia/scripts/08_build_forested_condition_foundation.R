#!/usr/bin/env Rscript

# Build the condition-level foundation for forest-community analyses.
# The output retains every condition, marks forest membership, and normalizes
# condition area weights across only the forested part of each FIA visit.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/forest_analysis.R"))

config <- load_config()
fia_cfg <- config$processed$fia
summary_dir <- here(fia_cfg$summaries$output_dir)
input_path <- file.path(summary_dir, "plot_condition_metadata.parquet")
output_name <- fia_cfg$summaries$files$forested_condition_foundation
if (is.null(output_name) || !nzchar(output_name)) {
  output_name <- "forested_condition_foundation.parquet"
}
output_path <- file.path(summary_dir, output_name)
qa_dir <- here("05_fia/qa/outputs")

if (!file.exists(input_path)) {
  stop(
    "Condition metadata is missing: ", input_path, "\n",
    "Run: Rscript 05_fia/scripts/05_build_fia_summaries.R"
  )
}

cat("Build Forested-Condition Foundation\n")
cat("===================================\n\n")
cat(glue("Input:  {input_path}\n"))
cat(glue("Output: {output_path}\n\n"))

conditions <- as.data.table(read_parquet(input_path))

# Add forest flags, forest-normalized weights, and visit-level proportion checks.
foundation <- build_forested_condition_foundation(conditions)

# Keep one QA row per plot visit even though the foundation is condition-level.
visit_qa <- unique(foundation[, .(
  stable_plot_id, PLT_CN, INVYR, state, STATECD,
  n_conditions, n_forested_conditions, n_nonsampled_conditions,
  total_condition_proportion, forested_plot_proportion,
  n_missing_condition_proportions,
  no_forested_conditions, very_small_forested_proportion,
  nonsampled_zero_proportion,
  condition_proportions_not_one,
  condition_proportion_quality_flag
)])
setorder(visit_qa, state, stable_plot_id, INVYR, PLT_CN)

# Summarize the condition-proportion issues that need human review.
summary_qa <- visit_qa[, .(
  n_plot_visits = .N,
  n_no_forested_conditions = sum(no_forested_conditions),
  n_partially_forested_visits = sum(
    !is.na(forested_plot_proportion) &
      forested_plot_proportion > 0 &
      forested_plot_proportion < 1
  ),
  n_very_small_forested_proportion = sum(very_small_forested_proportion),
  n_missing_condition_proportions =
    sum(n_missing_condition_proportions > 0L),
  n_nonsampled_zero_proportion =
    sum(nonsampled_zero_proportion),
  n_condition_proportions_not_one =
    sum(condition_proportions_not_one),
  median_forested_plot_proportion =
    median(forested_plot_proportion, na.rm = TRUE),
  p05_forested_plot_proportion =
    as.numeric(quantile(forested_plot_proportion, 0.05, na.rm = TRUE)),
  p95_forested_plot_proportion =
    as.numeric(quantile(forested_plot_proportion, 0.95, na.rm = TRUE))
)]

# Record how many condition rows a later forest-only filter would retain.
filter_qa <- data.table(
  metric = c(
    "condition_rows_total",
    "condition_rows_forested",
    "condition_rows_removed_as_nonforest",
    "plot_visits_total",
    "plot_visits_with_forested_condition"
  ),
  value = c(
    nrow(foundation),
    foundation[is_forested_condition == TRUE, .N],
    foundation[is_forested_condition != TRUE, .N],
    nrow(visit_qa),
    visit_qa[no_forested_conditions == FALSE, .N]
  )
)

dir_create(summary_dir)
dir_create(qa_dir)

# Replace the canonical foundation atomically after the full build succeeds.
write_parquet_atomic(foundation, output_path, compression = "snappy")
fwrite(
  visit_qa,
  file.path(qa_dir, "forested_condition_visit_qa.csv")
)
fwrite(
  summary_qa,
  file.path(qa_dir, "forested_condition_summary.csv")
)
fwrite(
  filter_qa,
  file.path(qa_dir, "forested_condition_filter_counts.csv")
)

cat("QA summary:\n")
print(summary_qa)
cat("\n")
cat(glue("Condition rows: {format(nrow(foundation), big.mark = ',')}\n"))
cat(glue("Plot visits:    {format(nrow(visit_qa), big.mark = ',')}\n"))
cat("Done.\n")
