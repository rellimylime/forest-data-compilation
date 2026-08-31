# ==============================================================================
# 01_build_fia_survey_intervals.R
# Audit FIA PLOT visit links and build resolved previous/current intervals.
#
# Inputs:
#   05_fia/data/processed/summaries/plot_visit_context.parquet
#
# Outputs:
#   08_disturbance_linkage/data/processed/fia_visit_pairing_audit.parquet
#   08_disturbance_linkage/data/processed/fia_survey_intervals.parquet
#   08_disturbance_linkage/qa/outputs/survey_interval_pairing_counts.csv
#
# The audit contains every current-window PLOT record. The interval table
# contains only records whose previous endpoint can be resolved to an available
# PLOT record on the same stable plot. Sampling and date usability remain
# explicit flags; they are not silently filtered out.
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
source(here("scripts/utils/fia_intervals.R"))

config <- load_config()
fia_cfg <- config$raw$fia
fia_processed <- config$processed$fia
link_cfg <- config$processed$disturbance_linkage

context_filename <- fia_processed$summaries$files$plot_visit_context
if (is.null(context_filename) || !nzchar(context_filename)) {
  context_filename <- "plot_visit_context.parquet"
}
context_path <- file.path(
  here(fia_processed$summaries$output_dir),
  context_filename
)

out_dir <- here(link_cfg$output_dir)
qa_dir <- if (is.null(link_cfg$qa_dir) || !nzchar(link_cfg$qa_dir)) {
  qa_dir <- here("08_disturbance_linkage/qa/outputs")
} else {
  here(link_cfg$qa_dir)
}

audit_filename <- link_cfg$files$fia_visit_pairing_audit
if (is.null(audit_filename) || !nzchar(audit_filename)) {
  audit_filename <- "fia_visit_pairing_audit.parquet"
}
interval_filename <- link_cfg$files$fia_survey_intervals
if (is.null(interval_filename) || !nzchar(interval_filename)) {
  interval_filename <- "fia_survey_intervals.parquet"
}
counts_filename <- link_cfg$files$survey_interval_pairing_counts
if (is.null(counts_filename) || !nzchar(counts_filename)) {
  counts_filename <- "survey_interval_pairing_counts.csv"
}

audit_path <- file.path(out_dir, audit_filename)
interval_path <- file.path(out_dir, interval_filename)
counts_path <- file.path(qa_dir, counts_filename)
summary_path <- file.path(
  qa_dir,
  "survey_interval_pairing_summary.csv"
)

if (!file.exists(context_path)) {
  stop(
    "FIA plot-visit context not found: ", context_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R"
  )
}

cat("Build FIA Survey Intervals\n")
cat("==========================\n\n")
cat(glue("Visit context:   {context_path}"), "\n")
cat(glue("Current window:  {fia_cfg$invyr_min}-{fia_cfg$invyr_max}"), "\n")
cat(glue("Audit output:    {audit_path}"), "\n")
cat(glue("Interval output: {interval_path}"), "\n\n")

visits <- as.data.table(read_parquet(context_path))
products <- build_fia_pairing_products(
  visits,
  current_invyr_min = fia_cfg$invyr_min,
  current_invyr_max = fia_cfg$invyr_max
)

dir_create(out_dir)
dir_create(qa_dir)
write_parquet_atomic(
  products$visit_pairing_audit,
  audit_path,
  compression = "snappy"
)
write_parquet_atomic(
  products$survey_intervals,
  interval_path,
  compression = "snappy"
)

tmp_counts <- tempfile(
  pattern = "survey_interval_pairing_counts_",
  tmpdir = qa_dir,
  fileext = ".csv"
)
fwrite(products$pairing_counts, tmp_counts)
file_copy(tmp_counts, counts_path, overwrite = TRUE)
unlink(tmp_counts, force = TRUE)

pairing_summary <- products$visit_pairing_audit[, .(
  n_current_visits = .N,
  n_visits_with_explicit_PREV_PLT_CN =
    sum(!is.na(current_PREV_PLT_CN)),
  pct_visits_with_explicit_PREV_PLT_CN =
    mean(!is.na(current_PREV_PLT_CN)),
  n_structural_matches =
    sum(pairing_class == "structural_match"),
  pct_all_visits_structural_match =
    mean(pairing_class == "structural_match"),
  n_chronological_fallback_candidates =
    sum(pairing_class == "previous_link_missing"),
  n_nonadjacent_official_links =
    sum(pairing_class == "previous_link_to_nonadjacent_visit"),
  n_previous_link_conflicts =
    sum(pairing_class == "previous_link_conflict"),
  n_date_missing =
    sum(date_status == "missing"),
  n_date_reversed =
    sum(date_status == "reversed"),
  n_date_overlapping_or_same_period =
    sum(date_status == "overlapping_or_same_period")
)]
fwrite(pairing_summary, summary_path)

cat("Pairing classes:\n")
print(
  products$visit_pairing_audit[, .(
    n_current_visits = .N,
    n_pairing_usable = sum(pairing_usable, na.rm = TRUE)
  ), by = pairing_class][order(-n_current_visits)]
)
cat("\n")
cat(glue(
  "Visit audit rows: {format(nrow(products$visit_pairing_audit), big.mark = ',')}"
), "\n")
cat(glue(
  "Resolved intervals: {format(nrow(products$survey_intervals), big.mark = ',')}"
), "\n")
cat(glue(
  "Technically usable intervals: ",
  "{format(products$survey_intervals[pairing_usable == TRUE, .N], big.mark = ',')}"
), "\n")
cat("\nDone.\n")
