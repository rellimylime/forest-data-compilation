# ==============================================================================
# Validate the FIA visit-pairing audit and resolved survey intervals.
#
# Writes:
#   08_disturbance_linkage/qa/outputs/
#     fia_survey_interval_validation_checks.csv
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
fia_cfg <- config$raw$fia
link_cfg <- config$processed$disturbance_linkage

context_path <- here(link_cfg$inputs$fia_plot_visit_context)
audit_path <- file.path(
  here(link_cfg$output_dir),
  link_cfg$files$fia_visit_pairing_audit
)
interval_path <- file.path(
  here(link_cfg$output_dir),
  link_cfg$files$fia_survey_intervals
)
counts_path <- file.path(
  here(link_cfg$qa_dir),
  link_cfg$files$survey_interval_pairing_counts
)
checks_path <- file.path(
  here(link_cfg$qa_dir),
  "fia_survey_interval_validation_checks.csv"
)

required <- c(context_path, audit_path, interval_path, counts_path)
missing <- required[!file.exists(required)]
if (length(missing) > 0L) {
  stop("Missing interval-foundation output(s): ", paste(missing, collapse = ", "))
}

context <- as.data.table(read_parquet(context_path))
audit <- as.data.table(read_parquet(audit_path))
intervals <- as.data.table(read_parquet(interval_path))
pairing_counts <- fread(counts_path)

checks <- list()
add_check <- function(name, passed, observed, expected, detail = NA_character_) {
  checks[[length(checks) + 1L]] <<- data.table(
    check = name,
    status = if (isTRUE(passed)) "pass" else "fail",
    observed = as.character(observed),
    expected = as.character(expected),
    detail = detail
  )
}

context_dups <- context[, .N, by = PLT_CN][N > 1L, .N]
audit_dups <- audit[, .N, by = current_PLT_CN][N > 1L, .N]
interval_dups <- intervals[, .N, by = interval_id][N > 1L, .N]
expected_audit_rows <- context[
  !is.na(INVYR) &
    INVYR >= fia_cfg$invyr_min &
    INVYR <= fia_cfg$invyr_max,
  .N
]

add_check("plot_visit_context_unique_PLT_CN", context_dups == 0L, context_dups, 0)
add_check("visit_pairing_audit_unique_current_PLT_CN", audit_dups == 0L, audit_dups, 0)
add_check("survey_intervals_unique_interval_id", interval_dups == 0L, interval_dups, 0)
add_check(
  "audit_covers_configured_current_window",
  nrow(audit) == expected_audit_rows,
  nrow(audit),
  expected_audit_rows
)
add_check(
  "pairing_counts_sum_to_audit",
  sum(pairing_counts$n_current_visits) == nrow(audit),
  sum(pairing_counts$n_current_visits),
  nrow(audit)
)

context_ids <- context$PLT_CN
missing_previous <- sum(!intervals$previous_PLT_CN %in% context_ids)
missing_current <- sum(!intervals$current_PLT_CN %in% context_ids)
add_check(
  "all_interval_previous_endpoints_exist",
  missing_previous == 0L,
  missing_previous,
  0
)
add_check(
  "all_interval_current_endpoints_exist",
  missing_current == 0L,
  missing_current,
  0
)

structural_mismatch <- intervals[
  pairing_class == "structural_match" &
    previous_PLT_CN != current_PREV_PLT_CN,
  .N
]
fallback_with_official_link <- intervals[
  selected_pair_source == "chronological_sampled_fallback" &
    !is.na(current_PREV_PLT_CN),
  .N
]
add_check(
  "structural_matches_follow_PREV_PLT_CN",
  structural_mismatch == 0L,
  structural_mismatch,
  0
)
add_check(
  "fallback_pairs_have_null_PREV_PLT_CN",
  fallback_with_official_link == 0L,
  fallback_with_official_link,
  0
)

allowed_pairing_classes <- c(
  "structural_match",
  "previous_link_missing",
  "previous_link_to_nonadjacent_visit",
  "previous_link_target_unavailable",
  "previous_link_conflict",
  "first_observed_visit"
)
unknown_classes <- setdiff(unique(audit$pairing_class), allowed_pairing_classes)
add_check(
  "pairing_classes_are_documented",
  length(unknown_classes) == 0L,
  paste(unknown_classes, collapse = ";"),
  "none"
)

usable_contract_failures <- intervals[
  pairing_usable == TRUE &
    (
      previous_is_sampled_plot != TRUE |
        current_is_sampled_plot != TRUE |
        date_status != "ordered" |
        is.na(interval_years_min) |
        is.na(interval_years_max) |
        interval_years_min <= 0 |
        interval_years_max < interval_years_min
    ),
  .N
]
add_check(
  "usable_intervals_satisfy_technical_contract",
  usable_contract_failures == 0L,
  usable_contract_failures,
  0,
  paste(
    "Both endpoints sampled; ordered dates; positive duration;",
    "maximum duration not less than minimum."
  )
)

id_mismatch <- intervals[
  interval_id != paste(
    "fia_interval_v1",
    stable_plot_id,
    as.character(previous_PLT_CN),
    as.character(current_PLT_CN),
    sep = "|"
  ),
  .N
]
add_check(
  "interval_id_is_deterministic_composite",
  id_mismatch == 0L,
  id_mismatch,
  0
)

checks_dt <- rbindlist(checks)
dir_create(dirname(checks_path))
tmp_path <- tempfile(
  pattern = "fia_survey_interval_validation_",
  tmpdir = dirname(checks_path),
  fileext = ".csv"
)
fwrite(checks_dt, tmp_path)
file_copy(tmp_path, checks_path, overwrite = TRUE)
unlink(tmp_path, force = TRUE)

print(checks_dt)
if (checks_dt[status == "fail", .N] > 0L) {
  stop("FIA survey interval validation failed; see ", checks_path)
}

cat(glue("\nAll {nrow(checks_dt)} interval-foundation checks passed."), "\n")
cat(glue("Checks: {checks_path}"), "\n")
