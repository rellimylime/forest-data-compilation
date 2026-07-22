# ==============================================================================
# 02_disturbance_survey_coverage.R
# Pre/post-disturbance survey coverage QA (configurable).
#
# Question this answers:
#   "How many FIA stable plots have a survey BEFORE and AFTER a disturbance?"
#   -- i.e. the plots that could support a before/after comparison of that
#   disturbance. You choose which disturbance type(s) to ask about, how many
#   surveys must fall on each side, and whether to require the event of interest
#   to be the plot's first recorded disturbance (the clean baseline subset).
#
# All of that is designated in config.yaml, not hard-coded here:
#   processed.thermophilization.disturbance_survey_coverage
#     disturbance_types:  friendly name -> FIA COND.DSTRBCD code set
#     queries:            named coverage questions (see config for each field)
#
# Input (read directly, so this QA does not depend on the 07 build outputs):
#   05_fia/data/processed/summaries/plot_disturbance_classification.parquet
#   -- condition grain, carries raw DSTRBCD1-3 / DSTRBYR1-3 and INVYR.
#
# Definitions used here (documented in 07_thermophilization/README.md):
#   survey year   one distinct INVYR for a stable plot (a measurement occasion).
#   disturbance   a distinct (disturbance class group, disturbance year) pair for
#     event       a stable plot, from any of the three DSTRBCD/DSTRBYR slots on
#                 any of its conditions. Same-year duplicate codes across
#                 conditions collapse to one event.
#   dated event   an event whose year is a real calendar year (excludes 0 = no
#                 year and 9999 = continuous/unknown). Only dated events can be
#                 placed before/after a survey.
#   before/after  a plot "has surveys before and after" a disturbance of a type
#                 if there is a dated event of that type with >= min_surveys_before
#                 survey years strictly earlier and >= min_surveys_after survey
#                 years on or after it (the visit that records a disturbance is an
#                 "after" survey, since FIA records DSTRBYR <= INVYR).
#
# FIA reality (see config comments and the FIADB Database Description):
#   FIA records a disturbance code only when the event hit >= 25% of the trees in
#   the condition (>= 1 acre). "No code" means "below FIA's threshold," not
#   "undisturbed." The insect code cannot isolate bark beetle; crown_fire
#   (DSTRBCD 32) is the closest FIA proxy for high-severity fire.
#
# Outputs (07_thermophilization/qa/outputs/):
#   disturbance_survey_coverage_summary.csv   headline count per configured query
#   disturbance_survey_coverage_by_plot.parquet  per-plot flags for reuse
#   disturbance_survey_coverage_checks.csv    internal-consistency + FIA sanity
#
# Usage:
#   Rscript 07_thermophilization/qa/02_disturbance_survey_coverage.R
#   Rscript 07_thermophilization/qa/02_disturbance_survey_coverage.R --limit=5000
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("07_thermophilization/scripts/disturbance_coverage_helpers.R"))

# ------------------------------------------------------------------------------
# Command line options
# ------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  eq_hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(eq_hit) > 0) return(sub(paste0("^", flag, "="), "", eq_hit[[1]]))
  flag_pos <- which(args == flag)
  if (length(flag_pos) > 0 && flag_pos[[1]] < length(args)) return(args[[flag_pos[[1]] + 1]])
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

qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}
dir_create(qa_dir)

coverage_config <- thermo_config$disturbance_survey_coverage
if (is.null(coverage_config) || is.null(coverage_config$queries)) {
  stop(paste0(
    "config.yaml processed.thermophilization.disturbance_survey_coverage.queries ",
    "is not defined."
  ))
}
type_registry <- coverage_config$disturbance_types
queries <- coverage_config$queries

disturbance_path <- file.path(fia_summary_dir, "plot_disturbance_classification.parquet")
if (!file.exists(disturbance_path)) {
  stop(glue("Required input file not found: {disturbance_path}"))
}

summary_path <- file.path(qa_dir, "disturbance_survey_coverage_summary.csv")
by_plot_path <- file.path(qa_dir, "disturbance_survey_coverage_by_plot.parquet")
checks_path  <- file.path(qa_dir, "disturbance_survey_coverage_checks.csv")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

write_csv_safely <- function(df, path) {
  dir_create(dirname(path))
  tmp_path <- tempfile(pattern = paste0(path_file(path), "_tmp_"),
                       tmpdir = dirname(path), fileext = ".csv")
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)
  fwrite(df, tmp_path)
  file_copy(tmp_path, path, overwrite = TRUE)
}

write_parquet_safely <- function(df, path, compression = "snappy") {
  dir_create(dirname(path))
  tmp_path <- tempfile(pattern = paste0(path_file(path), "_tmp_"),
                       tmpdir = dirname(path), fileext = ".parquet")
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)
  write_parquet(df, tmp_path, compression = compression)
  file_copy(tmp_path, path, overwrite = TRUE)
}

# Collapse a raw FIA DSTRBCD to a coarse class group, so the same physical event
# recorded with slightly different codes (e.g. ground fire 31 and crown fire 32
# for one fire) counts once. Groups follow the FIA DSTRBCD tens ranges.
dstrb_class_group <- function(code) {
  code <- as.integer(code)
  fcase(
    code %in% 10:19, "insect",
    code %in% 20:29, "disease",
    code %in% 30:39, "fire",
    code %in% 40:49, "animal",
    code %in% 50:59, "weather",
    code %in% 60:69, "vegetation",
    code %in% 70:79, "other",
    code %in% 80:89, "human",
    code %in% 90:99, "geologic",
    default = "other"
  )
}

# Resolve a query's list of type names to a concrete code set.
# Returns is_any = TRUE if any listed type is the catch-all "any" ([]),
# in which case every recorded disturbance code matches.
resolve_codes <- function(type_names, registry) {
  codes <- integer(0)
  is_any <- FALSE
  for (tn in type_names) {
    if (!tn %in% names(registry)) {
      stop(glue("Query references unknown disturbance type '{tn}'. ",
                "Add it under disturbance_survey_coverage.disturbance_types."))
    }
    tc <- suppressWarnings(as.integer(unlist(registry[[tn]])))
    tc <- tc[!is.na(tc)]
    if (length(tc) == 0) is_any <- TRUE
    codes <- c(codes, tc)
  }
  list(codes = sort(unique(codes)), is_any = is_any)
}

query_field <- function(q, field, default) {
  if (is.null(q[[field]])) default else q[[field]]
}

# ------------------------------------------------------------------------------
# Load and reshape
# ------------------------------------------------------------------------------

cat("Disturbance Survey Coverage QA\n")
cat("==============================\n\n")
cat(glue("Input:  {disturbance_path}"), "\n")
cat(glue("Config queries: {length(queries)}"), "\n\n")

needed_cols <- c(
  "stable_plot_id", "INVYR",
  "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
  "DSTRBYR1", "DSTRBYR2", "DSTRBYR3"
)
dt <- as.data.table(read_parquet(disturbance_path, col_select = tidyselect::all_of(needed_cols)))

missing_cols <- setdiff(needed_cols, names(dt))
if (length(missing_cols) > 0) {
  stop(glue("Disturbance classification is missing required column(s): ",
            "{paste(missing_cols, collapse = ', ')}"))
}

dt <- dt[!is.na(stable_plot_id) & !is.na(INVYR)]
dt[, INVYR := as.integer(INVYR)]

if (is_smoke_run) {
  keep_plots <- unique(dt$stable_plot_id)[seq_len(min(limit_arg, uniqueN(dt$stable_plot_id)))]
  dt <- dt[stable_plot_id %in% keep_plots]
}

# Survey years: one row per (stable plot, distinct INVYR).
survey_years <- unique(dt[, .(stable_plot_id, survey_year = INVYR)])
setkey(survey_years, stable_plot_id)

# All disturbance events (long over the three DSTRBCD/DSTRBYR slots).
# Keep every record with a real disturbance code (nonzero, non-missing), even if
# its year is missing or continuous. Such records cannot be ordered, so a plot
# carrying one cannot establish a clean "no prior disturbance" baseline.
slot_events <- rbindlist(lapply(1:3, function(j) {
  cc <- paste0("DSTRBCD", j)
  yc <- paste0("DSTRBYR", j)
  e <- dt[, .(stable_plot_id, code = as.integer(get(cc)), raw_year = as.integer(get(yc)))]
  e[!is.na(code) & code != 0L]
}))

if (nrow(slot_events) == 0) {
  warning("No disturbance events found; all coverage counts will be zero.")
}

slot_events[, class_group := dstrb_class_group(code)]
# year_key groups events for counting: real years group by year; missing years
# (0/NA) collapse to one "unknown-year" bucket per class; 9999 stays distinct
# (a continuous disturbance is one event with unknown timing).
slot_events[, year_key := fifelse(is.na(raw_year) | raw_year == 0L, -1L, raw_year)]
slot_events <- unique(slot_events[, .(stable_plot_id, code, class_group, raw_year, year_key)])

# Dated events: usable for the before/after test (exclude no-year and continuous).
dated_events <- slot_events[!is.na(raw_year) & !(raw_year %in% c(0L, 9999L))]
dated_events[, year := as.integer(raw_year)]

# Per-plot count of distinct disturbance events of any type (diagnostic only).
n_events_any <- unique(
  slot_events[, .(stable_plot_id, class_group, year_key)]
)[, .(n_events_any = .N), by = stable_plot_id]

# Per-plot survey-year summary, the spine of the per-plot output.
plot_spine <- survey_years[, .(
  n_survey_years = uniqueN(survey_year),
  first_survey_year = min(survey_year),
  last_survey_year = max(survey_year)
), by = stable_plot_id]
plot_spine <- merge(plot_spine, n_events_any, by = "stable_plot_id", all.x = TRUE)
plot_spine[is.na(n_events_any), n_events_any := 0L]

n_plots_total <- nrow(plot_spine)

# ------------------------------------------------------------------------------
# Per-plot coverage for a single query
# ------------------------------------------------------------------------------

# Returns a data.table keyed by stable_plot_id with one logical column named by
# the query, TRUE where the plot satisfies the query.
evaluate_query <- function(q) {
  q_name <- q$name
  types <- unlist(q$types)
  resolved <- resolve_codes(types, type_registry)
  min_before <- as.integer(query_field(q, "min_surveys_before", 1L))
  min_after  <- as.integer(query_field(q, "min_surveys_after", 1L))
  first_only <- isTRUE(query_field(q, "first_disturbance_only", FALSE))

  # Dated events of the requested type.
  type_dated <- if (resolved$is_any) {
    dated_events
  } else {
    dated_events[code %in% resolved$codes]
  }
  type_dated <- unique(type_dated[, .(stable_plot_id, year)])

  # For the first-disturbance cohort, the candidate must be the earliest dated
  # disturbance of any type, plots with unknown-timing events are excluded, and
  # post-event surveys are counted only until the next dated disturbance.
  per_year <- evaluate_disturbance_windows(
    type_dated = type_dated,
    dated_events = dated_events,
    slot_events = slot_events,
    survey_years = survey_years,
    min_before = min_before,
    min_after = min_after,
    require_first = first_only
  )
  bracketed_plots <- unique(per_year[bracketed == TRUE]$stable_plot_id)
  match_plots <- bracketed_plots

  out <- data.table(stable_plot_id = plot_spine$stable_plot_id)
  out[, (q_name) := stable_plot_id %in% match_plots]

  # Carry a small result record for the summary and checks.
  attr(out, "record") <- list(
    name = q_name,
    types = paste(types, collapse = "+"),
    codes = if (resolved$is_any) "any" else paste(resolved$codes, collapse = ","),
    is_any = resolved$is_any,
    code_set = resolved$codes,
    min_surveys_before = min_before,
    min_surveys_after = min_after,
    first_disturbance_only = first_only,
    n_plots_with_dated_type_disturbance = uniqueN(type_dated$stable_plot_id),
    n_plots_before_after = length(bracketed_plots),
    n_plots_match = length(match_plots)
  )
  out
}

# ------------------------------------------------------------------------------
# Evaluate all queries
# ------------------------------------------------------------------------------

per_plot <- copy(plot_spine)
records <- vector("list", length(queries))

for (i in seq_along(queries)) {
  res <- evaluate_query(queries[[i]])
  records[[i]] <- attr(res, "record")
  per_plot <- merge(per_plot, res, by = "stable_plot_id", all.x = TRUE)
  q_name <- records[[i]]$name
  per_plot[is.na(get(q_name)), (q_name) := FALSE]
}

summary <- rbindlist(lapply(records, function(r) data.table(
  query = r$name,
  types = r$types,
  codes = r$codes,
  min_surveys_before = r$min_surveys_before,
  min_surveys_after = r$min_surveys_after,
  first_disturbance_only = r$first_disturbance_only,
  n_stable_plots_total = n_plots_total,
  n_plots_with_dated_type_disturbance = r$n_plots_with_dated_type_disturbance,
  n_plots_before_after = r$n_plots_before_after,
  n_plots_match = r$n_plots_match
)))
summary[, smoke_limit := if (is_smoke_run) limit_arg else NA_integer_]

# ------------------------------------------------------------------------------
# Internal-consistency and FIA sanity checks
# ------------------------------------------------------------------------------
# These are deliberately grounded in facts that MUST hold if the computation is
# correct, so a failure points at a real bug or data problem rather than taste.

checks <- list()
add_check <- function(check, description, status, value = NA, expected = NA) {
  checks[[length(checks) + 1]] <<- data.table(
    check = check, description = description,
    status = status, value = as.character(value), expected = as.character(expected)
  )
}

# 1. A plot matched on before/after must own at least min_before + min_after
#    distinct survey years. If not, the before/after counting is broken.
too_few_surveys <- 0L
for (r in records) {
  need <- r$min_surveys_before + r$min_surveys_after
  matched <- per_plot[get(r$name) == TRUE]
  too_few_surveys <- too_few_surveys + nrow(matched[n_survey_years < need])
}
add_check(
  "matched_plots_have_enough_surveys",
  "Every matched plot has >= (min_surveys_before + min_surveys_after) survey years.",
  if (too_few_surveys == 0L) "pass" else "FAIL",
  too_few_surveys, 0L
)

# 2. No continuous (9999) or zero/NA year leaked into the dated-event set used
#    for the before/after test.
bad_dated_years <- dated_events[is.na(year) | year %in% c(0L, 9999L), .N]
add_check(
  "no_continuous_or_zero_year_in_before_after",
  "Dated events used for before/after carry only real calendar years (no 0/9999/NA).",
  if (bad_dated_years == 0L) "pass" else "FAIL",
  bad_dated_years, 0L
)

# 3. Nesting by code set: among plain before/after queries (no first-event rule) with
#    identical survey thresholds, a query whose codes are a subset of another's
#    (or the other is "any") must match no more plots than the superset query.
count_by_name <- setNames(vapply(records, function(r) r$n_plots_match, integer(1)),
                          vapply(records, function(r) r$name, character(1)))
nesting_failures <- 0L
nesting_tested <- 0L
plain <- Filter(function(r) !r$first_disturbance_only, records)
for (a in plain) for (b in plain) {
  if (identical(a$name, b$name)) next
  if (a$min_surveys_before != b$min_surveys_before) next
  if (a$min_surveys_after != b$min_surveys_after) next
  a_subset_b <- b$is_any || (!a$is_any && all(a$code_set %in% b$code_set))
  if (a_subset_b) {
    nesting_tested <- nesting_tested + 1L
    if (a$n_plots_match > b$n_plots_match) nesting_failures <- nesting_failures + 1L
  }
}
add_check(
  "code_subset_nesting_holds",
  "For same-threshold before/after queries, a code-subset query matches <= its superset (e.g. crown_fire <= fire <= any).",
  if (nesting_failures == 0L) "pass" else "FAIL",
  glue("{nesting_failures} failed of {nesting_tested} tested"), "0 failed"
)

# 4. First-disturbance nesting: a first_* query must match no more plots than
#    the same-type, same-threshold plain query.
first_failures <- 0L
first_tested <- 0L
for (s in Filter(function(r) r$first_disturbance_only, records)) {
  base <- Filter(function(r) !r$first_disturbance_only &&
                   identical(r$types, s$types) &&
                   r$min_surveys_before == s$min_surveys_before &&
                   r$min_surveys_after == s$min_surveys_after, records)
  for (b in base) {
    first_tested <- first_tested + 1L
    if (s$n_plots_match > b$n_plots_match) first_failures <- first_failures + 1L
  }
}
add_check(
  "first_disturbance_nesting_holds",
  "Each first-disturbance query matches <= its same-type plain before/after query.",
  if (first_failures == 0L) "pass" else "FAIL",
  glue("{first_failures} failed of {first_tested} tested"), "0 failed"
)

# 5. FIA sanity: a disturbance year should not be later than the INVYR of the
#    visit that recorded it (FIA records disturbances that already happened).
#    Count offending condition-slot records as a data-quality signal.
future_dstrb <- rbindlist(lapply(1:3, function(j) {
  cc <- paste0("DSTRBCD", j); yc <- paste0("DSTRBYR", j)
  e <- dt[, .(code = as.integer(get(cc)), yr = as.integer(get(yc)), INVYR)]
  e[!is.na(code) & code != 0L & !is.na(yr) & !(yr %in% c(0L, 9999L))]
}))
n_future_dstrb <- future_dstrb[yr > INVYR, .N]
add_check(
  "disturbance_year_not_after_recording_visit",
  "DSTRBYR is not later than the INVYR that recorded it (FIA data-quality signal, informational).",
  if (n_future_dstrb == 0L) "pass" else "info",
  n_future_dstrb, 0L
)

# 6. Survey years fall in a plausible modern-FIA range.
sy_min <- min(survey_years$survey_year); sy_max <- max(survey_years$survey_year)
add_check(
  "survey_years_in_plausible_range",
  "Survey years lie within [1930, current year + 1].",
  if (sy_min >= 1930L && sy_max <= as.integer(format(Sys.Date(), "%Y")) + 1L) "pass" else "info",
  glue("min={sy_min}, max={sy_max}"), "[1930, current+1]"
)

checks_dt <- rbindlist(checks)

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

setorder(per_plot, stable_plot_id)
write_parquet_safely(per_plot, by_plot_path)
write_csv_safely(summary, summary_path)
write_csv_safely(checks_dt, checks_path)

cat("\nDone.\n")
cat(glue("Per-query summary: {summary_path}"), "\n")
cat(glue("Per-plot flags:    {by_plot_path}"), "\n")
cat(glue("Consistency checks:{checks_path}"), "\n\n")
cat(glue("Stable plots evaluated: {format(n_plots_total, big.mark = ',')}"), "\n\n")
print(summary[, .(query, n_plots_before_after, n_plots_match)])
cat("\n")
print(checks_dt[, .(check, status, value)])
