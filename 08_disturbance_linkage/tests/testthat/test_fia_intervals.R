source(here::here("tests/testthat/helpers.R"))
source(here::here("scripts/utils/fia_intervals.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(testthat)
})

make_visit <- function(
  stable_plot_id,
  PLT_CN,
  PREV_PLT_CN = NA_integer_,
  INVYR,
  month = 7L,
  day = 15L,
  sampled = TRUE,
  state = "CA"
) {
  data.table(
    stable_plot_id = stable_plot_id,
    PLT_CN = PLT_CN,
    PREV_PLT_CN = PREV_PLT_CN,
    INVYR = INVYR,
    MEASYEAR = INVYR,
    MEASMON = month,
    MEASDAY = day,
    PLOT_STATUS_CD = if (sampled) 1L else 2L,
    is_sampled_plot = sampled,
    LAT = 35 + PLT_CN / 10000,
    LON = -120,
    STATECD = 6L,
    state = state
  )
}

test_that("FIA measurement bounds retain day precision and use bounded fallbacks", {
  x <- data.table(
    INVYR = c(2020L, 2020L, 2020L, 2020L, NA_integer_),
    MEASYEAR = c(2020L, 2020L, 2020L, NA_integer_, NA_integer_),
    MEASMON = c(2L, 2L, NA_integer_, NA_integer_, NA_integer_),
    MEASDAY = c(29L, 0L, NA_integer_, NA_integer_, NA_integer_)
  )

  out <- add_fia_measurement_date_bounds(x)

  expect_equal(
    out$measurement_date_precision,
    c("day", "month", "year", "inventory_year_fallback", "missing")
  )
  expect_equal(as.character(out$measurement_date_lower[1]), "2020-02-29")
  expect_equal(as.character(out$measurement_date_lower[2]), "2020-02-01")
  expect_equal(as.character(out$measurement_date_upper[2]), "2020-02-29")
  expect_equal(as.character(out$measurement_date_lower[3]), "2020-01-01")
  expect_equal(as.character(out$measurement_date_upper[3]), "2020-12-31")
  expect_equal(out$measurement_date_issue[2], "invalid_measurement_day")
  expect_equal(out$measurement_date_issue[4], "missing_measurement_year")
})

test_that("pairing audit separates link structure, fallback, and interval usability", {
  visits <- rbindlist(list(
    make_visit("A", 101L, INVYR = 2000L),
    make_visit("A", 102L, PREV_PLT_CN = 101L, INVYR = 2005L),
    make_visit("A", 103L, INVYR = 2007L, sampled = FALSE),
    make_visit("A", 104L, PREV_PLT_CN = 101L, INVYR = 2010L),
    make_visit("A", 105L, INVYR = 2015L),
    make_visit("B", 201L, INVYR = 2003L),
    make_visit("C", 301L, PREV_PLT_CN = 999L, INVYR = 2004L),
    make_visit("D", 401L, PREV_PLT_CN = 101L, INVYR = 2006L)
  ))
  visits <- add_fia_measurement_date_bounds(visits)

  products <- build_fia_pairing_products(
    visits,
    current_invyr_min = 2000L,
    current_invyr_max = 2020L
  )
  audit <- products$visit_pairing_audit
  intervals <- products$survey_intervals

  expect_equal(audit[current_PLT_CN == 102L, pairing_class], "structural_match")
  expect_equal(
    audit[current_PLT_CN == 104L, pairing_class],
    "previous_link_to_nonadjacent_visit"
  )
  expect_equal(audit[current_PLT_CN == 105L, pairing_class], "previous_link_missing")
  expect_equal(
    audit[current_PLT_CN == 301L, pairing_class],
    "previous_link_target_unavailable"
  )
  expect_equal(audit[current_PLT_CN == 401L, pairing_class], "previous_link_conflict")
  expect_equal(audit[current_PLT_CN == 201L, pairing_class], "first_observed_visit")

  expect_equal(audit[current_PLT_CN == 105L, previous_PLT_CN], 104L)
  expect_equal(
    audit[current_PLT_CN == 105L, selected_pair_source],
    "chronological_sampled_fallback"
  )
  expect_true(all(intervals$pairing_usable))
  expect_equal(nrow(intervals), 3L)
  expect_equal(uniqueN(intervals$interval_id), nrow(intervals))
  expect_true(all(intervals$interval_years_min > 0))
})

test_that("resolved pairs with overlapping date bounds remain auditable but unusable", {
  visits <- rbindlist(list(
    make_visit("A", 101L, INVYR = 2000L, month = NA_integer_, day = NA_integer_),
    make_visit(
      "A", 102L, PREV_PLT_CN = 101L, INVYR = 2000L,
      month = NA_integer_, day = NA_integer_
    )
  ))
  visits <- add_fia_measurement_date_bounds(visits)
  products <- build_fia_pairing_products(visits, 2000L, 2000L)

  expect_equal(nrow(products$survey_intervals), 1L)
  expect_equal(
    products$survey_intervals$date_status,
    "overlapping_or_same_period"
  )
  expect_false(products$survey_intervals$pairing_usable)
  expect_equal(
    products$survey_intervals$pairing_usable_reason,
    "overlapping_or_same_measurement_period"
  )
})

test_that("built FIA interval products satisfy their file contracts", {
  config <- qa_load_config()
  link_cfg <- config$processed$disturbance_linkage
  context_path <- qa_path(link_cfg$inputs$fia_plot_visit_context)
  audit_path <- qa_path(
    file.path(link_cfg$output_dir, link_cfg$files$fia_visit_pairing_audit)
  )
  intervals_path <- qa_path(
    file.path(link_cfg$output_dir, link_cfg$files$fia_survey_intervals)
  )

  context_path <- qa_require_file(context_path)
  audit_path <- qa_require_file(audit_path)
  intervals_path <- qa_require_file(intervals_path)

  context <- qa_read_parquet_head(context_path, n = 100000)
  audit <- qa_read_parquet_head(audit_path, n = 100000)
  intervals <- qa_read_parquet_head(intervals_path, n = 100000)

  qa_expect_cols(context, c(
    "stable_plot_id", "PLT_CN", "PREV_PLT_CN",
    "MEASYEAR", "MEASMON", "MEASDAY",
    "measurement_date_lower", "measurement_date_upper",
    "PLOT_STATUS_CD", "is_sampled_plot"
  ))
  qa_expect_cols(audit, c(
    "current_PLT_CN", "pairing_class", "date_status",
    "pairing_usable", "pairing_usable_reason"
  ))
  qa_expect_cols(intervals, c(
    "interval_id", "previous_PLT_CN", "current_PLT_CN",
    "previous_date_lower", "current_date_upper",
    "interval_years_min", "interval_years_max"
  ))
  qa_expect_unique_key(context, "PLT_CN", "plot_visit_context PLT_CN")
  qa_expect_unique_key(audit, "current_PLT_CN", "visit audit current_PLT_CN")
  qa_expect_unique_key(intervals, "interval_id", "survey interval_id")
})
