source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages(library(data.table))

source(here::here(
  "07_thermophilization/scripts/disturbance_coverage_helpers.R"
))

test_that("first-disturbance windows allow later events but stop before them", {
  surveys <- data.table(
    stable_plot_id = "A",
    survey_year = c(2000L, 2010L, 2020L, 2030L)
  )
  dated <- data.table(
    stable_plot_id = "A",
    code = c(30L, 10L),
    raw_year = c(2010L, 2025L),
    year = c(2010L, 2025L)
  )
  slots <- copy(dated)

  out <- evaluate_disturbance_windows(
    type_dated = dated[code == 30L],
    dated_events = dated,
    slot_events = slots,
    survey_years = surveys,
    min_before = 1L,
    min_after = 2L,
    require_first = TRUE
  )

  expect_equal(nrow(out), 1L)
  expect_equal(out$first_disturbance_year, 2010L)
  expect_equal(out$next_disturbance_year, 2025L)
  expect_equal(out$n_before, 1L)
  expect_equal(out$n_after_before_next, 2L)
  expect_true(out$bracketed)
})

test_that("a later event cannot qualify as the first disturbance", {
  surveys <- data.table(
    stable_plot_id = "A",
    survey_year = c(2000L, 2010L, 2020L, 2030L)
  )
  dated <- data.table(
    stable_plot_id = "A",
    code = c(30L, 10L),
    raw_year = c(2010L, 2025L),
    year = c(2010L, 2025L)
  )

  out <- evaluate_disturbance_windows(
    type_dated = dated[code == 10L],
    dated_events = dated,
    slot_events = dated,
    survey_years = surveys,
    require_first = TRUE
  )
  expect_equal(nrow(out), 0L)
})

test_that("unknown-timing events exclude a clean first-disturbance baseline", {
  surveys <- data.table(
    stable_plot_id = "A",
    survey_year = c(2000L, 2010L)
  )
  dated <- data.table(
    stable_plot_id = "A", code = 30L, raw_year = 2010L, year = 2010L
  )
  slots <- rbindlist(list(
    dated,
    data.table(stable_plot_id = "A", code = 10L, raw_year = 9999L, year = NA_integer_)
  ), fill = TRUE)

  out <- evaluate_disturbance_windows(
    type_dated = dated,
    dated_events = dated,
    slot_events = slots,
    survey_years = surveys,
    require_first = TRUE
  )
  expect_equal(nrow(out), 0L)
})
