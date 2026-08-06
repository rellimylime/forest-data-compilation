source(here::here("tests/testthat/helpers.R"))
source(here::here("scripts/utils/forest_analysis.R"))

suppressPackageStartupMessages({
  library(data.table)
})

test_that("fire evidence pairs disturbance codes and years within slots", {
  conditions <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    CONDID = 1L,
    stable_plot_id = "plot-1",
    state = "XX",
    is_forested_condition = TRUE,
    DSTRBCD1 = 31L,
    DSTRBYR1 = 2016L,
    DSTRBCD2 = 50L,
    DSTRBYR2 = 2015L,
    DSTRBCD3 = 32L,
    DSTRBYR3 = 9999L
  )
  context <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    MEASYEAR = 2019L,
    measurement_date_upper = as.IDate("2019-12-31")
  )

  evidence <- build_fia_fire_slot_evidence(conditions, context)

  expect_equal(evidence$disturbance_slot, c(1L, 3L))
  expect_equal(evidence$disturbance_code, c(31L, 32L))
  expect_equal(evidence$disturbance_year, c(2016L, 9999L))
  expect_false(any(evidence$disturbance_year == 2015L))
  expect_equal(
    evidence$fire_year_status,
    c("valid", "continuous_or_unknown")
  )
  expect_true(all(grepl("^cond1/slot(1|3):", evidence$fire_disturbance_slot_evidence)))
})

test_that("fire-year validation uses measurement year before INVYR", {
  conditions <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    CONDID = 1L,
    DSTRBCD1 = 31L,
    DSTRBYR1 = 2020L,
    DSTRBCD2 = NA_integer_,
    DSTRBYR2 = NA_integer_,
    DSTRBCD3 = NA_integer_,
    DSTRBYR3 = NA_integer_
  )
  context <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    MEASYEAR = 2019L,
    measurement_date_upper = as.IDate("2019-12-31")
  )

  evidence <- build_fia_fire_slot_evidence(conditions, context)

  expect_equal(evidence$fire_year_status, "post_measurement_year")
  expect_equal(evidence$fire_year_validation_source, "MEASYEAR")
  expect_false(evidence$valid_fire_year)
})

test_that("measurement year can validate a fire year after INVYR", {
  conditions <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    CONDID = 1L,
    DSTRBCD1 = 31L,
    DSTRBYR1 = 2021L,
    DSTRBCD2 = NA_integer_,
    DSTRBYR2 = NA_integer_,
    DSTRBCD3 = NA_integer_,
    DSTRBYR3 = NA_integer_
  )
  context <- data.table(
    PLT_CN = 1,
    INVYR = 2020L,
    MEASYEAR = 2021L,
    measurement_date_upper = as.IDate("2021-06-30")
  )

  evidence <- build_fia_fire_slot_evidence(conditions, context)

  expect_equal(evidence$fire_year_status, "valid")
  expect_equal(evidence$fire_year_validation_source, "MEASYEAR")
  expect_true(evidence$valid_fire_year)
  expect_gt(evidence$disturbance_year, evidence$INVYR)
  expect_lte(evidence$disturbance_year, evidence$measurement_year_upper)
})
