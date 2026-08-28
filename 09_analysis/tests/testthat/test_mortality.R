library(testthat)
library(data.table)

source(here::here("09_analysis/scripts/utils/mortality.R"))

test_that("AGENTCD families use FIA decade groups", {
  expect_equal(
    fia_agent_family(c(10L, 19L, 20L, 29L, 30L, 39L, 40L, 0L, NA)),
    c("insect", "insect", "disease", "disease", "fire", "fire",
      "other", NA, NA)
  )
})

test_that("AGENTCD completeness separates zero and missing", {
  expect_equal(
    fia_agent_completeness(c(10L, 0L, NA, -1L)),
    c("positive", "zero", "missing", "unexpected")
  )
})

test_that("only mortality GRM components identify interval deaths", {
  expect_equal(
    fia_is_verified_interval_death(
      c("MORTALITY1", "MORTALITY2", "INGROWTH", "CUT1", NA)
    ),
    c(TRUE, TRUE, FALSE, FALSE, FALSE)
  )
})

test_that("condition eligibility requires stable forest endpoints", {
  expect_equal(
    fia_condition_interval_eligible(
      c(TRUE, FALSE, TRUE, TRUE),
      c(1L, 1L, 1L, 2L),
      c(1L, 1L, 1L, 1L),
      c(.30, .80, .29, .80),
      c(.30, .80, .80, .80)
    ),
    c(TRUE, FALSE, FALSE, FALSE)
  )
})

test_that("sampling elements follow size and macroplot signatures", {
  expect_equal(
    fia_sampling_element(
      diameter = c(2, 8, 12, 7),
      tpa_unadj = c(74.965282, 6.018046, .999188, 6.018046),
      subptyp = c(NA, NA, NA, 3L)
    ),
    c("microplot", "subplot", "macroplot", "macroplot")
  )
})

test_that("measurement midpoint uses the available date bounds", {
  expect_equal(
    fia_mid_date(
      as.Date(c("2020-01-01", "2020-01-01", NA)),
      as.Date(c("2020-01-11", NA, "2020-01-11"))
    ),
    as.Date(c("2020-01-06", "2020-01-01", "2020-01-11"))
  )
})
