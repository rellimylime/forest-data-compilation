library(data.table)
library(here)
library(testthat)

robustness_dir <- here(
  "09_analysis", "results", "model_runs",
  "20260822_cumulative_mortality_site_cwd_all_groups_v01",
  "robustness"
)

test_that("robustness baseline reproduces the approved models", {
  comparison <- fread(file.path(
    robustness_dir, "baseline_reproduction.csv"
  ))
  expect_equal(max(comparison$estimate_absolute_difference), 0, tolerance = 1e-12)
  expect_equal(
    max(comparison$standard_error_absolute_difference),
    0,
    tolerance = 1e-12
  )
})

test_that("robustness scenarios and life-stage tests are complete", {
  fits <- fread(file.path(robustness_dir, "model_fit.csv"))
  interactions <- fread(file.path(
    robustness_dir, "life_stage_interaction_tests.csv"
  ))
  pairwise <- fread(file.path(
    robustness_dir, "life_stage_pairwise_differences.csv"
  ))

  expect_equal(fits[scenario == "baseline", .N], 12L)
  expect_equal(fits[scenario == "common_histories", .N], 9L)
  expect_equal(interactions[, .N], 15L)
  expect_equal(pairwise[, .N], 45L)
  expect_true(all(interactions$p_value >= 0 & interactions$p_value <= 1))
})
