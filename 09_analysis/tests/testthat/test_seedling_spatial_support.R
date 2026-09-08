library(data.table)
library(here)
library(testthat)

measurement_dir <- here(
  "09_analysis", "qa", "outputs", "seedling_spatial_support"
)
sensitivity_dir <- here(
  "09_analysis", "qa", "outputs", "seedling_subplot_mortality_sensitivity"
)
support_robustness_dir <- here(
  "09_analysis", "qa", "outputs", "seedling_support_robustness"
)

test_that("seedling condition and subplot assignments are complete", {
  path <- file.path(
    measurement_dir, "seedling_condition_assignment_audit.csv"
  )
  skip_if_not(file.exists(path), "Run the seedling spatial-support QA first")
  audit <- fread(path)
  value_for <- function(metric_name) audit[metric == metric_name, value]

  expect_equal(value_for("seedling_rows_missing_condition"), 0)
  expect_equal(value_for("seedling_rows_missing_subplot"), 0)
  expect_equal(value_for("duplicate_seedling_condition_subplot_species_keys"), 0)
  expect_equal(value_for("verified_deaths_without_current_subplot"), 0)
})

test_that("subplot sensitivity reproduces unrestricted production mortality", {
  path <- file.path(
    sensitivity_dir, "production_reconstruction_check.csv"
  )
  skip_if_not(file.exists(path), "Run the subplot mortality sensitivity first")
  check <- fread(path)

  expect_equal(nrow(check), 4L)
  expect_true(all(check$maximum_absolute_difference <= 1e-8))
})

test_that("subplot sensitivity produces paired seedling model results", {
  path <- file.path(
    sensitivity_dir, "seedling_subplot_model_coefficients.csv"
  )
  skip_if_not(file.exists(path), "Run the subplot mortality sensitivity first")
  coefficients <- fread(path)

  expect_setequal(
    unique(coefficients$scenario),
    c("condition_wide", "seedling_subplot_union")
  )
  expect_setequal(
    unique(coefficients$response),
    c("temperature", "precipitation", "CWD")
  )
  expect_equal(
    coefficients[grepl("_cumulative_mortality_pct$", term), .N],
    18L
  )
})

test_that("seedling support robustness covers every requested comparison", {
  path <- file.path(
    support_robustness_dir, "seedling_support_interaction_tests.csv"
  )
  skip_if_not(file.exists(path), "Run the seedling support robustness QA first")
  interactions <- fread(path)

  expect_setequal(
    unique(interactions$support_id),
    c(
      "initial_seedling_tally", "initial_species_richness",
      "eligible_microplots", "microplots_with_seedlings"
    )
  )
  expect_setequal(
    unique(interactions$response),
    c("temperature", "precipitation", "CWD")
  )
  expect_setequal(
    unique(interactions$agent),
    c("Fire", "Insect", "Disease")
  )
  expect_equal(nrow(interactions), 36L)
  expect_true(all(interactions$p_value >= 0 & interactions$p_value <= 1))
  expect_true(all(
    interactions$p_value_fdr >= 0 & interactions$p_value_fdr <= 1
  ))
})

test_that("seedling support models keep one fixed cohort", {
  path <- file.path(
    support_robustness_dir, "seedling_support_model_fit.csv"
  )
  skip_if_not(file.exists(path), "Run the seedling support robustness QA first")
  fits <- fread(path)

  expect_equal(uniqueN(fits$n), 1L)
  expect_equal(uniqueN(fits$stable_plots), 1L)
  expect_equal(nrow(fits), 12L)
})

test_that("fixed seedling QA cohort reproduces production coefficients", {
  path <- file.path(
    support_robustness_dir, "baseline_reproduction_check.csv"
  )
  skip_if_not(file.exists(path), "Run the seedling support robustness QA first")
  check <- fread(path)

  expect_true(all(check$estimate_absolute_difference <= 1e-12))
  expect_true(all(check$standard_error_absolute_difference <= 1e-12))
})
