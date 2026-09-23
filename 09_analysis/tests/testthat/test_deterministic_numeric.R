library(testthat)

source(here::here(
  "09_analysis", "scripts", "utils", "deterministic_numeric.R"
))

test_that("deterministic sums do not depend on input order", {
  values <- c(1e16, 1, -1e16, 0.1, 0.2, 0.3)
  expected <- deterministic_sum(values)

  expect_identical(deterministic_sum(rev(values)), expected)
  expect_identical(deterministic_sum(values[c(2, 5, 1, 6, 3, 4)]), expected)
})

test_that("deterministic sums preserve sum missing-value semantics", {
  expect_true(is.na(deterministic_sum(c(1, NA_real_))))
  expect_identical(deterministic_sum(c(1, NA_real_), na.rm = TRUE), 1)
  expect_identical(deterministic_sum(numeric()), 0)
})

test_that("weighted means do not depend on row order", {
  value <- c(-3.5, 0.25, 11, 100.125)
  weight <- c(0.1, 4, 2.5, 0.005)
  expected <- deterministic_weighted_mean(value, weight)
  order <- c(4, 2, 1, 3)

  expect_identical(
    deterministic_weighted_mean(value[order], weight[order]),
    expected
  )
})
