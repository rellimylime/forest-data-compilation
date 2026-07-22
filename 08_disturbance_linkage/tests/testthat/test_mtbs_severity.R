source(here::here("tests/testthat/helpers.R"))

source(here::here("scripts/utils/mtbs_severity.R"))

test_that("MTBS masks are excluded from severity denominators", {
  out <- summarize_mtbs_classes(
    severity = c(4L, 4L, 3L, 6L, 6L),
    valid_classes = 1:6,
    high_severity_classes = 4L,
    non_processing_classes = 6L
  )

  expect_equal(out$n_pixels_total, 5L)
  expect_equal(out$n_pixels_valid, 3L)
  expect_equal(out$n_pixels_masked, 2L)
  expect_equal(out$frac_pixels_masked, 2 / 5)
  expect_equal(out$frac_high_severity, 2 / 3)
  expect_equal(out$dominant_severity_class, 4L)
  expect_false("mean_severity_class" %in% names(out))
})

test_that("mask-only MTBS coverage has no severity result", {
  out <- summarize_mtbs_classes(
    severity = c(6L, 6L),
    valid_classes = 1:6,
    high_severity_classes = 4L,
    non_processing_classes = 6L
  )

  expect_equal(out$n_pixels_valid, 0L)
  expect_true(is.na(out$frac_high_severity))
  expect_true(is.na(out$dominant_severity_class))
})
