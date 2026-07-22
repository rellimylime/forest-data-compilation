source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(tibble)
})

# summary_helpers.R (fia_should_rebuild / fia_force_requested) + atomic writer.
source(here::here("05_fia/scripts/summaries/summary_helpers.R"))
source(here::here("scripts/utils/parquet_atomic.R"))

make_parquet <- function(path, df = tibble(a = 1:3)) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(df, path)
  path
}

test_that("missing output triggers a rebuild", {
  out <- file.path(tempfile("fresh"), "out.parquet")
  rb <- fia_should_rebuild(out, input_paths = character(0))
  expect_true(rb$rebuild)
  expect_match(rb$reason, "missing")
})

test_that("an output newer than its inputs is skipped", {
  dir <- tempfile("fresh"); dir.create(dir)
  inp <- make_parquet(file.path(dir, "in.parquet"))
  Sys.sleep(0.05)
  out <- make_parquet(file.path(dir, "out.parquet"))
  rb <- fia_should_rebuild(out, input_paths = inp,
                           required_cols = "a")
  expect_false(rb$rebuild)
})

test_that("an input newer than the output triggers a rebuild", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  Sys.sleep(0.05)
  inp <- make_parquet(file.path(dir, "in.parquet"))
  rb <- fia_should_rebuild(out, input_paths = inp)
  expect_true(rb$rebuild)
  expect_match(rb$reason, "input newer")
})

test_that("a missing required column triggers a rebuild (schema drift)", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"), tibble(a = 1:3))
  rb <- fia_should_rebuild(out, input_paths = character(0),
                           required_cols = c("a", "b"))
  expect_true(rb$rebuild)
  expect_match(rb$reason, "schema")
})

test_that("a missing declared input cannot leave a stale output marked current", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  missing_input <- file.path(dir, "missing.parquet")
  rb <- fia_should_rebuild(out, input_paths = missing_input)
  expect_true(rb$rebuild)
  expect_match(rb$reason, "declared input missing")
})

test_that("--force (option) forces a rebuild of an otherwise-fresh output", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  withr::defer(options(fia_force_rebuild = FALSE))
  options(fia_force_rebuild = TRUE)
  expect_true(fia_should_rebuild(out)$rebuild)
  expect_true(fia_force_requested("plot_tree_metrics"))
  options(fia_force_rebuild = c("plot_tree_metrics"))
  expect_true(fia_force_requested("plot_tree_metrics"))
  expect_false(fia_force_requested("plot_seedling_metrics"))
  options(fia_force_rebuild = FALSE)
})

test_that("write_parquet_atomic leaves no partial file and is readable", {
  dir <- tempfile("atomic"); dir.create(dir)
  out <- file.path(dir, "x.parquet")
  write_parquet_atomic(tibble(a = 1:5, b = letters[1:5]), out)
  expect_true(file.exists(out))
  # No leftover temp files in the directory.
  leftovers <- list.files(dir, pattern = "_tmp_")
  expect_length(leftovers, 0)
  d <- arrow::read_parquet(out)
  expect_equal(nrow(d), 5)
})

test_that("write_parquet_atomic atomically replaces an existing product", {
  dir <- tempfile("atomic_replace"); dir.create(dir)
  out <- file.path(dir, "x.parquet")
  write_parquet_atomic(tibble(version = 1L), out)
  write_parquet_atomic(tibble(version = 2L), out)
  expect_equal(arrow::read_parquet(out)$version, 2L)
  expect_length(list.files(dir, pattern = "_tmp_"), 0)
})
