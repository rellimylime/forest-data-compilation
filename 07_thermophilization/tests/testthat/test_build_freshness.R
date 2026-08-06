source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(tibble)
})

source(here::here("scripts/utils/build_freshness.R"))

make_parquet <- function(path, df = tibble(a = 1:3)) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(df, path)
  path
}

# ------------------------------------------------------------------------------
# --force parsing
# ------------------------------------------------------------------------------

test_that("--force with no value forces every product", {
  expect_true(build_force_from_args(c("--layer=trees", "--force")))
})

test_that("--force=<product> forces only the named products", {
  expect_equal(
    build_force_from_args(c("--force=forest_first_last_change")),
    "forest_first_last_change"
  )
  expect_equal(
    build_force_from_args(c("--force=a,b")),
    c("a", "b")
  )
})

test_that("no --force flag means nothing is forced", {
  expect_false(build_force_from_args(c("--layer=trees", "--limit=100")))
})

test_that("an empty --force= value forces everything rather than nothing", {
  expect_true(build_force_from_args("--force="))
})

# ------------------------------------------------------------------------------
# Rebuild decisions
# ------------------------------------------------------------------------------

test_that("an output newer than its inputs is skipped", {
  dir <- tempfile("fresh"); dir.create(dir)
  inp <- make_parquet(file.path(dir, "in.parquet"))
  Sys.sleep(0.05)
  out <- make_parquet(file.path(dir, "out.parquet"))
  rb <- build_should_rebuild(out, input_paths = inp, required_cols = "a")
  expect_false(rb$rebuild)
  expect_match(rb$reason, "up to date")
})

test_that("an input rebuilt after the output triggers a rebuild", {
  # This is the 07 staleness case: the foundation is refreshed after the CWM
  # products were written, so every downstream product must rebuild.
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  Sys.sleep(0.05)
  inp <- make_parquet(file.path(dir, "foundation.parquet"))
  rb <- build_should_rebuild(out, input_paths = inp)
  expect_true(rb$rebuild)
  expect_match(rb$reason, "input newer")
})

test_that("only the newest of several inputs has to be newer", {
  dir <- tempfile("fresh"); dir.create(dir)
  old_input <- make_parquet(file.path(dir, "old.parquet"))
  Sys.sleep(0.05)
  out <- make_parquet(file.path(dir, "out.parquet"))
  Sys.sleep(0.05)
  new_input <- make_parquet(file.path(dir, "new.parquet"))
  rb <- build_should_rebuild(out, input_paths = c(old_input, new_input))
  expect_true(rb$rebuild)
  expect_match(rb$reason, "input newer")
})

test_that("a missing required column triggers a rebuild (schema drift)", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"), tibble(a = 1:3))
  rb <- build_should_rebuild(out, required_cols = c("a", "b"))
  expect_true(rb$rebuild)
  expect_match(rb$reason, "schema")
})

test_that("a missing declared input cannot leave a stale output marked current", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  rb <- build_should_rebuild(out, input_paths = file.path(dir, "gone.parquet"))
  expect_true(rb$rebuild)
  expect_match(rb$reason, "declared input missing")
})

test_that("a missing output triggers a rebuild", {
  out <- file.path(tempfile("fresh"), "out.parquet")
  rb <- build_should_rebuild(out)
  expect_true(rb$rebuild)
  expect_match(rb$reason, "missing")
})

# ------------------------------------------------------------------------------
# Force option
# ------------------------------------------------------------------------------

test_that("the shared option forces an otherwise-fresh output", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  withr::defer(options(build_force_rebuild = FALSE))

  expect_false(build_should_rebuild(out)$rebuild)
  options(build_force_rebuild = TRUE)
  expect_true(build_should_rebuild(out)$rebuild)

  options(build_force_rebuild = "forest_first_last_change")
  expect_true(build_force_requested("forest_first_last_change"))
  expect_false(build_force_requested("forest_visit_interval_change_trees"))

  options(build_force_rebuild = "all")
  expect_true(build_force_requested("forest_visit_interval_change_trees"))
})

test_that("each module's force option is independent of the others", {
  # 05_fia keeps "fia_force_rebuild"; forcing it must not force the 07 stages.
  withr::defer(options(fia_force_rebuild = FALSE, build_force_rebuild = FALSE))
  options(fia_force_rebuild = TRUE, build_force_rebuild = FALSE)

  expect_true(build_force_requested("x", option = "fia_force_rebuild"))
  expect_false(build_force_requested("x"))
})

test_that("an explicit force argument overrides the option", {
  dir <- tempfile("fresh"); dir.create(dir)
  out <- make_parquet(file.path(dir, "out.parquet"))
  expect_true(build_should_rebuild(out, force = TRUE)$rebuild)
})

test_that("build_log_decision prints one line and returns the decision", {
  decision <- list(rebuild = FALSE, reason = "up to date")
  expect_output(
    result <- build_log_decision("forest_first_last_change.parquet", decision),
    "skip"
  )
  expect_equal(result, decision)
})
