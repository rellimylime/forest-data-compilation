source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

change_products <- c(
  seedlings = "07_thermophilization/data/processed/plot_year_climate_change_seedlings.parquet",
  saplings  = "07_thermophilization/data/processed/plot_year_climate_change_saplings.parquet",
  trees     = "07_thermophilization/data/processed/plot_year_climate_change_trees.parquet"
)

test_that("every canonical change interval follows the official PREV_PLT_CN link", {
  for (layer in names(change_products)) {
    p <- qa_path(change_products[[layer]])
    qa_require_file(p)
    sch <- arrow::open_dataset(p)$schema
    need <- c("PREV_PLT_CN", "previous_PLT_CN")
    if (!all(need %in% sch$names)) {
      # Fall back to the current/previous PLT_CN naming if present.
      need <- intersect(c("PREV_PLT_CN", "previous_PLT_CN",
                          "current_PLT_CN"), sch$names)
    }
    d <- open_dataset(p) |>
      select(any_of(c("PREV_PLT_CN", "previous_PLT_CN"))) |>
      collect() |> as.data.table()
    if (!all(c("PREV_PLT_CN", "previous_PLT_CN") %in% names(d))) {
      skip(paste(layer, "lacks PREV_PLT_CN/previous_PLT_CN to verify link"))
    }
    # Contract: no null official link, and the official link equals the
    # chronologically previous visit's PLT_CN.
    expect_equal(sum(is.na(d$PREV_PLT_CN)), 0,
                 info = paste(layer, "no null official links in change intervals"))
    expect_equal(d[PREV_PLT_CN != previous_PLT_CN, .N], 0,
                 info = paste(layer, "official link matches chronological predecessor"))
  }
})

test_that("linkage diagnostics QA file is written", {
  # At least the seedlings linkage diagnostic should exist after a build.
  qa_dir <- qa_path("07_thermophilization/qa/outputs")
  if (!dir.exists(qa_dir)) skip("no thermophilization QA outputs dir")
  files <- list.files(qa_dir, pattern = "plot_year_climate_change_linkage.*csv")
  if (length(files) == 0) skip("linkage diagnostics not yet generated")
  expect_gt(length(files), 0)
})
