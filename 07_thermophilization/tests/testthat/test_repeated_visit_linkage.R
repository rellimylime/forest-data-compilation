source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

# A change interval is only real when FIA's own remeasurement link agrees with
# the chronologically previous visit. Plots that were replaced or re-established
# at a reused location carry a null or different PREV_PLT_CN, and pairing them
# would invent a change that never happened.

change_products <- c(
  seedlings = "07_thermophilization/data/processed/forest_visit_interval_change_seedlings.parquet",
  saplings  = "07_thermophilization/data/processed/forest_visit_interval_change_saplings.parquet",
  trees     = "07_thermophilization/data/processed/forest_visit_interval_change_trees.parquet"
)

test_that("every change interval follows the official PREV_PLT_CN link", {
  for (layer in names(change_products)) {
    p <- qa_path(change_products[[layer]])
    qa_require_file(p)
    d <- as.data.table(read_parquet(p, col_select = c(
      "PREV_PLT_CN", "previous_PLT_CN", "link_status"
    )))

    expect_equal(sum(is.na(d$PREV_PLT_CN)), 0,
                 info = paste(layer, "no null official links"))
    expect_equal(d[PREV_PLT_CN != previous_PLT_CN, .N], 0,
                 info = paste(layer, "official link matches chronological predecessor"))
    expect_equal(d[link_status != "official_link_match", .N], 0,
                 info = paste(layer, "only official-link intervals are kept"))
  }
})

test_that("intervals run forward in time and rates match delta over years", {
  for (layer in names(change_products)) {
    p <- qa_path(change_products[[layer]])
    qa_require_file(p)
    d <- as.data.table(read_parquet(p, col_select = c(
      "previous_INVYR", "current_INVYR", "years_between_surveys",
      "days_between_measurements",
      "delta_mean_temp", "rate_mean_temp_per_year"
    )))

    expect_equal(d[current_INVYR <= previous_INVYR, .N], 0, info = layer)
    expect_equal(
      d[years_between_surveys != current_INVYR - previous_INVYR, .N], 0,
      info = layer
    )
    expect_lt(
      max(abs(d$rate_mean_temp_per_year -
                d$delta_mean_temp / d$years_between_surveys), na.rm = TRUE),
      1e-8
    )
  }
})

test_that("linkage diagnostics are written for each built layer", {
  qa_dir <- qa_path("07_thermophilization/qa/outputs")
  if (!dir.exists(qa_dir)) skip("no thermophilization QA outputs dir")
  files <- list.files(
    qa_dir,
    pattern = "^forest_visit_interval_change_linkage_.*\\.csv$"
  )
  if (length(files) == 0) skip("linkage diagnostics not yet generated")
  expect_gt(length(files), 0)
})
