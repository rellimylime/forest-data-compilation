source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

source(here::here("scripts/utils/fia_seedling.R"))

# ---- Unit tests of the eligibility contract ----------------------------------

test_that("records with null TREECOUNT but positive calculated abundance are kept", {
  treecount      <- c(NA,  NA, 3L,  NA, NA, NA)
  treecount_calc <- c(6,    6, 3,   0,  NA, NA)
  tpa_unadj      <- c(449.8, NA, 224.9, NA, NA, 74.9)
  keep <- seedling_eligible(treecount, treecount_calc, tpa_unadj)
  # row1 calc+tpa, row2 calc-only (the ME/CO restoration), row3 both,
  # row6 tpa-only -> all kept
  expect_equal(keep, c(TRUE, TRUE, TRUE, FALSE, FALSE, TRUE))
})

test_that("zero or invalid calculated counts remain excluded", {
  keep <- seedling_eligible(
    treecount      = c(NA, NA, NA),
    treecount_calc = c(0, -1, NA),
    tpa_unadj      = c(NA, NA, 0)
  )
  expect_true(all(!keep))
})

test_that("raw TREECOUNT is used only when no calculated field is available", {
  # Whole column lacks calc + tpa -> fall back to raw TREECOUNT.
  keep <- seedling_eligible(
    treecount      = c(5L, 0L, NA),
    treecount_calc = c(NA, NA, NA),
    tpa_unadj      = c(NA, NA, NA)
  )
  expect_equal(keep, c(TRUE, FALSE, FALSE))
})

# ---- Product-level regression assertions -------------------------------------

test_that("seedling species product is unique at its declared grain", {
  p <- qa_path("05_fia/data/processed/summaries/plot_seedling_species.parquet")
  qa_require_file(p)
  d <- open_dataset(p) |>
    select(PLT_CN, INVYR, CONDID, SUBP, SPCD) |>
    collect() |> as.data.table()
  expect_equal(nrow(unique(d)), nrow(d))
})

test_that("previously affected seedling states retain records (completeness)", {
  # State partitions carry the restored records; TREECOUNT_CALC is fully populated.
  for (st in c("ME", "OR", "CA", "CO")) {
    dir_st <- qa_path(file.path("05_fia/data/processed/seedlings", paste0("state=", st)))
    if (!dir.exists(dir_st)) { skip(paste("no seedling partition for", st)); next }
    d <- open_dataset(dir_st) |>
      select(any_of(c("treecount_total", "treecount_calc_total"))) |>
      collect() |> as.data.table()
    expect_gt(nrow(d), 0)
    if ("treecount_calc_total" %in% names(d)) {
      expect_true(all(!is.na(d$treecount_calc_total) & d$treecount_calc_total > 0),
                  info = paste(st, "treecount_calc_total populated"))
    }
  }
})
