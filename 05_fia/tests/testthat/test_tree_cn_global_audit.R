source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(data.table)
})

test_that("physical raw TREE.CN values are globally unique", {
  path <- qa_path(
    "05_fia/qa/outputs/fia_tree_cn_global_audit_summary.csv"
  )
  qa_require_file(path)
  audit <- fread(path)
  value <- setNames(audit$value, audit$metric)

  expect_equal(as.integer(value[["raw_tree_files"]]), 50L)
  expect_gt(as.numeric(value[["raw_tree_rows"]]), 0)
  expect_equal(as.integer(value[["duplicate_TREE_CN_values"]]), 0L)
  expect_equal(as.integer(value[["rows_with_duplicated_TREE_CN"]]), 0L)
  expect_equal(value[["TREE_CN_globally_unique"]], "TRUE")
})
