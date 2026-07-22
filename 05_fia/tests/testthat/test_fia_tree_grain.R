source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

required_tree_state_cols <- c("PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD",
                              "COMMON_NAME", "SCIENTIFIC_NAME",
                              "ba_per_acre", "n_trees_tpa")

test_that("tree state partitions carry the condition/subplot/species grain", {
  tree_dir <- qa_path("05_fia/data/processed/trees")
  qa_require_dir(tree_dir)
  files <- list.files(tree_dir, pattern = "[.]parquet$", recursive = TRUE,
                      full.names = TRUE)
  if (length(files) == 0) skip("no tree partitions present")
  sch <- arrow::open_dataset(files[[1]])$schema
  missing <- setdiff(required_tree_state_cols, sch$names)
  expect(length(missing) == 0,
         paste("Tree partition missing required grain columns:",
               paste(missing, collapse = ", ")))
})

test_that("plot_tree_species is unique at PLT_CN x INVYR x CONDID x SUBP x SPCD", {
  p <- qa_path("05_fia/data/processed/summaries/plot_tree_species.parquet")
  qa_require_file(p)
  d <- open_dataset(p) |>
    select(PLT_CN, INVYR, CONDID, SUBP, SPCD) |>
    collect() |> as.data.table()
  qa_expect_cols(d, c("PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD"))
  expect_equal(nrow(unique(d)), nrow(d))
})

test_that("plot_sapling_species carries condition/subplot/species identity", {
  p <- qa_path("05_fia/data/processed/summaries/plot_sapling_species.parquet")
  qa_require_file(p)
  sch <- arrow::open_dataset(p)$schema
  qa_expect_cols(
    data.frame(matrix(ncol = length(sch$names), nrow = 0,
                      dimnames = list(NULL, sch$names))),
    c("PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD", "SCIENTIFIC_NAME")
  )
})
