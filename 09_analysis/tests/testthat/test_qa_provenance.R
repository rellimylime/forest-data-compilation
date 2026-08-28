library(testthat)

manifest_path <- here::here("09_analysis", "qa", "qa_products.csv")
products <- read.csv(manifest_path, stringsAsFactors = FALSE)

test_that("every QA product has one existing producer", {
  expect_false(anyDuplicated(products$output_path) > 0)
  expect_true(all(file.exists(here::here(products$producer_script))))
  expect_true(all(startsWith(
    products$output_path,
    paste0("09_analysis/qa/outputs/", products$stage, "/")
  )))
})

test_that("each producer explicitly names its QA output", {
  named <- vapply(seq_len(nrow(products)), function(i) {
    producer <- readLines(
      here::here(products$producer_script[[i]]),
      warn = FALSE
    )
    any(grepl(
      basename(products$output_path[[i]]),
      producer,
      fixed = TRUE
    ))
  }, logical(1))
  expect_true(all(named))
})

test_that("every QA producer is tracked by Git", {
  tracked <- system2(
    "git",
    c("-C", shQuote(here::here()), "ls-files"),
    stdout = TRUE
  )
  expect_true(all(products$producer_script %in% tracked))
})
