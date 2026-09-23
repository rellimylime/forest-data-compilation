library(data.table)
library(here)
library(testthat)

source(here("09_analysis", "scripts", "utils", "model_samples.R"))

test_that("common histories contain both required life stages", {
  samples <- data.table(
    history_id = c(
      "both", "both", "all_three", "all_three", "all_three",
      "wrong_pair", "wrong_pair", "adult_only"
    ),
    layer = c(
      "saplings", "trees", "saplings", "seedlings", "trees",
      "seedlings", "trees", "trees"
    )
  )

  expect_setequal(
    complete_history_ids_for_layers(samples, c("saplings", "trees")),
    c("both", "all_three")
  )
})
