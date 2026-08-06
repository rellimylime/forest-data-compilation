source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

test_that("forested-condition foundation has the declared grain and weights", {
  path <- qa_path(
    "05_fia/data/processed/summaries/forested_condition_foundation.parquet"
  )
  qa_require_file(path)
  data <- open_dataset(path) |>
    select(
      PLT_CN, INVYR, CONDID, COND_STATUS_CD, CONDPROP_UNADJ,
      is_forested_condition, forested_plot_proportion,
      forested_condition_weight
    ) |>
    collect() |>
    as.data.table()

  expect_equal(
    data[, .N, by = .(PLT_CN, INVYR, CONDID)][N > 1L, .N],
    0L
  )
  forest_weights <- data[
    is_forested_condition == TRUE &
      !is.na(forested_plot_proportion) &
      forested_plot_proportion > 0,
    .(weight_sum = sum(forested_condition_weight)),
    by = .(PLT_CN, INVYR)
  ]
  expect_lt(max(abs(forest_weights$weight_sum - 1)), 1e-8)
  expect_equal(
    data[is_forested_condition != TRUE & !is.na(forested_condition_weight), .N],
    0L
  )
  expect_lt(
    max(
      abs(
        data[
          is_forested_condition == TRUE &
            !is.na(forested_condition_weight),
          forested_condition_weight -
            CONDPROP_UNADJ / forested_plot_proportion
        ]
      ),
      na.rm = TRUE
    ),
    1e-12
  )
})
