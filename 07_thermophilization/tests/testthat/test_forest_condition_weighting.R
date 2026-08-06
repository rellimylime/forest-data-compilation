source(here::here("tests/testthat/helpers.R"))
source(here::here("scripts/utils/forest_analysis.R"))

suppressPackageStartupMessages({
  library(data.table)
})

test_that("forested weights exclude nonforest and normalize over forest only", {
  conditions <- data.table(
    PLT_CN = rep(10, 3),
    INVYR = rep(2020L, 3),
    CONDID = 1:3,
    stable_plot_id = rep("x", 3),
    COND_STATUS_CD = c(1L, 2L, 1L),
    CONDPROP_UNADJ = c(0.2, 0.7, 0.1)
  )
  foundation <- build_forested_condition_foundation(conditions)

  expect_equal(
    foundation[is_forested_condition == TRUE, sum(forested_condition_weight)],
    1
  )
  expect_equal(
    foundation[CONDID == 1L, forested_condition_weight],
    2 / 3
  )
  expect_true(is.na(foundation[CONDID == 2L, forested_condition_weight]))
  expect_equal(
    unique(foundation$forested_plot_proportion),
    0.3
  )
})

test_that("nonsampled zero-proportion visits are not reported as sampled proportion errors", {
  conditions <- data.table(
    PLT_CN = 10,
    INVYR = 2000L,
    CONDID = 1L,
    stable_plot_id = "x",
    COND_STATUS_CD = 5L,
    CONDPROP_UNADJ = 0
  )

  foundation <- build_forested_condition_foundation(conditions)

  expect_true(foundation$nonsampled_zero_proportion)
  expect_false(foundation$condition_proportions_not_one)
  expect_equal(
    foundation$condition_proportion_quality_flag,
    "nonsampled_zero_proportion"
  )
})

test_that("condition disturbance flags count a condition once across slots", {
  conditions <- data.table(
    PLT_CN = 10,
    INVYR = 2020L,
    CONDID = 1L,
    stable_plot_id = "x",
    COND_STATUS_CD = 1L,
    CONDPROP_UNADJ = 1,
    DSTRBCD1 = 32L,
    DSTRBCD2 = 32L,
    DSTRBCD3 = 10L,
    DSTRBYR1 = 2018L,
    DSTRBYR2 = 2018L,
    DSTRBYR3 = 9999L
  )
  foundation <- build_forested_condition_foundation(conditions)

  expect_true(foundation$has_fire_condition)
  expect_true(foundation$has_crown_fire_condition)
  expect_true(foundation$has_insect_condition)
  expect_equal(foundation$valid_disturbance_codes, "10;32")
  expect_equal(foundation$valid_disturbance_years, "2018")
})

test_that("plot-visit CWM uses normalized forested condition weights", {
  conditions <- data.table(
    PLT_CN = rep(10, 3),
    INVYR = rep(2020L, 3),
    CONDID = 1:3,
    stable_plot_id = rep("x", 3),
    COND_STATUS_CD = c(1L, 2L, 1L),
    CONDPROP_UNADJ = c(0.2, 0.7, 0.1)
  )
  metrics <- data.table(
    PLT_CN = rep(10, 3),
    INVYR = rep(2020L, 3),
    CONDID = 1:3,
    cwm_temp = c(10, 100, 40)
  )
  foundation <- build_forested_condition_foundation(conditions)
  result <- aggregate_forested_condition_cwm(
    metrics,
    foundation,
    "cwm_temp"
  )

  expect_equal(result$cwm_temp, 20)
  expect_true(result$forest_conditions_only)
  expect_equal(result$forested_condition_weight_with_layer, 1)
})

test_that("the foundation decides forest status, not a copy in the metrics table", {
  # Community tables carry their own is_forested_condition. If a stale copy were
  # allowed to win the merge, a nonforest condition would be counted as forest.
  conditions <- data.table(
    PLT_CN = rep(10, 2),
    INVYR = rep(2020L, 2),
    CONDID = 1:2,
    stable_plot_id = rep("x", 2),
    COND_STATUS_CD = c(1L, 2L),
    CONDPROP_UNADJ = c(0.5, 0.5)
  )
  metrics <- data.table(
    PLT_CN = rep(10, 2),
    INVYR = rep(2020L, 2),
    CONDID = 1:2,
    is_forested_condition = c(TRUE, TRUE),
    cwm_temp = c(10, 100)
  )
  foundation <- build_forested_condition_foundation(conditions)
  result <- aggregate_forested_condition_cwm(metrics, foundation, "cwm_temp")

  expect_equal(result$cwm_temp, 10)
  expect_equal(result$n_forested_conditions_with_layer, 1L)
  expect_equal(
    attr(result, "condition_join_diagnostics")$shadowed_columns_dropped_from_metrics,
    "is_forested_condition"
  )
})

test_that("conditions absent from the foundation are counted, not silently dropped", {
  # FIA tree records exist whose CONDID has no COND row. They have no condition
  # status and no area weight, so they cannot be aggregated -- but the count has
  # to surface rather than vanishing into a left join.
  n <- 100L
  conditions <- data.table(
    PLT_CN = rep(seq_len(n), each = 2),
    INVYR = 2020L,
    CONDID = rep(1:2, times = n),
    stable_plot_id = paste0("p", rep(seq_len(n), each = 2)),
    COND_STATUS_CD = rep(c(1L, 2L), times = n),
    CONDPROP_UNADJ = 0.5
  )
  foundation <- build_forested_condition_foundation(conditions)
  metrics <- rbind(
    conditions[, .(PLT_CN, INVYR, CONDID,
                   cwm_temp = fifelse(CONDID == 1L, 10, 100))],
    data.table(PLT_CN = 1, INVYR = 2020L, CONDID = 99L, cwm_temp = 999)
  )
  result <- aggregate_forested_condition_cwm(metrics, foundation, "cwm_temp")
  diagnostics <- attr(result, "condition_join_diagnostics")

  expect_equal(diagnostics$n_conditions_missing_from_foundation, 1L)
  expect_equal(diagnostics$n_forested_condition_rows_aggregated, 100L)
  expect_equal(diagnostics$n_nonforest_condition_rows_excluded, 100L)
  expect_true(all(result$cwm_temp == 10))
})

test_that("a wholesale key mismatch between the two tables stops the build", {
  conditions <- data.table(
    PLT_CN = rep(10, 2),
    INVYR = rep(2020L, 2),
    CONDID = 1:2,
    stable_plot_id = rep("x", 2),
    COND_STATUS_CD = c(1L, 1L),
    CONDPROP_UNADJ = c(0.5, 0.5)
  )
  foundation <- build_forested_condition_foundation(conditions)
  metrics <- data.table(
    PLT_CN = rep(777, 2),
    INVYR = rep(2020L, 2),
    CONDID = 1:2,
    cwm_temp = c(10, 100)
  )

  expect_error(
    aggregate_forested_condition_cwm(metrics, foundation, "cwm_temp"),
    "missing from the forested-condition foundation"
  )
})
