source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

# The forest plot-visit CWM is the analysis response. Its contract is:
#   1. only forested conditions contribute, and
#   2. each one is weighted by its share of the visit's forested area.
# These tests recompute that from the two upstream products and compare.

visit_products <- c(
  seedlings = "07_thermophilization/data/processed/forest_plot_visit_cwm_seedlings.parquet",
  saplings  = "07_thermophilization/data/processed/forest_plot_visit_cwm_saplings.parquet",
  trees     = "07_thermophilization/data/processed/forest_plot_visit_cwm_trees.parquet"
)
condition_products <- c(
  seedlings = "07_thermophilization/data/processed/plot_community_climate_seedlings.parquet",
  saplings  = "07_thermophilization/data/processed/plot_community_climate_saplings.parquet",
  trees     = "07_thermophilization/data/processed/plot_community_climate_trees.parquet"
)
foundation_product <-
  "05_fia/data/processed/summaries/forested_condition_foundation.parquet"

test_that("every row declares the forest-only, area-weighted estimand", {
  for (layer in names(visit_products)) {
    p <- qa_path(visit_products[[layer]])
    qa_require_file(p)
    d <- as.data.table(read_parquet(p, col_select = c(
      "community_layer", "forest_conditions_only",
      "condition_area_weighting", "partial_forest_handling",
      "forested_condition_weight_with_layer"
    )))

    expect_true(all(d$forest_conditions_only), info = layer)
    expect_equal(
      unique(d$condition_area_weighting),
      "CONDPROP_UNADJ_normalized_over_forested_conditions",
      info = layer
    )
    expect_equal(unique(d$community_layer), layer)
    # Weights are normalized within the visit, so they cannot exceed the whole.
    expect_lte(max(d$forested_condition_weight_with_layer, na.rm = TRUE), 1 + 1e-9)
  }
})

test_that("plot-visit CWM equals condition CWMs weighted by forested area share", {
  foundation_path <- qa_path(foundation_product)
  qa_require_file(foundation_path)
  foundation <- as.data.table(read_parquet(foundation_path, col_select = c(
    "PLT_CN", "INVYR", "CONDID", "is_forested_condition", "forested_condition_weight"
  )))

  for (layer in names(visit_products)) {
    p_visit <- qa_path(visit_products[[layer]])
    p_cond <- qa_path(condition_products[[layer]])
    qa_require_file(p_visit)
    qa_require_file(p_cond)

    conditions <- as.data.table(read_parquet(p_cond, col_select = c(
      "PLT_CN", "INVYR", "CONDID", "mean_temp"
    )))
    conditions <- merge(
      conditions, foundation,
      by = c("PLT_CN", "INVYR", "CONDID"), all.x = TRUE, sort = FALSE
    )
    conditions <- conditions[
      !is.na(is_forested_condition) & is_forested_condition &
        !is.na(mean_temp) & !is.na(forested_condition_weight) &
        forested_condition_weight > 0
    ]
    recomputed <- conditions[, .(
      expected_mean_temp =
        sum(mean_temp * forested_condition_weight) / sum(forested_condition_weight)
    ), by = .(PLT_CN, INVYR)]

    actual <- as.data.table(read_parquet(p_visit, col_select = c(
      "PLT_CN", "INVYR", "mean_temp"
    )))
    compared <- merge(recomputed, actual, by = c("PLT_CN", "INVYR"))
    if (nrow(compared) == 0) skip(paste("no comparable visits for", layer))

    expect_equal(nrow(compared), nrow(recomputed), info = layer)
    expect_lt(
      max(abs(compared$mean_temp - compared$expected_mean_temp), na.rm = TRUE),
      1e-9
    )
  }
})

test_that("visits with no forested condition are absent from the product", {
  foundation_path <- qa_path(foundation_product)
  qa_require_file(foundation_path)
  foundation <- as.data.table(read_parquet(foundation_path, col_select = c(
    "PLT_CN", "INVYR", "is_forested_condition"
  )))
  nonforest_visits <- foundation[, .(
    any_forest = any(is_forested_condition)
  ), by = .(PLT_CN, INVYR)][any_forest == FALSE]
  if (nrow(nonforest_visits) == 0) skip("no fully nonforest visits in the foundation")

  for (layer in names(visit_products)) {
    p <- qa_path(visit_products[[layer]])
    qa_require_file(p)
    visits <- as.data.table(read_parquet(p, col_select = c("PLT_CN", "INVYR")))
    leaked <- merge(
      visits, nonforest_visits[, .(PLT_CN, INVYR)],
      by = c("PLT_CN", "INVYR")
    )
    expect_equal(nrow(leaked), 0L, info = layer)
  }
})
