source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

test_that("canonical FIA forest disturbance is bounded and slot-safe", {
  path <- qa_path(
    "08_disturbance_linkage/data/processed/fia_forest_disturbance_measures.parquet"
  )
  qa_require_file(path)
  schema <- open_dataset(path)$schema$names
  expect_false(any(grepl("is_high_severity|high_severity_threshold", schema)))

  data <- open_dataset(path) |>
    select(
      stable_plot_id, PLT_CN, INVYR,
      prop_any_fire_of_forested, prop_crown_fire_of_forested,
      prop_insect_of_forested, prop_disease_of_forested,
      fire_timing_definition
    ) |>
    collect() |>
    as.data.table()
  expect_equal(data[, .N, by = .(PLT_CN, INVYR)][N > 1L, .N], 0L)
  for (column in grep("^prop_", names(data), value = TRUE)) {
    values <- data[[column]]
    values <- values[!is.na(values)]
    expect_true(all(values >= 0 & values <= 1 + 1e-8), info = column)
  }
  expect_true(all(
    data$fire_timing_definition ==
      "same_slot_DSTRBCD1-3_to_DSTRBYR1-3_measurement_year_validated"
  ))

  evidence_path <- qa_path(
    "08_disturbance_linkage/data/processed/fia_fire_disturbance_slot_evidence.parquet"
  )
  qa_require_file(evidence_path)
  evidence <- as.data.table(read_parquet(
    evidence_path,
    col_select = c(
      "stable_plot_id", "PLT_CN", "INVYR", "CONDID",
      "disturbance_slot", "disturbance_code", "fire_year_status"
    )
  ))
  expect_equal(
    evidence[, .N, by = .(
      stable_plot_id, PLT_CN, INVYR, CONDID, disturbance_slot
    )][N > 1L, .N],
    0L
  )
  expect_true(all(evidence$disturbance_code %in% c(30L, 31L, 32L)))
  expect_true(all(evidence$fire_year_status %in% c(
    "valid", "continuous_or_unknown", "date_unavailable",
    "invalid_year_code", "post_measurement_year"
  )))
})

test_that("forest plot-visit products are unique forest plot visits", {
  for (stage in c("seedlings", "saplings", "trees")) {
    path <- qa_path(paste0(
      "07_thermophilization/data/processed/",
      "forest_plot_visit_cwm_", stage, ".parquet"
    ))
    qa_require_file(path)
    data <- open_dataset(path) |>
      select(
        stable_plot_id, PLT_CN, INVYR, forest_conditions_only,
        forested_plot_proportion, forested_condition_weight_with_layer
      ) |>
      collect() |>
      as.data.table()
    expect_equal(data[, .N, by = .(PLT_CN, INVYR)][N > 1L, .N], 0L)
    expect_true(all(data$forest_conditions_only))
    expect_true(all(data$forested_plot_proportion > 0, na.rm = TRUE))
    expect_true(
      all(
        data$forested_condition_weight_with_layer >= 0 &
          data$forested_condition_weight_with_layer <= 1 + 1e-8,
        na.rm = TRUE
      )
    )
  }
})

test_that("first-to-last product remains unique and ordered", {
  path <- qa_path(
    "07_thermophilization/data/processed/forest_first_last_change.parquet"
  )
  qa_require_file(path)
  data <- as.data.table(read_parquet(
    path,
    col_select = c(
      "stable_plot_id", "remeasurement_component_id",
      "first_PLT_CN", "last_PLT_CN",
      "measurement_date_lower_first", "measurement_date_lower_last",
      "n_visits_observed"
    )
  ))
  # The key is the remeasurement component, not the stable plot. A plot whose
  # PREV_PLT_CN chain is broken holds several separate histories and correctly
  # contributes one row per history; requiring uniqueness by stable_plot_id
  # would force those back into a single pair spanning the break.
  expect_equal(anyDuplicated(data$remeasurement_component_id), 0L)
  expect_true(all(data$first_PLT_CN != data$last_PLT_CN))
  expect_true(all(data$n_visits_observed >= 2L))
  expect_true(all(
    data$measurement_date_lower_first <= data$measurement_date_lower_last,
    na.rm = TRUE
  ))
})

test_that("neutral FIA damage candidates are bounded and do not select a primary", {
  path <- qa_path(
    "08_disturbance_linkage/data/processed/fia_condition_damage_agent_candidates"
  )
  qa_require_dir(path)
  data <- open_dataset(path) |>
    select(
      "stable_plot_id", "PLT_CN", "INVYR", "CONDID", "DAMAGE_AGENT_CD",
      "COND_STATUS_CD", "tree_record_fraction", "tpa_unadj_fraction",
      "basal_area_fraction", "primary_severity_measure_selected",
      "condition_area_weighting_applied",
      "mortality_agents_included"
    ) |>
    collect() |>
    as.data.table()
  expect_equal(
    data[, .N, by = .(
      PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD
    )][N > 1L, .N],
    0L
  )
  for (column in grep("_fraction$", names(data), value = TRUE)) {
    expect_true(
      all(data[[column]] >= 0 & data[[column]] <= 1 + 1e-8, na.rm = TRUE),
      info = column
    )
  }
  expect_true(any(data$COND_STATUS_CD != 1L, na.rm = TRUE))
  expect_false(any(data$primary_severity_measure_selected))
  expect_false(any(data$condition_area_weighting_applied))
  expect_false(any(data$mortality_agents_included))
})

test_that("tree damage evidence and denominators preserve their declared grains", {
  evidence_path <- qa_path(
    "08_disturbance_linkage/data/processed/fia_tree_damage_agent_evidence"
  )
  denominator_path <- qa_path(
    "08_disturbance_linkage/data/processed/fia_condition_damage_denominators"
  )
  qa_require_dir(evidence_path)
  qa_require_dir(denominator_path)

  evidence <- open_dataset(evidence_path) |>
    select(
      PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD,
      n_source_agent_slots, source_agent_slots
    ) |>
    collect() |>
    as.data.table()
  expect_equal(
    evidence[, .N, by = .(
      PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD
    )][N > 1L, .N],
    0L
  )
  expect_true(all(evidence$n_source_agent_slots >= 1L))
  expect_true(all(!is.na(evidence$source_agent_slots)))

  denominators <- open_dataset(denominator_path) |>
    select(
      stable_plot_id, PLT_CN, INVYR, CONDID,
      eligible_tree_record_count, eligible_tpa_unadj_sum,
      eligible_basal_area_sqft_per_acre, denominator_definition_id,
      eligible_status_codes, minimum_diameter_inches,
      requires_positive_tpa
    ) |>
    collect() |>
    as.data.table()
  expect_equal(
    denominators[, .N, by = .(
      stable_plot_id, PLT_CN, INVYR, CONDID
    )][N > 1L, .N],
    0L
  )
  expect_true(all(denominators$eligible_tree_record_count >= 0L))
  expect_true(all(denominators$eligible_tpa_unadj_sum >= 0))
  expect_true(all(denominators$eligible_basal_area_sqft_per_acre >= 0))
  expect_true(all(
    denominators$denominator_definition_id ==
      "live_tree_dia_ge_1in_positive_tpa_unadj"
  ))
  expect_true(all(denominators$requires_positive_tpa))
})

test_that("official FIA damage-agent lookup is complete at exact-code grain", {
  path <- qa_path("05_fia/lookups/fia_damage_agent_lookup.csv")
  qa_require_file(path)
  lookup <- fread(path)
  expect_equal(anyDuplicated(lookup$DAMAGE_AGENT_CD), 0L)
  expect_gte(nrow(lookup), 966L)
  expect_true("scientific_name_or_other" %in% names(lookup))
  expect_true(all(!is.na(lookup$official_label)))
  expect_true(all(
    lookup$review_status == "official_v9.4_definition_only"
  ))
  expect_true(all(
    lookup$manual_version_applicability ==
      "not_established_by_source_appendix"
  ))
  expect_true(all(lookup$is_insect_agent %in% c(TRUE, FALSE)))
})

test_that("IDS exact-agent evidence does not mix in nondetection rows", {
  path <- qa_path(paste0(
    "08_disturbance_linkage/data/processed/",
    "ids_annual_agent_evidence/survey_year=2020.parquet"
  ))
  qa_require_file(path)
  data <- as.data.table(read_parquet(path))
  expect_equal(
    data[, .N, by = .(stable_plot_id, survey_year, dca_code)][N > 1L, .N],
    0L
  )
  expect_true(all(data$survey_year == 2020L))
  expect_false(any(is.na(data$dca_code)))
  expect_false(any(data$exact_agent_grouping_applied))
  expect_equal(
    data[!is.na(dca_code) & is.na(agent_label), .N],
    0L
  )
})
