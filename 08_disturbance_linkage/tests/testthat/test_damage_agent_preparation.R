source(here::here("tests/testthat/helpers.R"))
source(here::here("scripts/utils/forest_analysis.R"))
source(here::here("scripts/utils/fia_damage_agents.R"))

suppressPackageStartupMessages({
  library(data.table)
})

test_that("damage-agent preparation preserves evidence and neutral candidates", {
  conditions <- data.table(
    PLT_CN = c(1, 1),
    INVYR = c(2020L, 2020L),
    CONDID = c(1L, 2L),
    stable_plot_id = c("plot-1", "plot-1"),
    state = c("XX", "XX"),
    COND_STATUS_CD = c(1L, 2L),
    CONDPROP_UNADJ = c(0.8, 0.2),
    is_forested_condition = c(TRUE, FALSE),
    forested_plot_proportion = c(0.8, 0.8),
    forested_condition_weight = c(1, NA_real_),
    MANUAL = c(9.4, 5.1)
  )
  trees <- data.table(
    CN = bit64::as.integer64(101:104),
    PLT_CN = c(1, 1, 1, 1),
    INVYR = rep(2020L, 4),
    CONDID = c(1L, 1L, 2L, 1L),
    SUBP = c(1L, 1L, 1L, 1L),
    TREE = 1:4,
    SPCD = c(100L, 100L, 200L, 100L),
    STATUSCD = c(1L, 1L, 1L, 2L),
    DIA = c(10, 5, 8, 12),
    TPA_UNADJ = c(2, 3, 4, 5),
    DAMAGE_AGENT_CD1 = c(11000L, 11000L, 11000L, 11000L),
    DAMAGE_AGENT_CD2 = c(11000L, 30000L, 0L, 0L),
    DAMAGE_AGENT_CD3 = c(0L, 0L, 0L, 0L)
  )
  lookup <- data.table(
    DAMAGE_AGENT_CD = c(11000L, 30000L),
    official_label = c("Bark Beetles", "Fire"),
    agent_group = c("Bark Beetles", "Fire"),
    is_insect_agent = c(TRUE, FALSE),
    region = c("All", "All"),
    code_status = c("current_in_v9.4", "current_in_v9.4"),
    review_status = c(
      "official_v9.4_definition_only",
      "official_v9.4_definition_only"
    )
  )

  result <- fia_prepare_damage_agent_state(
    trees,
    conditions,
    lookup,
    dia_min_inches = 1,
    invyr_min = 2000,
    invyr_max = 2024
  )

  evidence <- result$tree_evidence
  denominators <- result$condition_denominators
  candidates <- result$condition_agent_candidates

  expect_equal(
    evidence[
      TREE_CN == bit64::as.integer64(101) & DAMAGE_AGENT_CD == 11000L,
      .N
    ],
    1L
  )
  expect_equal(
    evidence[
      TREE_CN == bit64::as.integer64(101) & DAMAGE_AGENT_CD == 11000L,
      n_source_agent_slots
    ],
    2L
  )
  expect_true(any(evidence$is_forested_condition == FALSE))
  expect_true(any(evidence$STATUSCD == 2L))
  expect_equal(
    unique(evidence[CONDID == 1L, definition_applicability_status]),
    "v9.4_definition_matches_visit_manual"
  )
  expect_equal(
    unique(evidence[CONDID == 2L, definition_applicability_status]),
    "historical_manual_definition_not_validated"
  )

  expect_equal(
    denominators[CONDID == 1L, eligible_tree_record_count],
    2L
  )
  expect_equal(
    denominators[CONDID == 2L, eligible_tree_record_count],
    1L
  )
  expect_true(all(
    denominators$denominator_definition_id ==
      "live_tree_dia_ge_1in_positive_tpa_unadj"
  ))
  bark <- candidates[CONDID == 1L & DAMAGE_AGENT_CD == 11000L]
  expect_equal(bark$affected_tree_record_count, 2L)
  expect_equal(bark$tree_record_fraction, 1)
  expect_equal(
    nrow(result$insect_severity_candidates),
    candidates[is_insect_agent == TRUE, .N]
  )
  expect_false(any(candidates$primary_severity_measure_selected))
  expect_false(any(candidates$condition_area_weighting_applied))
})
