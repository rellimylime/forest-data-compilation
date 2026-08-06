#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(here)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
fia_cfg <- config$processed$fia
root <- here(link_cfg$output_dir)
qa_dir <- here(link_cfg$qa_dir)
dir_create(qa_dir)
foundation_path <- file.path(
  here(fia_cfg$summaries$output_dir),
  fia_cfg$summaries$files$forested_condition_foundation
)

paths <- list(
  evidence = file.path(root, link_cfg$files$fia_tree_damage_agent_evidence),
  denominators = file.path(
    root,
    link_cfg$files$fia_condition_damage_denominators
  ),
  candidates = file.path(
    root,
    link_cfg$files$fia_condition_damage_agent_candidates
  ),
  insects = file.path(
    root,
    link_cfg$files$fia_insect_severity_candidates
  )
)
missing <- names(paths)[!dir.exists(unlist(paths))]
if (length(missing) > 0L) {
  stop("Missing damage-agent product(s): ", paste(missing, collapse = ", "))
}

evidence <- open_dataset(paths$evidence) |>
  select(
    state, PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD,
    COND_STATUS_CD, is_forested_condition, condition_join_status,
    n_source_agent_slots, review_status, is_insect_agent, MANUAL,
    definition_applicability_status, region_applicability_status
  ) |>
  collect() |>
  as.data.table()
denominators <- open_dataset(paths$denominators) |>
  select(
    stable_plot_id, state, PLT_CN, INVYR, CONDID, COND_STATUS_CD,
    eligible_tree_record_count, eligible_tpa_unadj_sum,
    eligible_basal_area_sqft_per_acre,
    denominator_definition_id, minimum_diameter_inches,
    requires_positive_tpa
  ) |>
  collect() |>
  as.data.table()
candidates <- open_dataset(paths$candidates) |>
  collect() |>
  as.data.table()
insects <- open_dataset(paths$insects) |>
  select(
    stable_plot_id, PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD,
    COND_STATUS_CD, is_forested_condition, is_insect_agent,
    definition_applicability_status
  ) |>
  collect() |>
  as.data.table()
foundation_keys <- as.data.table(read_parquet(
  foundation_path,
  col_select = c(
    "stable_plot_id", "PLT_CN", "INVYR", "CONDID", "COND_STATUS_CD"
  )
))

checks <- data.table(
  check = c(
    "evidence_key_unique",
    "denominator_key_unique",
    "candidate_key_unique",
    "insect_key_unique",
    "all_evidence_codes_present_in_v9_4_lookup",
    "definition_applicability_status_complete",
    "all_insect_rows_officially_insect",
    "insect_condition_fields_retained",
    "denominators_retain_all_foundation_conditions",
    "candidate_counts_within_denominators",
    "candidate_tpa_within_denominators",
    "candidate_basal_area_within_denominators",
    "candidate_fractions_bounded",
    "no_primary_measure_selected",
    "no_condition_area_weighting_applied",
    "structured_denominator_definition_complete"
  ),
  passed = c(
    evidence[, .N, by = .(
      PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD
    )][N > 1L, .N] == 0L,
    denominators[, .N, by = .(
      stable_plot_id, PLT_CN, INVYR, CONDID
    )][N > 1L, .N] == 0L,
    candidates[, .N, by = .(
      stable_plot_id, PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD
    )][N > 1L, .N] == 0L,
    insects[, .N, by = .(
      stable_plot_id, PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD
    )][N > 1L, .N] == 0L,
    evidence[
      review_status != "official_v9.4_definition_only" |
        is.na(review_status),
      .N
    ] == 0L,
    evidence[
      is.na(definition_applicability_status) |
        is.na(region_applicability_status),
      .N
    ] == 0L,
    insects[is_insect_agent != TRUE | is.na(is_insect_agent), .N] == 0L,
    all(c("COND_STATUS_CD", "is_forested_condition") %in% names(insects)),
    fsetequal(
      denominators[, .(
        stable_plot_id, PLT_CN, INVYR, CONDID, COND_STATUS_CD
      )],
      foundation_keys
    ),
    candidates[
      affected_tree_record_count > eligible_tree_record_count,
      .N
    ] == 0L,
    candidates[
      affected_tpa_unadj_sum > eligible_tpa_unadj_sum + 1e-8,
      .N
    ] == 0L,
    candidates[
      affected_basal_area_sqft_per_acre >
        eligible_basal_area_sqft_per_acre + 1e-8,
      .N
    ] == 0L,
    candidates[
      tree_record_fraction < 0 | tree_record_fraction > 1 + 1e-8 |
        tpa_unadj_fraction < 0 | tpa_unadj_fraction > 1 + 1e-8 |
        basal_area_fraction < 0 | basal_area_fraction > 1 + 1e-8,
      .N
    ] == 0L,
    candidates[primary_severity_measure_selected == TRUE, .N] == 0L,
    candidates[condition_area_weighting_applied == TRUE, .N] == 0L,
    denominators[
      is.na(denominator_definition_id) |
        is.na(minimum_diameter_inches) |
        is.na(requires_positive_tpa),
      .N
    ] == 0L
  )
)

summary <- data.table(
  metric = c(
    "tree_agent_evidence_rows",
    "condition_denominator_rows",
    "condition_agent_candidate_rows",
    "insect_severity_candidate_rows",
    "exact_damage_agent_codes",
    "official_insect_agent_codes_observed",
    "evidence_rows_on_forested_conditions",
    "evidence_rows_on_nonforest_conditions",
    "evidence_rows_with_unknown_condition_status",
    "conditions_with_zero_eligible_trees",
    "tree_agent_pairs_with_repeated_source_slots",
    "unmatched_tree_agent_pairs",
    "codes_missing_from_v9_4_lookup",
    "evidence_rows_with_historical_definition_not_validated"
  ),
  value = c(
    nrow(evidence),
    nrow(denominators),
    nrow(candidates),
    nrow(insects),
    uniqueN(evidence$DAMAGE_AGENT_CD),
    uniqueN(evidence[is_insect_agent == TRUE, DAMAGE_AGENT_CD]),
    evidence[COND_STATUS_CD == 1L, .N],
    evidence[COND_STATUS_CD != 1L, .N],
    evidence[is.na(COND_STATUS_CD), .N],
    denominators[eligible_tree_record_count == 0L, .N],
    evidence[n_source_agent_slots > 1L, .N],
    evidence[get("condition_join_status") == "condition_unmatched", .N],
    evidence[
      review_status != "official_v9.4_definition_only" |
        is.na(review_status),
      .N
    ],
    evidence[
      definition_applicability_status ==
        "historical_manual_definition_not_validated",
      .N
    ]
  )
)

condition_status_label <- function(code) {
  fcase(
    code == 1L, "accessible_forest_land",
    code == 2L, "nonforest_land",
    code == 3L, "noncensus_water",
    code == 4L, "census_water",
    code == 5L, "nonsampled_possible_forest",
    is.na(code), "unknown_or_unmatched",
    default = "other_code"
  )
}

condition_status_counts <- rbindlist(lapply(
  list(
    tree_evidence = evidence,
    condition_denominators = denominators,
    condition_agent_candidates = candidates,
    insect_severity_candidates = insects
  ),
  function(product) {
    product[, .(
      rows = .N,
      conditions = uniqueN(paste(PLT_CN, INVYR, CONDID, sep = "|"))
    ), by = .(
      COND_STATUS_CD,
      condition_status = condition_status_label(COND_STATUS_CD)
    )]
  }),
  idcol = "product"
)
setorder(condition_status_counts, product, COND_STATUS_CD)

definition_applicability <- evidence[, .(
  evidence_rows = .N,
  exact_codes = uniqueN(DAMAGE_AGENT_CD),
  visits = uniqueN(paste(PLT_CN, INVYR, sep = "|"))
), by = .(
  MANUAL,
  definition_applicability_status,
  region_applicability_status
)]
setorder(definition_applicability, MANUAL, definition_applicability_status)

fwrite(
  checks,
  file.path(qa_dir, "fia_damage_agent_validation_checks.csv")
)
fwrite(
  summary,
  file.path(qa_dir, "fia_damage_agent_summary.csv")
)
fwrite(
  condition_status_counts,
  file.path(qa_dir, "fia_damage_agent_condition_status_counts.csv")
)
fwrite(
  definition_applicability,
  file.path(qa_dir, "fia_damage_agent_definition_applicability.csv")
)

print(checks)
cat("\n")
print(summary)
if (!all(checks$passed)) {
  stop("One or more FIA damage-agent validation checks failed.")
}
