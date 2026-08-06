# Shared preparation helpers for FIA TREE damage-agent evidence.

suppressPackageStartupMessages({
  library(data.table)
})

fia_damage_required_tree_cols <- c(
  "CN", "PLT_CN", "INVYR", "CONDID", "SUBP", "TREE", "SPCD",
  "STATUSCD", "DIA", "TPA_UNADJ",
  "DAMAGE_AGENT_CD1", "DAMAGE_AGENT_CD2", "DAMAGE_AGENT_CD3"
)

fia_add_damage_definition_applicability <- function(x) {
  out <- copy(as.data.table(x))

  # Add missing context fields so older state tables can use the same logic.
  if (!"MANUAL" %in% names(out)) out[, MANUAL := NA_real_]
  if (!"region" %in% names(out)) out[, region := NA_character_]
  if (!"code_status" %in% names(out)) {
    out[, code_status := NA_character_]
  }
  if (!"review_status" %in% names(out)) {
    out[, review_status := NA_character_]
  }

  # State whether the current v9.4 label is supported for the visit's manual.
  out[, definition_applicability_status := fcase(
    review_status == "unresolved_code_not_in_lookup" |
      is.na(review_status),
    "code_not_in_v9.4_lookup",
    code_status == "retired_as_of_manual_9.4",
    "retired_code_historical_definition_not_validated",
    is.na(MANUAL),
    "manual_missing_current_definition_only",
    MANUAL == 9.4,
    "v9.4_definition_matches_visit_manual",
    MANUAL < 9.4,
    "historical_manual_definition_not_validated",
    MANUAL > 9.4,
    "post_v9.4_manual_definition_not_validated",
    default = "manual_definition_not_validated"
  )]
  # Keep regional lookup limits visible instead of guessing state applicability.
  out[, region_applicability_status := fcase(
    review_status == "unresolved_code_not_in_lookup" |
      is.na(review_status),
    "code_not_in_v9.4_lookup",
    is.na(region) | !nzchar(region),
    "v9.4_region_field_blank",
    region == "All",
    "v9.4_national_scope",
    default = "v9.4_region_specific_scope_not_mapped_to_state"
  )]
  out[]
}

fia_prepare_damage_agent_state <- function(
  trees,
  conditions,
  lookup,
  dia_min_inches = 1,
  invyr_min = -Inf,
  invyr_max = Inf
) {
  tree <- copy(as.data.table(trees))
  condition <- copy(as.data.table(conditions))
  agent_lookup <- copy(as.data.table(lookup))

  # Older condition extracts may not contain every protocol field.
  for (column in c("MEASYEAR", "MEASMON", "MANUAL", "KINDCD", "CYCLE")) {
    if (!column %in% names(condition)) condition[, (column) := NA_real_]
  }

  # Fail before processing if any input cannot support the declared products.
  forest_assert_columns(
    tree,
    fia_damage_required_tree_cols,
    "Raw FIA TREE table"
  )
  forest_assert_columns(
    condition,
    c(
      "PLT_CN", "INVYR", "CONDID", "stable_plot_id", "state",
      "COND_STATUS_CD", "CONDPROP_UNADJ", "is_forested_condition"
    ),
    "FIA condition foundation"
  )
  forest_assert_columns(
    agent_lookup,
    c(
      "DAMAGE_AGENT_CD", "official_label", "agent_group",
      "is_insect_agent", "review_status"
    ),
    "FIA damage-agent lookup"
  )
  forest_assert_unique(
    condition,
    c("PLT_CN", "INVYR", "CONDID"),
    "FIA condition foundation"
  )
  forest_assert_unique(
    agent_lookup,
    "DAMAGE_AGENT_CD",
    "FIA damage-agent lookup"
  )

  # Restrict records to the requested inventory window without changing their grain.
  tree <- tree[
    !is.na(INVYR) & INVYR >= invyr_min & INVYR <= invyr_max
  ]
  # Preserve FIA's tree-record identifier under a name that states its source.
  tree[, TREE_CN := CN]
  tree[, CN := NULL]
  tree[, tree_key_duplicate_in_state := duplicated(TREE_CN) |
    duplicated(TREE_CN, fromLast = TRUE)]
  n_tree_cn_duplicate_rows <- tree[tree_key_duplicate_in_state == TRUE, .N]

  # Eligible trees are live, at least the chosen diameter, and have positive TPA.
  tree[, is_eligible_denominator :=
    STATUSCD == 1L &
      !is.na(DIA) & DIA >= dia_min_inches &
      !is.na(TPA_UNADJ) & TPA_UNADJ > 0]
  # Convert FIA diameter and unadjusted TPA into square feet of basal area per acre.
  tree[, basal_area_sqft_per_acre_from_tpa_unadj := fifelse(
    !is.na(DIA) & !is.na(TPA_UNADJ),
    0.005454 * DIA^2 * TPA_UNADJ,
    NA_real_
  )]

  condition_cols <- intersect(
    c(
      "PLT_CN", "INVYR", "CONDID", "stable_plot_id", "state",
      "COND_STATUS_CD", "CONDPROP_UNADJ", "is_forested_condition",
      "forested_plot_proportion", "forested_condition_weight",
      "MEASYEAR", "MEASMON", "MANUAL", "KINDCD", "CYCLE"
    ),
    names(condition)
  )
  # Attach forest status and condition area without dropping unmatched trees.
  tree <- merge(
    tree,
    condition[, ..condition_cols],
    by = c("PLT_CN", "INVYR", "CONDID"),
    all.x = TRUE,
    sort = FALSE
  )
  tree[, condition_join_status := fifelse(
    is.na(stable_plot_id),
    "condition_unmatched",
    "condition_matched"
  )]

  # Build the three possible denominators before looking at damage-agent codes.
  denominator_values <- tree[
    condition_join_status == "condition_matched" &
      is_eligible_denominator == TRUE,
    .(
      eligible_tree_record_count = uniqueN(TREE_CN),
      eligible_tpa_unadj_sum = sum(TPA_UNADJ, na.rm = TRUE),
      eligible_basal_area_sqft_per_acre = sum(
        basal_area_sqft_per_acre_from_tpa_unadj,
        na.rm = TRUE
      )
    ),
    by = .(PLT_CN, INVYR, CONDID)
  ]
  denominator_cols <- intersect(
    c(
      "PLT_CN", "INVYR", "CONDID", "stable_plot_id", "state",
      "COND_STATUS_CD", "CONDPROP_UNADJ", "is_forested_condition",
      "forested_plot_proportion", "forested_condition_weight",
      "MEASYEAR", "MEASMON", "MANUAL", "KINDCD", "CYCLE"
    ),
    names(condition)
  )
  # Retain conditions with no eligible trees so absence stays explicit.
  denominators <- merge(
    condition[, ..denominator_cols],
    denominator_values,
    by = c("PLT_CN", "INVYR", "CONDID"),
    all.x = TRUE,
    sort = FALSE
  )
  denominators[
    is.na(eligible_tree_record_count),
    `:=`(
      eligible_tree_record_count = 0L,
      eligible_tpa_unadj_sum = 0,
      eligible_basal_area_sqft_per_acre = 0
    )
  ]
  denominators[, `:=`(
    denominator_definition_id = paste0(
      "live_tree_dia_ge_",
      format(as.numeric(dia_min_inches), trim = TRUE),
      "in_positive_tpa_unadj"
    ),
    eligible_status_codes = "1",
    minimum_diameter_inches = as.numeric(dia_min_inches),
    requires_positive_tpa = TRUE,
    mortality_agents_included = FALSE
  )]

  # Turn the three FIA damage-agent slots into one exact-code evidence table.
  evidence <- melt(
    tree,
    id.vars = setdiff(
      names(tree),
      paste0("DAMAGE_AGENT_CD", 1:3)
    ),
    measure.vars = paste0("DAMAGE_AGENT_CD", 1:3),
    variable.name = "agent_slot",
    value.name = "DAMAGE_AGENT_CD",
    variable.factor = FALSE
  )
  evidence <- evidence[
    !is.na(DAMAGE_AGENT_CD) & DAMAGE_AGENT_CD != 0L
  ]
  evidence[, DAMAGE_AGENT_CD := as.integer(DAMAGE_AGENT_CD)]
  evidence[, agent_slot := as.integer(sub(
    "DAMAGE_AGENT_CD",
    "",
    agent_slot,
    fixed = TRUE
  ))]
  n_agent_slots <- nrow(evidence)

  # Collapse a repeated code on one tree so its abundance is counted only once.
  evidence <- evidence[, .(
    stable_plot_id = first(stable_plot_id),
    state = first(state),
    SUBP = first(SUBP),
    TREE = first(TREE),
    SPCD = first(SPCD),
    STATUSCD = first(STATUSCD),
    DIA = first(DIA),
    TPA_UNADJ = first(TPA_UNADJ),
    basal_area_sqft_per_acre_from_tpa_unadj =
      first(basal_area_sqft_per_acre_from_tpa_unadj),
    COND_STATUS_CD = first(COND_STATUS_CD),
    CONDPROP_UNADJ = first(CONDPROP_UNADJ),
    is_forested_condition = first(is_forested_condition),
    forested_plot_proportion = first(forested_plot_proportion),
    forested_condition_weight = first(forested_condition_weight),
    MEASYEAR = first(MEASYEAR),
    MEASMON = first(MEASMON),
    MANUAL = first(MANUAL),
    KINDCD = first(KINDCD),
    CYCLE = first(CYCLE),
    condition_join_status = first(condition_join_status),
    is_eligible_denominator = first(is_eligible_denominator),
    source_agent_slots = paste(sort(unique(agent_slot)), collapse = ";"),
    n_source_agent_slots = uniqueN(agent_slot)
  ), by = .(
    PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD
  )]
  n_tree_agent_pairs <- nrow(evidence)

  # Add reviewed v9.4 labels while retaining codes missing from the lookup.
  evidence <- merge(
    evidence,
    agent_lookup,
    by = "DAMAGE_AGENT_CD",
    all.x = TRUE,
    sort = FALSE
  )
  evidence[is.na(review_status), `:=`(
    review_status = "unresolved_code_not_in_lookup",
    is_insect_agent = NA
  )]
  evidence <- fia_add_damage_definition_applicability(evidence)
  evidence[, tree_agent_key_definition :=
    "PLT_CN+INVYR+CONDID+TREE_CN+DAMAGE_AGENT_CD"]

  # Aggregate affected records, TPA, and basal area separately for each exact code.
  affected <- evidence[
    condition_join_status == "condition_matched" &
      is_eligible_denominator == TRUE,
    .(
      affected_tree_record_count = uniqueN(TREE_CN),
      affected_tpa_unadj_sum = sum(TPA_UNADJ, na.rm = TRUE),
      affected_basal_area_sqft_per_acre = sum(
        basal_area_sqft_per_acre_from_tpa_unadj,
        na.rm = TRUE
      ),
      n_source_agent_slots = sum(n_source_agent_slots),
      source_agent_slots = paste(
        sort(unique(unlist(strsplit(
          source_agent_slots,
          ";",
          fixed = TRUE
        )))),
        collapse = ";"
      )
    ),
    by = .(
      PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD
    )
  ]
  candidates <- merge(
    affected,
    denominators,
    by = c("PLT_CN", "INVYR", "CONDID"),
    all.x = TRUE,
    sort = FALSE
  )
  candidates <- merge(
    candidates,
    agent_lookup,
    by = "DAMAGE_AGENT_CD",
    all.x = TRUE,
    sort = FALSE
  )
  # Calculate alternatives without selecting a primary severity measure.
  candidates[, `:=`(
    tree_record_fraction = fifelse(
      eligible_tree_record_count > 0,
      affected_tree_record_count / eligible_tree_record_count,
      NA_real_
    ),
    tpa_unadj_fraction = fifelse(
      eligible_tpa_unadj_sum > 0,
      affected_tpa_unadj_sum / eligible_tpa_unadj_sum,
      NA_real_
    ),
    basal_area_fraction = fifelse(
      eligible_basal_area_sqft_per_acre > 0,
      affected_basal_area_sqft_per_acre /
        eligible_basal_area_sqft_per_acre,
      NA_real_
    ),
    primary_severity_measure_selected = FALSE,
    condition_area_weighting_applied = FALSE
  )]
  candidates[is.na(review_status), `:=`(
    review_status = "unresolved_code_not_in_lookup",
    is_insect_agent = NA
  )]
  candidates <- fia_add_damage_definition_applicability(candidates)
  # The insect view is only a filter of the complete exact-agent candidate table.
  insect_candidates <- candidates[is_insect_agent == TRUE]

  setorder(evidence, state, PLT_CN, INVYR, CONDID, TREE_CN, DAMAGE_AGENT_CD)
  setorder(denominators, state, PLT_CN, INVYR, CONDID)
  setorder(candidates, state, PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD)
  setorder(insect_candidates, state, PLT_CN, INVYR, CONDID, DAMAGE_AGENT_CD)

  list(
    tree_evidence = evidence[],
    condition_denominators = denominators[],
    condition_agent_candidates = candidates[],
    insect_severity_candidates = insect_candidates[],
    audit = data.table(
      n_tree_rows_in_inventory_window = nrow(tree),
      n_tree_cn_duplicate_rows_in_state = n_tree_cn_duplicate_rows,
      n_tree_agent_source_slots = n_agent_slots,
      n_tree_agent_pairs = n_tree_agent_pairs,
      n_duplicate_tree_agent_slots_removed =
        n_agent_slots - n_tree_agent_pairs,
      n_unmatched_tree_agent_pairs =
        evidence[condition_join_status == "condition_unmatched", .N],
      n_unresolved_tree_agent_pairs =
        evidence[review_status == "unresolved_code_not_in_lookup", .N]
    )
  )
}
