#!/usr/bin/env Rscript

# Build a coherent cumulative tree risk set over each complete first-to-last
# stable-condition history. A tree is added once: at the first risk visit where
# it is observed alive in that condition. If it later dies, its original entry
# abundance weight is used in the numerator. Final-visit live entrants are not
# included because they have no subsequent mortality observation window.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(here)
})

source(here("scripts/utils/parquet_atomic.R"))

edge_path <- file.path(
  "09_analysis", "data", "intermediate", "complete_history_edges.parquet"
)
response_path <- file.path(
  "09_analysis", "data", "intermediate", "first_last_cwm_response.parquet"
)
death_path <- file.path(
  "09_analysis", "data", "intermediate", "interval_verified_deaths.parquet"
)
raw_dir <- file.path("05_fia", "data", "raw")
out_dir <- file.path("09_analysis", "data", "processed")
qa_dir <- file.path(
  "09_analysis", "qa", "outputs", "04_cumulative_mortality"
)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

for (path in c(edge_path, response_path, death_path)) {
  if (!file.exists(path)) stop("Missing input: ", path)
}

as_id <- function(x) {
  z <- as.character(x)
  z[z %chin% c("", "NA")] <- NA_character_
  z
}

prop_for_tree <- function(diameter, tpa, micro, subplot, macro, generic) {
  specific <- fcase(
    !is.na(diameter) & diameter >= 1 & diameter < 5, micro,
    !is.na(tpa) & abs(tpa - 0.999188) < 0.02, macro,
    !is.na(diameter) & diameter >= 5, subplot,
    default = NA_real_
  )
  fifelse(!is.na(specific) & specific > 0, specific, generic)
}

edges <- as.data.table(read_parquet(edge_path))
responses <- as.data.table(read_parquet(response_path))
verified_deaths <- as.data.table(read_parquet(death_path))
for (nm in c("PREV_PLT_CN", "T2_PLT_CN")) edges[, (nm) := as_id(get(nm))]
for (nm in c("first_PLT_CN", "last_PLT_CN")) responses[, (nm) := as_id(get(nm))]
for (nm in c("PREV_PLT_CN", "T2_PLT_CN", "T1_TRE_CN", "current_TRE_CN")) {
  verified_deaths[, (nm) := as_id(get(nm))]
}
edges[, history_id := paste(remeasurement_component_id, CONDID, sep = "|")]
responses[, history_id := paste(remeasurement_component_id, CONDID, sep = "|")]

if (edges[, anyDuplicated(stable_condition_interval_key)]) {
  stop("History edges are not unique by stable condition interval key")
}

risk_visit_cols <- c(
  "history_id", "stable_plot_id", "remeasurement_component_id", "state",
  "CONDID", "t1_visit_number", "PREV_PLT_CN", "T1_CONDPROP_UNADJ",
  "T1_MICRPROP_UNADJ", "T1_SUBPPROP_UNADJ", "T1_MACRPROP_UNADJ"
)
risk_visits <- unique(edges[, ..risk_visit_cols])
setnames(risk_visits, c("t1_visit_number", "PREV_PLT_CN"),
         c("risk_visit_number", "PLT_CN"))

agents <- c("fire", "insect", "disease")
state_results <- list()
state_flow <- list()

for (state_name in sort(unique(risk_visits$state))) {
  message("Cumulative risk sets: ", state_name)
  rv <- risk_visits[state == state_name]
  ed <- edges[state == state_name]
  tree_path <- file.path(raw_dir, state_name, paste0(state_name, "_TREE.csv"))
  if (!file.exists(tree_path)) stop("Missing FIA TREE input: ", tree_path)

  tree <- fread(
    tree_path,
    select = c(
      "CN", "PLT_CN", "PREV_TRE_CN", "CONDID", "STATUSCD", "DIA",
      "TPA_UNADJ", "P2A_GRM_FLG"
    ),
    showProgress = FALSE
  )
  tree[, `:=`(
    CN = as_id(CN),
    PLT_CN = as_id(PLT_CN),
    PREV_TRE_CN = as_id(PREV_TRE_CN)
  )]
  tree <- tree[PLT_CN %chin% rv$PLT_CN]

  # A lineage enters the denominator the first time it is observed alive in a
  # risk visit. P2A rows are excluded consistently with interval mortality.
  live <- tree[
    STATUSCD == 1L & !is.na(DIA) & DIA >= 1 &
      (is.na(P2A_GRM_FLG) | P2A_GRM_FLG != "Y")
  ]
  live <- merge(
    rv,
    live,
    by = c("PLT_CN", "CONDID"),
    all = FALSE,
    allow.cartesian = TRUE,
    sort = FALSE
  )
  live[, condition_prop_used := prop_for_tree(
    DIA, TPA_UNADJ, T1_MICRPROP_UNADJ, T1_SUBPPROP_UNADJ,
    T1_MACRPROP_UNADJ, T1_CONDPROP_UNADJ
  )]
  live[, valid_weight := !is.na(TPA_UNADJ) & TPA_UNADJ > 0 &
         !is.na(condition_prop_used) & condition_prop_used > 0]
  live[, candidate_entry_weight := fifelse(
    valid_weight, TPA_UNADJ / condition_prop_used, NA_real_
  )]

  if (live[, anyDuplicated(paste(history_id, risk_visit_number, CN, sep = "|"))]) {
    stop("Duplicate live tree within a history visit in state ", state_name)
  }

  mapped_parts <- list()
  entry_parts <- list()
  previous_map <- NULL
  for (visit_number in sort(unique(live$risk_visit_number))) {
    current <- live[risk_visit_number == visit_number]
    if (visit_number == 1L) {
      current[, `:=`(
        linked_within_condition_history = FALSE,
        entry_PLT_CN = PLT_CN,
        entry_TRE_CN = CN,
        entry_weight = candidate_entry_weight,
        entry_visit_number = risk_visit_number
      )]
    } else {
      predecessor <- if (is.null(previous_map)) {
        data.table(
          history_id = character(), predecessor_TRE_CN = character(),
          inherited_entry_PLT_CN = character(), inherited_entry_TRE_CN = character(),
          inherited_entry_weight = numeric(), inherited_entry_visit_number = integer()
        )
      } else {
        previous_map[, .(
          history_id,
          predecessor_TRE_CN = CN,
          inherited_entry_PLT_CN = entry_PLT_CN,
          inherited_entry_TRE_CN = entry_TRE_CN,
          inherited_entry_weight = entry_weight,
          inherited_entry_visit_number = entry_visit_number
        )]
      }
      current <- merge(
        current,
        predecessor,
        by.x = c("history_id", "PREV_TRE_CN"),
        by.y = c("history_id", "predecessor_TRE_CN"),
        all.x = TRUE,
        sort = FALSE
      )
      current[, linked_within_condition_history := !is.na(inherited_entry_TRE_CN)]
      current[, `:=`(
        entry_PLT_CN = fifelse(
          linked_within_condition_history, inherited_entry_PLT_CN, PLT_CN
        ),
        entry_TRE_CN = fifelse(
          linked_within_condition_history, inherited_entry_TRE_CN, CN
        ),
        entry_weight = fifelse(
          linked_within_condition_history, inherited_entry_weight,
          candidate_entry_weight
        ),
        entry_visit_number = fifelse(
          linked_within_condition_history, inherited_entry_visit_number,
          risk_visit_number
        )
      )]
      current[, c(
        "inherited_entry_PLT_CN", "inherited_entry_TRE_CN",
        "inherited_entry_weight", "inherited_entry_visit_number"
      ) := NULL]
    }

    current[, is_population_entry := !linked_within_condition_history]
    entry_parts[[as.character(visit_number)]] <-
      current[is_population_entry == TRUE]
    mapped_parts[[as.character(visit_number)]] <- current
    previous_map <- current[, .(
      history_id, CN, entry_PLT_CN, entry_TRE_CN, entry_weight,
      entry_visit_number
    )]
  }

  mapped <- rbindlist(mapped_parts, fill = TRUE)
  entries <- rbindlist(entry_parts, fill = TRUE)
  entries <- unique(entries, by = c("history_id", "entry_PLT_CN", "entry_TRE_CN"))

  denominator <- entries[, .(
    cumulative_population_records = .N,
    cumulative_population_abundance = sum(entry_weight),
    invalid_entry_weight_records = sum(!valid_weight | is.na(entry_weight)),
    baseline_live_records = sum(entry_visit_number == 1L),
    baseline_live_abundance = sum(entry_weight[entry_visit_number == 1L]),
    intermediate_entry_records = sum(entry_visit_number > 1L),
    intermediate_entry_abundance = sum(entry_weight[entry_visit_number > 1L])
  ), by = .(
    history_id, stable_plot_id, remeasurement_component_id, state, CONDID
  )]

  deaths <- verified_deaths[state == state_name, .(
    stable_condition_interval_key, PREV_PLT_CN, T1_TRE_CN,
    current_TRE_CN, agent_family
  )]
  deaths <- merge(
    deaths,
    ed[, .(
      stable_condition_interval_key, history_id, stable_plot_id,
      remeasurement_component_id, CONDID
    )],
    by = "stable_condition_interval_key",
    all = FALSE,
    sort = FALSE
  )
  map_for_death <- mapped[, .(
    history_id,
    PREV_PLT_CN = PLT_CN,
    T1_TRE_CN = CN,
    entry_PLT_CN,
    entry_TRE_CN,
    entry_weight
  )]
  deaths <- merge(
    deaths,
    map_for_death,
    by = c("history_id", "PREV_PLT_CN", "T1_TRE_CN"),
    all.x = TRUE,
    sort = FALSE
  )
  deaths <- unique(
    deaths,
    by = c("history_id", "current_TRE_CN", "agent_family")
  )

  modeled_deaths <- deaths[agent_family %chin% agents]
  death_flow <- modeled_deaths[, .(
    modeled_death_records = .N,
    unmapped_modeled_death_records = sum(is.na(entry_weight))
  ), by = history_id]

  death_summary <- modeled_deaths[!is.na(entry_weight), .(
    death_records = .N,
    death_abundance = sum(entry_weight)
  ), by = .(
    history_id, stable_plot_id, remeasurement_component_id, state, CONDID,
    agent_family
  )]
  if (nrow(death_summary)) {
    death_wide <- dcast(
      death_summary,
      history_id + stable_plot_id + remeasurement_component_id + state + CONDID ~
        agent_family,
      value.var = c("death_records", "death_abundance"),
      fill = 0
    )
  } else {
    death_wide <- denominator[0, .(
      history_id, stable_plot_id, remeasurement_component_id, state, CONDID
    )]
    for (agent in agents) {
      death_wide[, (paste0("death_records_", agent)) := numeric()]
      death_wide[, (paste0("death_abundance_", agent)) := numeric()]
    }
  }

  out <- merge(
    denominator,
    death_wide,
    by = c(
      "history_id", "stable_plot_id", "remeasurement_component_id", "state",
      "CONDID"
    ),
    all.x = TRUE,
    sort = FALSE
  )
  out <- merge(out, death_flow, by = "history_id", all.x = TRUE, sort = FALSE)
  zero_cols <- c(
    "modeled_death_records", "unmapped_modeled_death_records",
    as.vector(outer(c("death_records", "death_abundance"), agents, paste, sep = "_"))
  )
  for (nm in zero_cols) {
    if (!nm %in% names(out)) out[, (nm) := 0]
    set(out, which(is.na(out[[nm]])), nm, 0)
  }
  for (agent in agents) {
    record_col <- paste0("death_records_", agent)
    abundance_col <- paste0("death_abundance_", agent)
    setnames(out, record_col, paste0(agent, "_death_records"))
    setnames(out, abundance_col, paste0(agent, "_death_abundance"))
    out[, (paste0(agent, "_cumulative_mortality_pct")) :=
      100 * get(paste0(agent, "_death_abundance")) /
        cumulative_population_abundance]
    rate_col <- paste0(agent, "_cumulative_mortality_pct")
    # Exact 100% mortality can land a few machine-epsilon units above 100.
    # Normalize only that numerical artifact; larger violations still fail.
    near_100 <- which(out[[rate_col]] > 100 & out[[rate_col]] <= 100 + 1e-8)
    if (length(near_100)) set(out, near_100, rate_col, 100)
  }
  out[, cumulative_riskset_complete :=
        invalid_entry_weight_records == 0 & unmapped_modeled_death_records == 0]

  state_results[[state_name]] <- out
  state_flow[[state_name]] <- data.table(
    state = state_name,
    histories = nrow(out),
    population_entries = nrow(entries),
    intermediate_entries = sum(entries$entry_visit_number > 1L),
    modeled_deaths = nrow(modeled_deaths),
    unmapped_modeled_deaths = sum(is.na(modeled_deaths$entry_weight))
  )
}

mortality <- rbindlist(state_results, fill = TRUE)
flow <- rbindlist(state_flow, fill = TRUE)

for (agent in agents) {
  rate_col <- paste0(agent, "_cumulative_mortality_pct")
  if (mortality[, any(get(rate_col) > 100 + 1e-8, na.rm = TRUE)]) {
    stop(agent, " cumulative mortality exceeds 100%; risk-set invariant failed")
  }
}

model_data <- merge(
  responses,
  mortality[cumulative_riskset_complete == TRUE],
  by = c(
    "history_id", "stable_plot_id", "remeasurement_component_id", "state",
    "CONDID"
  ),
  all = FALSE,
  sort = FALSE
)
setorder(model_data, state, stable_plot_id, remeasurement_component_id, CONDID, layer)

mortality_output <- mortality[cumulative_riskset_complete %in% TRUE]
mortality_output[, c(
  "invalid_entry_weight_records", "unmapped_modeled_death_records",
  "cumulative_riskset_complete"
) := NULL]
write_parquet_atomic(
  mortality_output,
  file.path(out_dir, "history_cumulative_mortality.parquet")
)
model_output <- copy(model_data)
model_output[, c(
  "invalid_entry_weight_records", "unmapped_modeled_death_records",
  "cumulative_riskset_complete"
) := NULL]
write_parquet_atomic(
  model_output,
  file.path(out_dir, "lifestage_model_base.parquet")
)
fwrite(flow, file.path(qa_dir, "cumulative_tree_flow_by_state.csv"))

model_flow <- model_data[, .(
  model_rows = .N,
  temperature_rows = sum(!is.na(delta_temperature)),
  precipitation_rows = sum(!is.na(delta_precipitation)),
  CWD_rows = sum(!is.na(delta_CWD)),
  positive_fire_rows = sum(fire_cumulative_mortality_pct > 0),
  positive_insect_rows = sum(insect_cumulative_mortality_pct > 0),
  positive_disease_rows = sum(disease_cumulative_mortality_pct > 0),
  rows_with_intermediate_entries = sum(intermediate_entry_records > 0)
), by = layer]
fwrite(model_flow, file.path(qa_dir, "cumulative_model_flow.csv"))

bounds <- model_data[, .(
  history_layer_rows = .N,
  fire_over_100 = sum(fire_cumulative_mortality_pct > 100 + 1e-8),
  insect_over_100 = sum(insect_cumulative_mortality_pct > 100 + 1e-8),
  disease_over_100 = sum(disease_cumulative_mortality_pct > 100 + 1e-8),
  max_fire_pct = max(fire_cumulative_mortality_pct),
  max_insect_pct = max(insect_cumulative_mortality_pct),
  max_disease_pct = max(disease_cumulative_mortality_pct)
)]
fwrite(bounds, file.path(qa_dir, "cumulative_mortality_bounds.csv"))

message(
  "Wrote ", format(uniqueN(mortality$history_id), big.mark = ","),
  " cumulative histories and ", format(nrow(model_data), big.mark = ","),
  " history-layer model rows."
)
