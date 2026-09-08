#!/usr/bin/env Rscript

# Check whether modeled seedlings and agent-attributed deaths occupy the same
# FIA subplots within each stable-condition history.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(here)
  library(lmtest)
  library(sandwich)
})

model_path <- here(
  "09_analysis", "data", "processed", "lifestage_model_data.parquet"
)
seedling_path <- here(
  "05_fia", "data", "processed", "summaries",
  "plot_seedling_species.parquet"
)
condition_cwm_path <- here(
  "09_analysis", "data", "processed", "condition_visit_cwm.parquet"
)
edge_path <- here(
  "09_analysis", "data", "intermediate", "complete_history_edges.parquet"
)
death_path <- here(
  "09_analysis", "data", "intermediate", "interval_verified_deaths.parquet"
)
component_path <- here(
  "09_analysis", "data", "processed", "fia_remeasurement_components.parquet"
)
raw_tree_dir <- here("05_fia", "data", "raw")
output_dir <- here(
  "09_analysis", "qa", "outputs", "seedling_spatial_support"
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

required_inputs <- c(
  model_path, seedling_path, condition_cwm_path, edge_path, death_path,
  component_path
)
missing_inputs <- required_inputs[!file.exists(required_inputs)]
if (length(missing_inputs)) {
  stop("Missing input(s): ", paste(missing_inputs, collapse = "; "))
}

# Normalize FIA control numbers before joining raw and processed products.
as_id <- function(x) {
  out <- as.character(x)
  out[out %chin% c("", "NA")] <- NA_character_
  out
}

# Select the actual complete-case seedling cohort used by all three models.
model <- as.data.table(read_parquet(model_path))
predictors <- c(
  "fire_cumulative_mortality_pct",
  "insect_cumulative_mortality_pct",
  "disease_cumulative_mortality_pct",
  "cumulative_site_CWD_mm",
  "full_survey_period_years"
)
responses <- c("delta_temperature", "delta_precipitation", "delta_CWD")
model_histories <- model[
  layer == "seedlings" & cumulative_site_CWD_complete %in% TRUE &
    complete.cases(model[, c(predictors, responses), with = FALSE])
]
model_histories <- unique(model_histories[, c(
  "history_id", "stable_plot_id", "remeasurement_component_id", "state",
  "CONDID", "first_PLT_CN", "last_PLT_CN", predictors, responses
), with = FALSE], by = "history_id")
for (column in c("first_PLT_CN", "last_PLT_CN")) {
  model_histories[, (column) := as_id(get(column))]
}
if (model_histories[, anyDuplicated(history_id)]) {
  stop("Seedling model histories are not unique")
}

# Identify histories collected under the older potentially censored tally rule.
visit_protocol <- as.data.table(read_parquet(
  component_path,
  col_select = c("PLT_CN", "INVYR", "MANUAL")
))
visit_protocol[, PLT_CN := as_id(PLT_CN)]
visit_protocol <- unique(visit_protocol, by = "PLT_CN")
first_protocol <- copy(visit_protocol)
setnames(
  first_protocol,
  c("PLT_CN", "INVYR", "MANUAL"),
  c("first_PLT_CN", "first_INVYR", "first_MANUAL")
)
final_protocol <- copy(visit_protocol)
setnames(
  final_protocol,
  c("PLT_CN", "INVYR", "MANUAL"),
  c("last_PLT_CN", "final_INVYR", "final_MANUAL")
)
model_histories <- merge(
  model_histories, first_protocol,
  by = "first_PLT_CN", all.x = TRUE, sort = FALSE
)
model_histories <- merge(
  model_histories, final_protocol,
  by = "last_PLT_CN", all.x = TRUE, sort = FALSE
)

protocol_rows <- rbindlist(list(
  model_histories[, .(
    history_id, endpoint = "first", INVYR = first_INVYR,
    MANUAL = first_MANUAL
  )],
  model_histories[, .(
    history_id, endpoint = "final", INVYR = final_INVYR,
    MANUAL = final_MANUAL
  )]
))
protocol_rows[, manual_group := fcase(
  is.na(MANUAL), "missing",
  MANUAL < 2, "MANUAL < 2",
  default = "MANUAL >= 2"
)]
protocol_summary <- protocol_rows[, .(
  histories = uniqueN(history_id),
  first_inventory_year = if (all(is.na(INVYR))) NA_integer_ else min(INVYR, na.rm = TRUE),
  last_inventory_year = if (all(is.na(INVYR))) NA_integer_ else max(INVYR, na.rm = TRUE)
), by = .(endpoint, manual_group)][order(endpoint, manual_group)]
fwrite(
  protocol_summary,
  file.path(output_dir, "seedling_protocol_coverage.csv")
)

# Refit the seedling models after conservatively excluding every old-manual endpoint.
# Return clustered coefficient and model-fit summaries for one protocol scenario.
fit_protocol_model <- function(data, response, scenario) {
  outcome <- paste0("delta_", response)
  fit <- lm(reformulate(predictors, response = outcome), data = data)
  covariance <- sandwich::vcovCL(
    fit, cluster = data$stable_plot_id, type = "HC1"
  )
  test <- as.matrix(lmtest::coeftest(fit, vcov. = covariance))
  critical <- qt(0.975, df = df.residual(fit))
  data.table(
    scenario,
    response,
    term = rownames(test),
    estimate = test[, 1L],
    std_error = test[, 2L],
    statistic = test[, 3L],
    p_value = test[, 4L],
    conf_low = test[, 1L] - critical * test[, 2L],
    conf_high = test[, 1L] + critical * test[, 2L],
    n = nobs(fit),
    stable_plots = uniqueN(data$stable_plot_id)
  )
}
manual_2plus <- model_histories[
  !is.na(first_MANUAL) & first_MANUAL >= 2 &
    !is.na(final_MANUAL) & final_MANUAL >= 2
]
protocol_sensitivity <- rbindlist(lapply(
  c("temperature", "precipitation", "CWD"),
  function(response) {
    rbindlist(list(
      fit_protocol_model(model_histories, response, "baseline"),
      fit_protocol_model(manual_2plus, response, "manual_2plus_endpoints")
    ))
  }
))
protocol_sensitivity <- protocol_sensitivity[term %chin% predictors]
fwrite(
  protocol_sensitivity,
  file.path(output_dir, "seedling_protocol_sensitivity.csv")
)

# Preserve the FIA condition and microplot identifier carried by each tally.
seedlings <- as.data.table(read_parquet(
  seedling_path,
  col_select = c(
    "PLT_CN", "CONDID", "SUBP", "SPCD", "seedlings_tpa",
    "treecount_calc_total", "n_seedling_records"
  )
))
seedlings[, PLT_CN := as_id(PLT_CN)]
seedling_key <- c("PLT_CN", "CONDID", "SUBP", "SPCD")
duplicate_seedling_keys <- seedlings[
  , sum(duplicated(.SD)), .SDcols = seedling_key
]

# Match observed seedling microplots to the first and final modeled visits.
endpoints <- rbindlist(list(
  model_histories[, .(
    history_id, state, CONDID, endpoint = "first", PLT_CN = first_PLT_CN
  )],
  model_histories[, .(
    history_id, state, CONDID, endpoint = "final", PLT_CN = last_PLT_CN
  )]
))
positive_seedlings <- unique(seedlings[
  !is.na(seedlings_tpa) & seedlings_tpa > 0 & !is.na(SUBP),
  .(PLT_CN, CONDID, SUBP)
])
endpoint_subplots <- merge(
  endpoints,
  positive_seedlings,
  by = c("PLT_CN", "CONDID"),
  all.x = TRUE,
  allow.cartesian = TRUE,
  sort = FALSE
)
endpoint_counts <- endpoint_subplots[, .(
  n_seedling_subplots = uniqueN(SUBP[!is.na(SUBP)]),
  seedling_subplots = paste(sort(unique(SUBP[!is.na(SUBP)])), collapse = ";")
), by = .(history_id, endpoint)]

support_summary <- endpoint_counts[, .(
  histories = .N
), by = .(endpoint, n_seedling_subplots)][order(endpoint, n_seedling_subplots)]
fwrite(
  support_summary,
  file.path(output_dir, "seedling_endpoint_subplot_support.csv")
)

# Count the numbered microplots assigned partly or fully to each condition.
# A positive MICRCOND_PROP means that the condition occurs on that microplot.
eligible_parts <- list()
for (state_name in sort(unique(model_histories$state))) {
  state_endpoints <- endpoints[state == state_name]
  subp_cond_path <- file.path(
    raw_tree_dir, state_name, paste0(state_name, "_SUBP_COND.csv")
  )
  if (!file.exists(subp_cond_path)) {
    stop("Missing FIA SUBP_COND input: ", subp_cond_path)
  }
  subp_cond <- fread(
    subp_cond_path,
    select = c("PLT_CN", "CONDID", "SUBP", "MICRCOND_PROP"),
    showProgress = FALSE
  )
  subp_cond[, PLT_CN := as_id(PLT_CN)]
  subp_cond <- subp_cond[
    PLT_CN %chin% state_endpoints$PLT_CN &
      !is.na(MICRCOND_PROP) & MICRCOND_PROP > 0
  ]
  eligible_parts[[state_name]] <- unique(subp_cond[, .(
    PLT_CN, CONDID, SUBP, MICRCOND_PROP
  )])
}
eligible_microplots <- rbindlist(eligible_parts, fill = TRUE)
endpoint_eligible <- merge(
  endpoints,
  eligible_microplots,
  by = c("PLT_CN", "CONDID"),
  all.x = TRUE,
  allow.cartesian = TRUE,
  sort = FALSE
)[, .(
  n_eligible_microplots = uniqueN(SUBP[!is.na(SUBP)]),
  eligible_microplots = paste(
    sort(unique(SUBP[!is.na(SUBP)])), collapse = ";"
  )
), by = .(history_id, endpoint)]

endpoint_support <- merge(
  endpoint_eligible,
  endpoint_counts,
  by = c("history_id", "endpoint"),
  all = TRUE,
  sort = FALSE
)
endpoint_support[, seedling_microplot_fraction := fifelse(
  n_eligible_microplots > 0,
  n_seedling_subplots / n_eligible_microplots,
  NA_real_
)]
support_distribution <- endpoint_support[, .(
  histories = .N,
  histories_without_eligible_microplot = sum(n_eligible_microplots == 0),
  histories_with_seedlings_outside_eligible_microplots = sum(
    n_seedling_subplots > n_eligible_microplots
  ),
  mean_eligible_microplots = mean(n_eligible_microplots),
  median_eligible_microplots = median(n_eligible_microplots),
  mean_microplots_with_seedlings = mean(n_seedling_subplots),
  median_microplots_with_seedlings = median(n_seedling_subplots),
  mean_seedling_microplot_fraction = mean(
    seedling_microplot_fraction, na.rm = TRUE
  ),
  median_seedling_microplot_fraction = median(
    seedling_microplot_fraction, na.rm = TRUE
  )
), by = endpoint]
fwrite(
  support_distribution,
  file.path(output_dir, "seedling_microplot_support_summary.csv")
)

# Describe the starting seedling community used to calculate each CWM response.
condition_cwm <- as.data.table(read_parquet(
  condition_cwm_path,
  col_select = c(
    "PLT_CN", "CONDID", "layer", "n_species",
    "total_individual_abundance", "temperature_niche_abundance",
    "precipitation_niche_abundance", "CWD_niche_abundance"
  )
))
condition_cwm[, PLT_CN := as_id(PLT_CN)]
initial_seedlings <- merge(
  model_histories[, .(history_id, first_PLT_CN, CONDID)],
  condition_cwm[layer == "seedlings"],
  by.x = c("first_PLT_CN", "CONDID"),
  by.y = c("PLT_CN", "CONDID"),
  all.x = TRUE,
  sort = FALSE
)
initial_tallies <- merge(
  model_histories[, .(history_id, first_PLT_CN, CONDID)],
  seedlings,
  by.x = c("first_PLT_CN", "CONDID"),
  by.y = c("PLT_CN", "CONDID"),
  all = FALSE,
  allow.cartesian = TRUE,
  sort = FALSE
)[, .(
  initial_calculated_seedling_tally = sum(treecount_calc_total, na.rm = TRUE),
  initial_seedling_database_records = sum(n_seedling_records, na.rm = TRUE)
), by = history_id]
initial_seedlings <- merge(
  initial_seedlings,
  initial_tallies,
  by = "history_id",
  all.x = TRUE,
  sort = FALSE
)
initial_seedlings[, `:=`(
  temperature_niche_coverage = temperature_niche_abundance /
    total_individual_abundance,
  precipitation_niche_coverage = precipitation_niche_abundance /
    total_individual_abundance,
  CWD_niche_coverage = CWD_niche_abundance / total_individual_abundance
)]
initial_seedlings <- merge(
  initial_seedlings,
  endpoint_support[endpoint == "first", .(
    history_id, n_eligible_microplots, n_seedling_subplots,
    seedling_microplot_fraction
  )],
  by = "history_id",
  all.x = TRUE,
  sort = FALSE
)

# Save the history-level support fields used by downstream seedling QA.
seedling_history_support <- initial_seedlings[, .(
  history_id,
  initial_calculated_seedling_tally,
  initial_seedling_species_richness = n_species,
  n_eligible_microplots,
  n_seedling_subplots,
  seedling_microplot_fraction
)]
if (seedling_history_support[, anyDuplicated(history_id)]) {
  stop("Seedling history support is not unique")
}
write_parquet(
  seedling_history_support,
  file.path(output_dir, "seedling_history_support.parquet")
)

# Summarize the observed range and central tendency of support variables.
summarize_numeric <- function(data, columns) {
  rbindlist(lapply(columns, function(column) {
    value <- data[[column]]
    usable <- value[!is.na(value) & is.finite(value)]
    data.table(
      variable = column,
      histories = length(value),
      missing = sum(is.na(value) | !is.finite(value)),
      minimum = min(usable),
      p05 = quantile(usable, 0.05, names = FALSE),
      p25 = quantile(usable, 0.25, names = FALSE),
      median = median(usable),
      mean = mean(usable),
      p75 = quantile(usable, 0.75, names = FALSE),
      p95 = quantile(usable, 0.95, names = FALSE),
      maximum = max(usable)
    )
  }))
}
initial_summary <- summarize_numeric(
  initial_seedlings,
  c(
    "total_individual_abundance", "initial_calculated_seedling_tally",
    "initial_seedling_database_records", "n_species", "n_eligible_microplots",
    "n_seedling_subplots", "seedling_microplot_fraction",
    "temperature_niche_coverage", "precipitation_niche_coverage",
    "CWD_niche_coverage"
  )
)
fwrite(
  initial_summary,
  file.path(output_dir, "seedling_initial_condition_summary.csv")
)

# Attach each verified death to its recorded current subplot using raw TREE.
edges <- as.data.table(read_parquet(
  edge_path,
  col_select = c(
    "stable_condition_interval_key", "remeasurement_component_id", "CONDID"
  )
))
edges[, history_id := paste(remeasurement_component_id, CONDID, sep = "|")]
deaths <- as.data.table(read_parquet(death_path))
for (column in c("T2_PLT_CN", "current_TRE_CN")) {
  deaths[, (column) := as_id(get(column))]
}
deaths <- merge(
  deaths,
  edges[, .(stable_condition_interval_key, history_id)],
  by = "stable_condition_interval_key",
  all = FALSE,
  sort = FALSE
)
deaths <- deaths[history_id %chin% model_histories$history_id]
deaths <- unique(
  deaths,
  by = c("history_id", "current_TRE_CN", "agent_family")
)

tree_locations <- list()
for (state_name in sort(unique(deaths$state))) {
  state_deaths <- deaths[state == state_name]
  tree_path <- file.path(raw_tree_dir, state_name, paste0(state_name, "_TREE.csv"))
  if (!file.exists(tree_path)) stop("Missing FIA TREE input: ", tree_path)
  tree <- fread(
    tree_path,
    select = c("CN", "PLT_CN", "CONDID", "SUBP"),
    showProgress = FALSE
  )
  tree[, `:=`(CN = as_id(CN), PLT_CN = as_id(PLT_CN))]
  tree <- tree[
    PLT_CN %chin% state_deaths$T2_PLT_CN &
      CN %chin% state_deaths$current_TRE_CN
  ]
  if (tree[, anyDuplicated(paste(PLT_CN, CN, sep = "|"))]) {
    stop("Duplicate current TREE locations in ", state_name)
  }
  tree_locations[[state_name]] <- tree[, .(
    T2_PLT_CN = PLT_CN,
    current_TRE_CN = CN,
    death_CONDID = CONDID,
    death_SUBP = SUBP
  )]
}
tree_locations <- rbindlist(tree_locations, fill = TRUE)
deaths <- merge(
  deaths,
  tree_locations,
  by = c("T2_PLT_CN", "current_TRE_CN"),
  all.x = TRUE,
  sort = FALSE
)

# Flag whether a death shares a subplot with observed seedlings at either endpoint.
final_subplots <- unique(endpoint_subplots[
  endpoint == "final" & !is.na(SUBP),
  .(history_id, death_SUBP = SUBP, in_final_seedling_subplot = TRUE)
])
either_subplots <- unique(endpoint_subplots[
  !is.na(SUBP),
  .(history_id, death_SUBP = SUBP, in_either_seedling_subplot = TRUE)
])
deaths <- merge(
  deaths, final_subplots,
  by = c("history_id", "death_SUBP"), all.x = TRUE, sort = FALSE
)
deaths <- merge(
  deaths, either_subplots,
  by = c("history_id", "death_SUBP"), all.x = TRUE, sort = FALSE
)
deaths[, `:=`(
  in_final_seedling_subplot = in_final_seedling_subplot %in% TRUE,
  in_either_seedling_subplot = in_either_seedling_subplot %in% TRUE
)]

agents <- c("fire", "insect", "disease")
positive_histories <- rbindlist(lapply(agents, function(agent) {
  column <- paste0(agent, "_cumulative_mortality_pct")
  model_histories[get(column) > 0, .(
    agent_family = agent,
    positive_model_histories = uniqueN(history_id)
  )]
}))
death_alignment <- deaths[agent_family %chin% agents, .(
  histories_with_death_records = uniqueN(history_id),
  death_records = .N,
  death_records_with_subplot = sum(!is.na(death_SUBP)),
  death_records_in_final_seedling_subplot = sum(in_final_seedling_subplot),
  death_records_in_either_seedling_subplot = sum(in_either_seedling_subplot),
  death_abundance = sum(T1_adjusted_weight, na.rm = TRUE),
  death_abundance_in_final_seedling_subplot = sum(
    T1_adjusted_weight[in_final_seedling_subplot], na.rm = TRUE
  ),
  death_abundance_in_either_seedling_subplot = sum(
    T1_adjusted_weight[in_either_seedling_subplot], na.rm = TRUE
  )
), by = agent_family]
death_alignment <- merge(
  positive_histories,
  death_alignment,
  by = "agent_family",
  all.x = TRUE,
  sort = FALSE
)
death_alignment[, `:=`(
  fraction_death_records_in_final_seedling_subplot =
    death_records_in_final_seedling_subplot / death_records_with_subplot,
  fraction_death_records_in_either_seedling_subplot =
    death_records_in_either_seedling_subplot / death_records_with_subplot,
  fraction_death_abundance_in_final_seedling_subplot =
    death_abundance_in_final_seedling_subplot / death_abundance,
  fraction_death_abundance_in_either_seedling_subplot =
    death_abundance_in_either_seedling_subplot / death_abundance
)]
fwrite(
  death_alignment,
  file.path(output_dir, "seedling_death_subplot_alignment.csv")
)

# Keep the basic assignment and linkage checks in one compact metric table.
assignment_audit <- data.table(
  metric = c(
    "complete_case_seedling_model_histories",
    "seedling_species_rows",
    "seedling_rows_missing_condition",
    "seedling_rows_missing_subplot",
    "seedling_rows_with_unexpected_subplot",
    "duplicate_seedling_condition_subplot_species_keys",
    "model_endpoint_rows_without_positive_seedlings",
    "model_endpoint_seedling_rows_with_unexpected_subplot",
    "verified_deaths_in_seedling_model_histories",
    "verified_deaths_without_current_subplot"
  ),
  value = c(
    nrow(model_histories),
    nrow(seedlings),
    sum(is.na(seedlings$CONDID)),
    sum(is.na(seedlings$SUBP)),
    sum(!is.na(seedlings$SUBP) & !seedlings$SUBP %in% 1:4),
    duplicate_seedling_keys,
    endpoint_counts[n_seedling_subplots == 0, .N],
    endpoint_subplots[!is.na(SUBP) & !SUBP %in% 1:4, .N],
    nrow(deaths),
    deaths[is.na(death_SUBP), .N]
  )
)
fwrite(
  assignment_audit,
  file.path(output_dir, "seedling_condition_assignment_audit.csv")
)

message("Seedling spatial-support QA: ", output_dir)
