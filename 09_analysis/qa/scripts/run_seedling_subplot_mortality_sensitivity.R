#!/usr/bin/env Rscript

# Refit seedling models after restricting the mortality risk set to numbered
# subplots that contain seedlings at the first or final history endpoint.

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
mortality_path <- here(
  "09_analysis", "data", "processed", "history_cumulative_mortality.parquet"
)
edge_path <- here(
  "09_analysis", "data", "intermediate", "complete_history_edges.parquet"
)
death_path <- here(
  "09_analysis", "data", "intermediate", "interval_verified_deaths.parquet"
)
seedling_path <- here(
  "05_fia", "data", "processed", "summaries",
  "plot_seedling_species.parquet"
)
raw_tree_dir <- here("05_fia", "data", "raw")
output_dir <- here(
  "09_analysis", "qa", "outputs", "seedling_subplot_mortality_sensitivity"
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

required_inputs <- c(
  model_path, mortality_path, edge_path, death_path, seedling_path
)
missing_inputs <- required_inputs[!file.exists(required_inputs)]
if (length(missing_inputs)) {
  stop("Missing input(s): ", paste(missing_inputs, collapse = "; "))
}

# Normalize FIA control numbers before joining raw tree records.
as_id <- function(x) {
  value <- as.character(x)
  value[value %chin% c("", "NA")] <- NA_character_
  value
}

# Select the condition proportion for the sampling element that observed a tree.
prop_for_tree <- function(diameter, tpa, micro, subplot, macro, generic) {
  specific <- fcase(
    !is.na(diameter) & diameter >= 1 & diameter < 5, micro,
    !is.na(tpa) & abs(tpa - 0.999188) < 0.02, macro,
    !is.na(diameter) & diameter >= 5, subplot,
    default = NA_real_
  )
  fifelse(!is.na(specific) & specific > 0, specific, generic)
}

agents <- c("fire", "insect", "disease")
mortality_terms <- paste0(agents, "_cumulative_mortality_pct")
other_predictors <- c("cumulative_site_CWD_mm", "full_survey_period_years")
predictors <- c(mortality_terms, other_predictors)
responses <- c("temperature", "precipitation", "CWD")

# Use the exact complete-case cohort from the three seedling models.
model <- as.data.table(read_parquet(model_path))
model_histories <- model[
  layer == "seedlings" & cumulative_site_CWD_complete %in% TRUE &
    complete.cases(model[, c(
      predictors, paste0("delta_", responses)
    ), with = FALSE])
]
model_histories <- unique(model_histories, by = "history_id")
for (column in c("first_PLT_CN", "last_PLT_CN")) {
  model_histories[, (column) := as_id(get(column))]
}

# Use the union of first- and final-endpoint subplots contributing seedlings.
seedlings <- as.data.table(read_parquet(
  seedling_path,
  col_select = c("PLT_CN", "CONDID", "SUBP", "seedlings_tpa")
))
seedlings[, PLT_CN := as_id(PLT_CN)]
positive_seedlings <- unique(seedlings[
  !is.na(seedlings_tpa) & seedlings_tpa > 0 & !is.na(SUBP),
  .(PLT_CN, CONDID, SUBP)
])
endpoints <- rbindlist(list(
  model_histories[, .(
    history_id, state, CONDID, endpoint = "first", PLT_CN = first_PLT_CN
  )],
  model_histories[, .(
    history_id, state, CONDID, endpoint = "final", PLT_CN = last_PLT_CN
  )]
))
endpoint_subplots <- merge(
  endpoints,
  positive_seedlings,
  by = c("PLT_CN", "CONDID"),
  all = FALSE,
  allow.cartesian = TRUE,
  sort = FALSE
)
allowed_subplots <- unique(endpoint_subplots[, .(history_id, SUBP)])
allowed_summary <- allowed_subplots[, .(
  allowed_subplot_count = uniqueN(SUBP)
), by = history_id]

# Reconstruct the production cumulative risk set while retaining entry subplot.
edges <- as.data.table(read_parquet(edge_path))
for (column in c("PREV_PLT_CN", "T2_PLT_CN")) {
  edges[, (column) := as_id(get(column))]
}
edges[, history_id := paste(remeasurement_component_id, CONDID, sep = "|")]
edges <- edges[history_id %chin% model_histories$history_id]
risk_visit_columns <- c(
  "history_id", "stable_plot_id", "remeasurement_component_id", "state",
  "CONDID", "t1_visit_number", "PREV_PLT_CN", "T1_CONDPROP_UNADJ",
  "T1_MICRPROP_UNADJ", "T1_SUBPPROP_UNADJ", "T1_MACRPROP_UNADJ"
)
risk_visits <- unique(edges[, ..risk_visit_columns])
setnames(
  risk_visits,
  c("t1_visit_number", "PREV_PLT_CN"),
  c("risk_visit_number", "PLT_CN")
)

verified_deaths <- as.data.table(read_parquet(death_path))
for (column in c(
  "PREV_PLT_CN", "T2_PLT_CN", "T1_TRE_CN", "current_TRE_CN"
)) {
  verified_deaths[, (column) := as_id(get(column))]
}

state_results <- list()
state_checks <- list()
for (state_name in sort(unique(model_histories$state))) {
  message("Seedling-subplot mortality: ", state_name)
  state_visits <- risk_visits[state == state_name]
  state_edges <- edges[state == state_name]
  state_history_ids <- unique(state_visits$history_id)
  tree_path <- file.path(
    raw_tree_dir, state_name, paste0(state_name, "_TREE.csv")
  )
  if (!file.exists(tree_path)) stop("Missing FIA TREE input: ", tree_path)

  trees <- fread(
    tree_path,
    select = c(
      "CN", "PLT_CN", "PREV_TRE_CN", "CONDID", "SUBP", "STATUSCD",
      "DIA", "TPA_UNADJ", "P2A_GRM_FLG"
    ),
    showProgress = FALSE
  )
  trees[, `:=`(
    CN = as_id(CN),
    PLT_CN = as_id(PLT_CN),
    PREV_TRE_CN = as_id(PREV_TRE_CN)
  )]
  trees <- trees[PLT_CN %chin% state_visits$PLT_CN]
  live <- trees[
    STATUSCD == 1L & !is.na(DIA) & DIA >= 1 &
      (is.na(P2A_GRM_FLG) | P2A_GRM_FLG != "Y")
  ]
  live <- merge(
    state_visits,
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
  live[, valid_weight :=
    !is.na(TPA_UNADJ) & TPA_UNADJ > 0 &
      !is.na(condition_prop_used) & condition_prop_used > 0]
  live[, candidate_entry_weight := fifelse(
    valid_weight, TPA_UNADJ / condition_prop_used, NA_real_
  )]

  if (live[, anyDuplicated(paste(
    history_id, risk_visit_number, CN, sep = "|"
  ))]) {
    stop("Duplicate live tree within a history visit in ", state_name)
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
        entry_SUBP = SUBP,
        entry_weight = candidate_entry_weight,
        entry_visit_number = risk_visit_number
      )]
    } else {
      predecessor <- if (is.null(previous_map)) {
        data.table(
          history_id = character(),
          predecessor_TRE_CN = character(),
          inherited_entry_PLT_CN = character(),
          inherited_entry_TRE_CN = character(),
          inherited_entry_SUBP = integer(),
          inherited_entry_weight = numeric(),
          inherited_entry_visit_number = integer()
        )
      } else {
        previous_map[, .(
          history_id,
          predecessor_TRE_CN = CN,
          inherited_entry_PLT_CN = entry_PLT_CN,
          inherited_entry_TRE_CN = entry_TRE_CN,
          inherited_entry_SUBP = entry_SUBP,
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
      current[, linked_within_condition_history :=
        !is.na(inherited_entry_TRE_CN)]
      current[, `:=`(
        entry_PLT_CN = fifelse(
          linked_within_condition_history, inherited_entry_PLT_CN, PLT_CN
        ),
        entry_TRE_CN = fifelse(
          linked_within_condition_history, inherited_entry_TRE_CN, CN
        ),
        entry_SUBP = fifelse(
          linked_within_condition_history, inherited_entry_SUBP, SUBP
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
        "inherited_entry_SUBP", "inherited_entry_weight",
        "inherited_entry_visit_number"
      ) := NULL]
    }
    current[, is_population_entry := !linked_within_condition_history]
    entry_parts[[as.character(visit_number)]] <- current[
      is_population_entry == TRUE
    ]
    mapped_parts[[as.character(visit_number)]] <- current
    previous_map <- current[, .(
      history_id, CN, entry_PLT_CN, entry_TRE_CN, entry_SUBP,
      entry_weight, entry_visit_number
    )]
  }

  mapped <- rbindlist(mapped_parts, fill = TRUE)
  entries <- unique(
    rbindlist(entry_parts, fill = TRUE),
    by = c("history_id", "entry_PLT_CN", "entry_TRE_CN")
  )
  state_allowed <- allowed_subplots[history_id %chin% state_history_ids]
  aligned_entries <- merge(
    entries,
    state_allowed,
    by.x = c("history_id", "entry_SUBP"),
    by.y = c("history_id", "SUBP"),
    all = FALSE,
    sort = FALSE
  )
  full_denominator <- entries[, .(
    reconstructed_full_denominator = sum(entry_weight)
  ), by = history_id]
  aligned_denominator <- aligned_entries[, .(
    aligned_population_records = .N,
    aligned_population_abundance = sum(entry_weight)
  ), by = history_id]

  deaths <- verified_deaths[state == state_name, .(
    stable_condition_interval_key, PREV_PLT_CN, T1_TRE_CN,
    current_TRE_CN, agent_family
  )]
  deaths <- merge(
    deaths,
    state_edges[, .(stable_condition_interval_key, history_id)],
    by = "stable_condition_interval_key",
    all = FALSE,
    sort = FALSE
  )
  deaths <- merge(
    deaths,
    mapped[, .(
      history_id, PREV_PLT_CN = PLT_CN, T1_TRE_CN = CN,
      entry_SUBP, entry_weight
    )],
    by = c("history_id", "PREV_PLT_CN", "T1_TRE_CN"),
    all.x = TRUE,
    sort = FALSE
  )
  deaths <- unique(
    deaths,
    by = c("history_id", "current_TRE_CN", "agent_family")
  )
  deaths <- deaths[agent_family %chin% agents & !is.na(entry_weight)]
  aligned_deaths <- merge(
    deaths,
    state_allowed,
    by.x = c("history_id", "entry_SUBP"),
    by.y = c("history_id", "SUBP"),
    all = FALSE,
    sort = FALSE
  )
  full_death <- deaths[, .(
    reconstructed_full_death_abundance = sum(entry_weight)
  ), by = .(history_id, agent_family)]
  aligned_death <- aligned_deaths[, .(
    aligned_death_records = .N,
    aligned_death_abundance = sum(entry_weight)
  ), by = .(history_id, agent_family)]
  aligned_death <- dcast(
    aligned_death,
    history_id ~ agent_family,
    value.var = c("aligned_death_records", "aligned_death_abundance"),
    fill = 0
  )

  state_output <- merge(
    full_denominator,
    aligned_denominator,
    by = "history_id",
    all.x = TRUE,
    sort = FALSE
  )
  state_output <- merge(
    state_output,
    aligned_death,
    by = "history_id",
    all.x = TRUE,
    sort = FALSE
  )
  for (agent in agents) {
    record_column <- paste0("aligned_death_records_", agent)
    abundance_column <- paste0("aligned_death_abundance_", agent)
    if (!record_column %in% names(state_output)) {
      state_output[, (record_column) := 0]
    }
    if (!abundance_column %in% names(state_output)) {
      state_output[, (abundance_column) := 0]
    }
    set(state_output, which(is.na(state_output[[record_column]])), record_column, 0)
    set(
      state_output,
      which(is.na(state_output[[abundance_column]])),
      abundance_column,
      0
    )
    state_output[, (paste0(agent, "_subplot_mortality_pct")) :=
      100 * get(abundance_column) / aligned_population_abundance]
  }
  state_output[, state := state_name]
  state_results[[state_name]] <- state_output

  state_checks[[state_name]] <- rbindlist(list(
    full_denominator[, .(
      state = state_name,
      component = "population_abundance",
      history_id,
      agent_family = NA_character_,
      reconstructed = reconstructed_full_denominator
    )],
    full_death[, .(
      state = state_name,
      component = "death_abundance",
      history_id,
      agent_family,
      reconstructed = reconstructed_full_death_abundance
    )]
  ), fill = TRUE)
}

subplot_mortality <- rbindlist(state_results, fill = TRUE)
reconstruction <- rbindlist(state_checks, fill = TRUE)
subplot_mortality <- merge(
  subplot_mortality,
  allowed_summary,
  by = "history_id",
  all.x = TRUE,
  sort = FALSE
)

# Confirm that the duplicated reconstruction exactly matches production totals.
production <- as.data.table(read_parquet(mortality_path))
production_denominator <- production[, .(
  history_id,
  production = cumulative_population_abundance
)]
denominator_check <- merge(
  reconstruction[component == "population_abundance"],
  production_denominator,
  by = "history_id",
  all.x = TRUE,
  sort = FALSE
)
production_deaths <- melt(
  production,
  id.vars = "history_id",
  measure.vars = paste0(agents, "_death_abundance"),
  variable.name = "agent_family",
  value.name = "production"
)
production_deaths[, agent_family := sub(
  "_death_abundance$", "", agent_family
)]
death_check <- merge(
  reconstruction[component == "death_abundance"],
  production_deaths,
  by = c("history_id", "agent_family"),
  all.x = TRUE,
  sort = FALSE
)
death_check[is.na(production), production := 0]
reconstruction_check <- rbindlist(list(
  denominator_check[, .(
    component = "population_abundance",
    records = .N,
    maximum_absolute_difference = max(abs(reconstructed - production))
  )],
  death_check[, .(
    component = paste0(agent_family, "_death_abundance"),
    records = .N,
    maximum_absolute_difference = max(abs(reconstructed - production))
  ), by = agent_family][, agent_family := NULL]
), fill = TRUE)
fwrite(
  reconstruction_check,
  file.path(output_dir, "production_reconstruction_check.csv")
)
if (reconstruction_check[, any(maximum_absolute_difference > 1e-8)]) {
  stop("Subplot sensitivity does not reproduce production mortality totals")
}

# Compare the condition-wide and spatially restricted mortality quantities.
comparison <- merge(
  model_histories[, c(
    "history_id", "stable_plot_id", mortality_terms,
    paste0("delta_", responses), other_predictors
  ), with = FALSE],
  subplot_mortality,
  by = "history_id",
  all.x = TRUE,
  sort = FALSE
)
comparison[, aligned_denominator_fraction :=
  aligned_population_abundance / reconstructed_full_denominator]

comparison_summary <- rbindlist(lapply(agents, function(agent) {
  condition_column <- paste0(agent, "_cumulative_mortality_pct")
  subplot_column <- paste0(agent, "_subplot_mortality_pct")
  usable <- comparison[
    !is.na(get(condition_column)) & !is.na(get(subplot_column))
  ]
  data.table(
    agent_family = agent,
    total_histories = nrow(comparison),
    usable_aligned_histories = nrow(usable),
    histories_lost = nrow(comparison) - nrow(usable),
    conditionwide_positive_histories = usable[get(condition_column) > 0, .N],
    subplot_positive_histories = usable[get(subplot_column) > 0, .N],
    correlation = cor(usable[[condition_column]], usable[[subplot_column]]),
    median_absolute_difference_pct = median(abs(
      usable[[subplot_column]] - usable[[condition_column]]
    )),
    p95_absolute_difference_pct = quantile(abs(
      usable[[subplot_column]] - usable[[condition_column]]
    ), 0.95, names = FALSE),
    maximum_absolute_difference_pct = max(abs(
      usable[[subplot_column]] - usable[[condition_column]]
    )),
    median_aligned_denominator_fraction = median(
      usable$aligned_denominator_fraction
    )
  )
}))
fwrite(
  comparison_summary,
  file.path(output_dir, "seedling_subplot_mortality_summary.csv")
)

# Fit condition-wide and aligned models on the identical usable histories.
# Return clustered coefficients and fit statistics for one mortality scenario.
fit_scenario <- function(data, response, scenario, aligned = FALSE) {
  fit_data <- copy(data)
  if (aligned) {
    for (agent in agents) {
      fit_data[, (paste0(agent, "_cumulative_mortality_pct")) :=
        get(paste0(agent, "_subplot_mortality_pct"))]
    }
  }
  fit_data <- fit_data[complete.cases(
    fit_data[, c(predictors, paste0("delta_", response)), with = FALSE]
  )]
  fit <- lm(
    reformulate(predictors, response = paste0("delta_", response)),
    data = fit_data
  )
  covariance <- sandwich::vcovCL(
    fit, cluster = fit_data$stable_plot_id, type = "HC1"
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
    stable_plots = uniqueN(fit_data$stable_plot_id)
  )
}

usable_comparison <- comparison[!is.na(aligned_population_abundance)]
coefficients <- rbindlist(lapply(responses, function(response) {
  rbindlist(list(
    fit_scenario(
      usable_comparison, response, "condition_wide", aligned = FALSE
    ),
    fit_scenario(
      usable_comparison, response, "seedling_subplot_union", aligned = TRUE
    )
  ))
}))
fwrite(
  coefficients,
  file.path(output_dir, "seedling_subplot_model_coefficients.csv")
)

message("Seedling subplot mortality sensitivity: ", output_dir)
