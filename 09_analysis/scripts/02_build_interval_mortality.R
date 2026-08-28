#!/usr/bin/env Rscript

# Build verified agent mortality for each eligible official PREV interval.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fs)
  library(here)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("09_analysis/scripts/utils/mortality.R"))

cfg <- load_config()
raw_dir <- here(cfg$raw$fia$local_dir)
analysis_dir <- here("09_analysis/data/processed")
intermediate_dir <- here("09_analysis/data/intermediate")
qa_dir <- here("09_analysis/qa/outputs/02_interval_mortality")
dir_create(c(analysis_dir, intermediate_dir, qa_dir))

agents <- c("fire", "insect", "disease")

as_id <- function(x) {
  value <- as.character(x)
  value[value %chin% c("", "NA")] <- NA_character_
  value
}

sum_or_zero <- function(x) sum(x, na.rm = TRUE)

annual_rate <- function(numerator, denominator, years) {
  value <- rep(NA_real_, length(numerator))
  usable <- !is.na(numerator) & numerator >= 0 &
    !is.na(denominator) & denominator > 0 &
    !is.na(years) & years > 0
  value[usable] <- 100 * numerator[usable] / denominator[usable] / years[usable]
  value
}

condition_prop_for_tree <- function(
    element,
    microplot_prop,
    subplot_prop,
    macroplot_prop,
    generic_prop) {
  specific <- fcase(
    element == "microplot", microplot_prop,
    element == "subplot", subplot_prop,
    element == "macroplot", macroplot_prop,
    default = NA_real_
  )
  fifelse(!is.na(specific) & specific > 0, specific, generic_prop)
}

interval_path <- file.path(analysis_dir, "stable_condition_intervals.parquet")
if (!file.exists(interval_path)) {
  stop("Missing stable condition intervals. Run script 01 first: ", interval_path)
}
intervals <- as.data.table(read_parquet(interval_path))
for (column in c("PREV_PLT_CN", "T2_PLT_CN")) {
  intervals[, (column) := as_id(get(column))]
}
if (intervals[, anyDuplicated(stable_condition_interval_key)]) {
  stop("Stable-condition interval keys are not unique")
}

process_state <- function(state_name, state_intervals) {
  message("Interval mortality: ", state_name)
  tree_path <- file.path(raw_dir, state_name, paste0(state_name, "_TREE.csv"))
  grm_path <- file.path(
    raw_dir, state_name, paste0(state_name, "_TREE_GRM_COMPONENT.csv")
  )
  missing_inputs <- c(tree_path, grm_path)[!file.exists(c(tree_path, grm_path))]
  if (length(missing_inputs)) {
    stop("Missing FIA mortality inputs for ", state_name, ": ",
         paste(missing_inputs, collapse = "; "))
  }

  tree <- fread(
    tree_path,
    select = c(
      "CN", "PLT_CN", "PREV_TRE_CN", "CONDID", "STATUSCD", "DIA",
      "P2A_GRM_FLG", "TPA_UNADJ", "AGENTCD"
    ),
    showProgress = FALSE
  )
  tree[, c("CN", "PLT_CN", "PREV_TRE_CN") := lapply(
    .SD, as_id
  ), .SDcols = c("CN", "PLT_CN", "PREV_TRE_CN")]
  visits <- unique(c(state_intervals$PREV_PLT_CN, state_intervals$T2_PLT_CN))
  tree <- tree[PLT_CN %chin% visits]
  if (tree[, anyDuplicated(paste(PLT_CN, CN, sep = "|"))]) {
    stop("Duplicate TREE control numbers within a visit in ", state_name)
  }

  # T1 defines condition membership and abundance. P2A rows are excluded.
  live_t1 <- tree[
    PLT_CN %chin% state_intervals$PREV_PLT_CN &
      STATUSCD == 1L & !is.na(DIA) & DIA >= 1 &
      (is.na(P2A_GRM_FLG) | P2A_GRM_FLG != "Y"),
    .(
      T1_TRE_CN = CN,
      PREV_PLT_CN = PLT_CN,
      CONDID,
      T1_DIA = DIA,
      T1_TPA_UNADJ = TPA_UNADJ
    )
  ]
  risk <- merge(
    state_intervals,
    live_t1,
    by = c("PREV_PLT_CN", "CONDID"),
    all = FALSE,
    allow.cartesian = TRUE,
    sort = FALSE
  )
  risk[, T1_sampling_element := fia_sampling_element(T1_DIA, T1_TPA_UNADJ)]
  risk[, T1_condition_prop_used := condition_prop_for_tree(
    T1_sampling_element,
    T1_MICRPROP_UNADJ,
    T1_SUBPPROP_UNADJ,
    T1_MACRPROP_UNADJ,
    T1_CONDPROP_UNADJ
  )]
  risk[, T1_weight_valid :=
    !is.na(T1_TPA_UNADJ) & T1_TPA_UNADJ > 0 &
      !is.na(T1_condition_prop_used) & T1_condition_prop_used > 0]
  risk[, T1_adjusted_weight := fifelse(
    T1_weight_valid,
    T1_TPA_UNADJ / T1_condition_prop_used,
    NA_real_
  )]
  risk[, risk_tree_key := paste(
    stable_condition_interval_key, T1_TRE_CN, sep = "|"
  )]
  if (risk[, anyDuplicated(risk_tree_key)]) {
    stop("Duplicate T1 risk-set tree keys in ", state_name)
  }

  # T2 and GRM supply the tree outcome and cause of death.
  grm <- fread(
    grm_path,
    select = c("TRE_CN", "PLT_CN", "MICR_COMPONENT_AL_FOREST"),
    showProgress = FALSE
  )
  grm[, c("TRE_CN", "PLT_CN") := lapply(
    .SD, as_id
  ), .SDcols = c("TRE_CN", "PLT_CN")]
  grm <- grm[PLT_CN %chin% state_intervals$T2_PLT_CN]
  grm <- grm[, .(
    is_mortality = any(fia_is_verified_interval_death(
      MICR_COMPONENT_AL_FOREST
    )),
    is_removal = any(MICR_COMPONENT_AL_FOREST %chin% c("CUT1", "CUT2"))
  ), by = .(current_TRE_CN = TRE_CN, T2_PLT_CN = PLT_CN)]

  current <- tree[PLT_CN %chin% state_intervals$T2_PLT_CN, .(
    current_TRE_CN = CN,
    T2_PLT_CN = PLT_CN,
    current_PREV_TRE_CN = PREV_TRE_CN,
    current_CONDID = CONDID,
    current_STATUSCD = STATUSCD,
    current_AGENTCD = AGENTCD
  )]
  current <- merge(
    current, grm,
    by = c("current_TRE_CN", "T2_PLT_CN"),
    all.x = TRUE,
    sort = FALSE
  )
  for (column in c("is_mortality", "is_removal")) {
    set(current, which(is.na(current[[column]])), column, FALSE)
  }

  linked <- merge(
    risk[, .(
      risk_tree_key, stable_condition_interval_key, T2_PLT_CN, T1_TRE_CN
    )],
    current,
    by.x = c("T2_PLT_CN", "T1_TRE_CN"),
    by.y = c("T2_PLT_CN", "current_PREV_TRE_CN"),
    all = FALSE,
    allow.cartesian = TRUE,
    sort = FALSE
  )
  link_summary <- linked[, {
    one_link <- uniqueN(current_TRE_CN) == 1L
    .(
      current_link_count = uniqueN(current_TRE_CN),
      current_TRE_CN = if (one_link) current_TRE_CN[[1L]] else NA_character_,
      current_CONDID = if (one_link) current_CONDID[[1L]] else NA_integer_,
      current_STATUSCD = if (one_link) current_STATUSCD[[1L]] else NA_integer_,
      current_AGENTCD = if (one_link) current_AGENTCD[[1L]] else NA_integer_,
      is_mortality = one_link && isTRUE(is_mortality[[1L]]),
      is_removal = one_link && isTRUE(is_removal[[1L]])
    )
  }, by = risk_tree_key]
  risk <- merge(risk, link_summary, by = "risk_tree_key", all.x = TRUE)
  risk[is.na(current_link_count), current_link_count := 0L]
  for (column in c("is_mortality", "is_removal")) {
    set(risk, which(is.na(risk[[column]])), column, FALSE)
  }

  risk[, agent_family := fia_agent_family(current_AGENTCD)]
  risk[, interval_outcome := fcase(
    current_link_count == 0L, "unmatched",
    current_link_count > 1L, "ambiguous link",
    is_mortality & is_removal, "ambiguous death and removal",
    is_mortality, "verified death",
    is_removal, "removal",
    current_STATUSCD == 1L, "survived",
    default = "unresolved"
  )]

  interval_summary <- risk[, .(
    T1_population_records = .N,
    T1_population_abundance = sum_or_zero(T1_adjusted_weight),
    T1_missing_weight_records = sum(!T1_weight_valid),
    T1_microplot_records = sum(T1_sampling_element == "microplot"),
    T1_subplot_records = sum(T1_sampling_element == "subplot"),
    T1_macroplot_records = sum(T1_sampling_element == "macroplot"),
    survived_records = sum(interval_outcome == "survived"),
    verified_death_records = sum(interval_outcome == "verified death"),
    removal_records = sum(interval_outcome == "removal"),
    unresolved_records = sum(interval_outcome %chin% c(
      "unmatched", "ambiguous link", "ambiguous death and removal", "unresolved"
    ))
  ), by = stable_condition_interval_key]
  interval_output <- merge(
    state_intervals,
    interval_summary,
    by = "stable_condition_interval_key",
    all.x = TRUE,
    sort = FALSE
  )
  count_columns <- setdiff(names(interval_summary), "stable_condition_interval_key")
  for (column in count_columns) {
    set(interval_output, which(is.na(interval_output[[column]])), column, 0)
  }

  for (agent in agents) {
    agent_summary <- risk[
      interval_outcome == "verified death" & agent_family == agent,
      .(
        death_records = .N,
        death_abundance = sum_or_zero(T1_adjusted_weight)
      ),
      by = stable_condition_interval_key
    ]
    setnames(
      agent_summary,
      c("death_records", "death_abundance"),
      c(paste0(agent, "_death_records"), paste0(agent, "_death_abundance"))
    )
    interval_output <- merge(
      interval_output,
      agent_summary,
      by = "stable_condition_interval_key",
      all.x = TRUE,
      sort = FALSE
    )
    for (column in setdiff(names(agent_summary), "stable_condition_interval_key")) {
      set(interval_output, which(is.na(interval_output[[column]])), column, 0)
    }
    interval_output[, (paste0(agent, "_annual_mortality_pct")) := annual_rate(
      get(paste0(agent, "_death_abundance")),
      T1_population_abundance,
      interval_years
    )]
  }

  interval_output[, analysis_ready :=
    T1_population_records > 0 & T1_population_abundance > 0 &
      T1_missing_weight_records == 0 & unresolved_records == 0]
  for (agent in agents) {
    set(
      interval_output,
      which(!interval_output$analysis_ready),
      paste0(agent, "_annual_mortality_pct"),
      NA_real_
    )
  }

  verified_deaths <- risk[
    interval_outcome == "verified death" & agent_family %chin% agents,
    .(
      state = state_name,
      stable_condition_interval_key,
      PREV_PLT_CN,
      T2_PLT_CN,
      CONDID,
      T1_TRE_CN,
      current_TRE_CN,
      current_CONDID,
      T1_adjusted_weight,
      current_AGENTCD,
      agent_family
    )
  ]

  list(intervals = interval_output, deaths = verified_deaths)
}

state_names <- sort(unique(intervals$state))
state_results <- lapply(state_names, function(state_name) {
  process_state(state_name, intervals[state == state_name])
})

interval_mortality <- rbindlist(
  lapply(state_results, `[[`, "intervals"),
  fill = TRUE
)
verified_deaths <- rbindlist(
  lapply(state_results, `[[`, "deaths"),
  fill = TRUE
)

if (nrow(interval_mortality) != nrow(intervals)) {
  stop("Interval mortality output lost stable-condition intervals")
}
if (interval_mortality[, anyDuplicated(stable_condition_interval_key)]) {
  stop("Interval mortality keys are not unique")
}
for (agent in agents) {
  if (interval_mortality[, any(
    get(paste0(agent, "_death_abundance")) - T1_population_abundance > 1e-8,
    na.rm = TRUE
  )]) {
    stop(agent, " mortality numerator exceeds the T1 denominator")
  }
}

output_columns <- c(
  "stable_condition_interval_key", "stable_plot_id",
  "remeasurement_component_id", "state", "CONDID", "PREV_PLT_CN",
  "T2_PLT_CN", "t1_visit_number", "t2_visit_number",
  "n_visits_in_component", "T1_INVYR", "T2_INVYR",
  "T1_measurement_date", "T2_measurement_date", "interval_years",
  "T1_CONDPROP_UNADJ", "T1_MICRPROP_UNADJ", "T1_SUBPPROP_UNADJ",
  "T1_MACRPROP_UNADJ", "T2_CONDPROP_UNADJ", "T2_MICRPROP_UNADJ",
  "T2_SUBPPROP_UNADJ", "T2_MACRPROP_UNADJ",
  "T1_population_records", "T1_population_abundance",
  "T1_microplot_records", "T1_subplot_records", "T1_macroplot_records",
  "survived_records", "verified_death_records", "removal_records",
  "unresolved_records", "analysis_ready",
  unlist(lapply(agents, function(agent) {
    c(
      paste0(agent, "_death_records"),
      paste0(agent, "_death_abundance"),
      paste0(agent, "_annual_mortality_pct")
    )
  }))
)
write_parquet_atomic(
  interval_mortality[, ..output_columns],
  file.path(analysis_dir, "interval_agent_mortality.parquet")
)
write_parquet_atomic(
  verified_deaths,
  file.path(intermediate_dir, "interval_verified_deaths.parquet")
)

flow <- data.table(
  stage = c(
    "eligible stable-condition intervals",
    "positive T1 population",
    "complete tree outcomes",
    "analysis-ready intervals"
  ),
  intervals = c(
    nrow(interval_mortality),
    sum(interval_mortality$T1_population_abundance > 0),
    sum(interval_mortality$unresolved_records == 0),
    sum(interval_mortality$analysis_ready)
  )
)
flow[, lost_from_previous := c(
  NA_integer_, head(intervals, -1L) - tail(intervals, -1L)
)]
fwrite(flow, file.path(qa_dir, "interval_flow.csv"))

agent_summary <- rbindlist(lapply(agents, function(agent) {
  value <- interval_mortality[
    analysis_ready %in% TRUE,
    get(paste0(agent, "_annual_mortality_pct"))
  ]
  positive <- value[!is.na(value) & value > 0]
  data.table(
    agent,
    intervals = sum(!is.na(value)),
    zero_intervals = sum(value == 0, na.rm = TRUE),
    positive_intervals = length(positive),
    median_positive = if (length(positive)) median(positive) else NA_real_,
    q99 = quantile(value, 0.99, na.rm = TRUE),
    maximum = max(value, na.rm = TRUE)
  )
}))
fwrite(agent_summary, file.path(qa_dir, "annual_mortality_summary.csv"))

message("Built verified T1-risk-set interval mortality")
