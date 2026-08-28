#!/usr/bin/env Rscript

# Build eligible stable-condition intervals and condition-level CWM changes.

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
fia_summary_dir <- here(cfg$processed$fia$summaries$output_dir)
niche_dir <- here(cfg$processed$species_niches$output_dir)
analysis_dir <- here("09_analysis/data/processed")
qa_dir <- here("09_analysis/qa/cwm")
dir_create(c(analysis_dir, qa_dir))

condition_cutoff <- cfg$processed$analysis$condition_minimum_proportion
condition_keys <- c("PLT_CN", "INVYR", "CONDID")
metric_map <- c(
  temperature = "tmean_annual_mean",
  precipitation = "pr_annual_sum",
  CWD = "cwd_annual_sum"
)

weighted_mean_or_na <- function(value, weight) {
  usable <- !is.na(value) & !is.na(weight) & weight > 0
  if (!any(usable)) return(NA_real_)
  weighted.mean(value[usable], weight[usable])
}

# Load one niche value per FIA species.
study_niches <- as.data.table(read_parquet(
  file.path(niche_dir, "species_climate_niches_us_study_area.parquet"),
  col_select = c("species_key", unname(metric_map))
))
global_niches <- as.data.table(read_parquet(
  file.path(
    niche_dir,
    cfg$processed$species_niches$files$species_climate_niches
  ),
  col_select = c("species_key", unname(metric_map))
))
niches <- unique(rbindlist(list(
  study_niches,
  global_niches[!species_key %in% study_niches$species_key]
)), by = "species_key")

# Build individual-abundance CWMs for each life stage and condition visit.
layer_specs <- list(
  trees = list(
    file = "plot_tree_species.parquet",
    raw = "n_trees_raw",
    abundance = "n_trees_tpa"
  ),
  saplings = list(
    file = "plot_sapling_species.parquet",
    raw = "n_trees_raw",
    abundance = "n_trees_tpa"
  ),
  seedlings = list(
    file = "plot_seedling_species.parquet",
    raw = "n_seedling_records",
    abundance = "seedlings_tpa"
  )
)

condition_cwm_parts <- list()
method_parts <- list()

for (layer_name in names(layer_specs)) {
  spec <- layer_specs[[layer_name]]
  message("Condition CWM: ", layer_name)

  community <- as.data.table(read_parquet(
    file.path(fia_summary_dir, spec$file),
    col_select = c(
      condition_keys, "SPCD", "state", "forest_type_group",
      "COND_STATUS_CD", "CONDPROP_UNADJ", "is_forested_condition",
      spec$raw, spec$abundance
    )
  ))
  community <- community[
    COND_STATUS_CD == 1L & is_forested_condition %in% TRUE &
      !is.na(CONDPROP_UNADJ) & CONDPROP_UNADJ >= condition_cutoff
  ]
  community[, species_key := paste0("fia_spcd:", as.integer(SPCD))]

  species <- community[, .(
    raw_weight = sum(get(spec$raw), na.rm = TRUE),
    abundance_weight = sum(get(spec$abundance), na.rm = TRUE),
    state = first(state),
    forest_type_group = first(forest_type_group),
    CONDPROP_UNADJ = first(CONDPROP_UNADJ)
  ), by = c(condition_keys, "species_key")]
  joined <- merge(species, niches, by = "species_key", all.x = TRUE, sort = FALSE)

  condition_cwm <- joined[, .(
    state = first(state),
    forest_type_group = first(forest_type_group),
    CONDPROP_UNADJ = first(CONDPROP_UNADJ),
    n_species = uniqueN(species_key[abundance_weight > 0]),
    total_individual_abundance = sum(abundance_weight, na.rm = TRUE),
    temperature_niche_abundance = sum(
      abundance_weight[!is.na(tmean_annual_mean)], na.rm = TRUE
    ),
    precipitation_niche_abundance = sum(
      abundance_weight[!is.na(pr_annual_sum)], na.rm = TRUE
    ),
    CWD_niche_abundance = sum(
      abundance_weight[!is.na(cwd_annual_sum)], na.rm = TRUE
    ),
    temperature = weighted_mean_or_na(tmean_annual_mean, abundance_weight),
    precipitation = weighted_mean_or_na(pr_annual_sum, abundance_weight),
    CWD = weighted_mean_or_na(cwd_annual_sum, abundance_weight)
  ), by = condition_keys]
  condition_cwm[, layer := layer_name]
  condition_cwm_parts[[layer_name]] <- condition_cwm

  method_parts[[layer_name]] <- data.table(
    layer = layer_name,
    active_weight = spec$abundance,
    grain = "condition visit"
  )
}

condition_cwm <- rbindlist(condition_cwm_parts, fill = TRUE)
setcolorder(condition_cwm, c(
  condition_keys, "state", "forest_type_group", "CONDPROP_UNADJ", "layer",
  setdiff(names(condition_cwm), c(
    condition_keys, "state", "forest_type_group", "CONDPROP_UNADJ", "layer"
  ))
))
write_parquet_atomic(
  condition_cwm,
  file.path(analysis_dir, "condition_visit_cwm.parquet")
)
fwrite(rbindlist(method_parts), file.path(qa_dir, "weighting_methods.csv"))

# Build official PREV intervals and match the same numeric CONDID.
components <- as.data.table(read_parquet(
  file.path(analysis_dir, "fia_remeasurement_components.parquet"),
  col_select = c(
    "PLT_CN", "PREV_PLT_CN", "INVYR", "state", "stable_plot_id",
    "remeasurement_component_id", "visit_number_in_component",
    "n_visits_in_component", "connectivity_edge_valid",
    "measurement_date_lower", "measurement_date_upper"
  )
))
components[, measurement_date := fia_mid_date(
  measurement_date_lower, measurement_date_upper
)]

previous_visits <- components[, .(
  PREV_PLT_CN = PLT_CN,
  T1_INVYR = INVYR,
  T1_measurement_date = measurement_date,
  t1_visit_number = visit_number_in_component
)]
edges <- merge(
  components[
    connectivity_edge_valid %in% TRUE & !is.na(PREV_PLT_CN),
    .(
      state, stable_plot_id, remeasurement_component_id,
      T2_PLT_CN = PLT_CN, PREV_PLT_CN, T2_INVYR = INVYR,
      T2_measurement_date = measurement_date,
      t2_visit_number = visit_number_in_component,
      n_visits_in_component
    )
  ],
  previous_visits,
  by = "PREV_PLT_CN",
  all.x = TRUE,
  sort = FALSE
)
edges[, interval_years := as.numeric(
  T2_measurement_date - T1_measurement_date
) / 365.2425]
edges <- edges[!is.na(interval_years) & interval_years > 0]

condition_metadata <- as.data.table(read_parquet(
  file.path(fia_summary_dir, "plot_condition_metadata.parquet"),
  col_select = c(
    "PLT_CN", "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ",
    "forest_type_group", "state"
  )
))

current_conditions <- merge(
  condition_metadata,
  edges,
  by.x = c("state", "PLT_CN"),
  by.y = c("state", "T2_PLT_CN"),
  all = FALSE
)
setnames(
  current_conditions,
  c("PLT_CN", "COND_STATUS_CD", "CONDPROP_UNADJ", "forest_type_group"),
  c(
    "T2_PLT_CN", "T2_COND_STATUS_CD", "T2_CONDPROP_UNADJ",
    "T2_forest_type_group"
  )
)
previous_conditions <- condition_metadata[, .(
  state,
  PREV_PLT_CN = PLT_CN,
  CONDID,
  T1_COND_STATUS_CD = COND_STATUS_CD,
  T1_CONDPROP_UNADJ = CONDPROP_UNADJ,
  T1_forest_type_group = forest_type_group
)]
intervals <- merge(
  current_conditions,
  previous_conditions,
  by = c("state", "PREV_PLT_CN", "CONDID"),
  all.x = TRUE,
  sort = FALSE
)
intervals[, eligible := fia_condition_interval_eligible(
  !is.na(T1_COND_STATUS_CD),
  T1_COND_STATUS_CD,
  T2_COND_STATUS_CD,
  T1_CONDPROP_UNADJ,
  T2_CONDPROP_UNADJ,
  condition_cutoff
)]

cohort_flow <- data.table(
  stage = c(
    "official PREV intervals",
    "current endpoint conditions",
    "same CONDID at both endpoints",
    "eligible forest conditions at both endpoints"
  ),
  n = c(
    nrow(edges),
    nrow(current_conditions),
    sum(!is.na(intervals$T1_COND_STATUS_CD)),
    sum(intervals$eligible)
  )
)
fwrite(cohort_flow, file.path(qa_dir, "condition_interval_flow.csv"))
intervals <- intervals[eligible %in% TRUE]

# Add the sampling-element condition proportions needed by mortality weights.
condition_prop_parts <- list()
for (state_name in sort(unique(intervals$state))) {
  condition_path <- file.path(raw_dir, state_name, paste0(state_name, "_COND.csv"))
  if (!file.exists(condition_path)) stop("Missing condition input: ", condition_path)
  visits <- unique(c(
    intervals[state == state_name, PREV_PLT_CN],
    intervals[state == state_name, T2_PLT_CN]
  ))
  state_props <- fread(
    condition_path,
    select = c(
      "PLT_CN", "CONDID", "MICRPROP_UNADJ", "SUBPPROP_UNADJ",
      "MACRPROP_UNADJ"
    ),
    showProgress = FALSE
  )[PLT_CN %in% visits]
  state_props[, state := state_name]
  condition_prop_parts[[state_name]] <- state_props
}
condition_props <- rbindlist(condition_prop_parts, fill = TRUE)

t1_props <- copy(condition_props)
setnames(
  t1_props,
  c("PLT_CN", "MICRPROP_UNADJ", "SUBPPROP_UNADJ", "MACRPROP_UNADJ"),
  c(
    "PREV_PLT_CN", "T1_MICRPROP_UNADJ", "T1_SUBPPROP_UNADJ",
    "T1_MACRPROP_UNADJ"
  )
)
t2_props <- copy(condition_props)
setnames(
  t2_props,
  c("PLT_CN", "MICRPROP_UNADJ", "SUBPPROP_UNADJ", "MACRPROP_UNADJ"),
  c(
    "T2_PLT_CN", "T2_MICRPROP_UNADJ", "T2_SUBPPROP_UNADJ",
    "T2_MACRPROP_UNADJ"
  )
)
intervals <- merge(
  intervals, t1_props,
  by = c("state", "PREV_PLT_CN", "CONDID"),
  all.x = TRUE
)
intervals <- merge(
  intervals, t2_props,
  by = c("state", "T2_PLT_CN", "CONDID"),
  all.x = TRUE
)
intervals[, stable_condition_interval_key := paste(
  state, PREV_PLT_CN, T2_PLT_CN, CONDID, sep = "|"
)]
if (intervals[, anyDuplicated(stable_condition_interval_key)]) {
  stop("Stable-condition interval keys are not unique")
}

interval_columns <- c(
  "stable_condition_interval_key", "stable_plot_id",
  "remeasurement_component_id", "state", "CONDID", "PREV_PLT_CN",
  "T2_PLT_CN", "t1_visit_number", "t2_visit_number",
  "n_visits_in_component", "T1_INVYR", "T2_INVYR",
  "T1_measurement_date", "T2_measurement_date", "interval_years",
  "T1_CONDPROP_UNADJ", "T1_MICRPROP_UNADJ", "T1_SUBPPROP_UNADJ",
  "T1_MACRPROP_UNADJ", "T2_CONDPROP_UNADJ", "T2_MICRPROP_UNADJ",
  "T2_SUBPPROP_UNADJ", "T2_MACRPROP_UNADJ", "T1_forest_type_group",
  "T2_forest_type_group"
)
intervals <- intervals[, ..interval_columns]
write_parquet_atomic(
  intervals,
  file.path(analysis_dir, "stable_condition_intervals.parquet")
)

# Pair the selected condition CWMs across each interval.
t1_cwm <- copy(condition_cwm)
setnames(t1_cwm, c("PLT_CN", "INVYR"), c("PREV_PLT_CN", "T1_CWM_INVYR"))
setnames(
  t1_cwm,
  c("temperature", "precipitation", "CWD"),
  c("T1_temperature", "T1_precipitation", "T1_CWD")
)
t2_cwm <- copy(condition_cwm)
setnames(t2_cwm, c("PLT_CN", "INVYR"), c("T2_PLT_CN", "T2_CWM_INVYR"))
setnames(
  t2_cwm,
  c("temperature", "precipitation", "CWD"),
  c("T2_temperature", "T2_precipitation", "T2_CWD")
)

cwm_change <- merge(
  intervals,
  t1_cwm[, .(
    PREV_PLT_CN, CONDID, layer, T1_temperature, T1_precipitation, T1_CWD
  )],
  by = c("PREV_PLT_CN", "CONDID"),
  all.x = TRUE,
  allow.cartesian = TRUE
)
cwm_change <- merge(
  cwm_change,
  t2_cwm[, .(
    T2_PLT_CN, CONDID, layer, T2_temperature, T2_precipitation, T2_CWD
  )],
  by = c("T2_PLT_CN", "CONDID", "layer"),
  all.x = TRUE
)
cwm_change <- cwm_change[!is.na(layer)]
for (metric in names(metric_map)) {
  cwm_change[, (paste0("delta_", metric)) :=
    get(paste0("T2_", metric)) - get(paste0("T1_", metric))]
}
cwm_columns <- c(
  "stable_condition_interval_key", "stable_plot_id",
  "remeasurement_component_id", "state", "PREV_PLT_CN", "T2_PLT_CN",
  "CONDID", "layer", "T1_INVYR", "T2_INVYR", "interval_years",
  unlist(lapply(names(metric_map), function(metric) {
    c(paste0("T1_", metric), paste0("T2_", metric), paste0("delta_", metric))
  }))
)
write_parquet_atomic(
  cwm_change[, ..cwm_columns],
  file.path(analysis_dir, "stable_condition_cwm_change.parquet")
)

message("Built condition histories and individual-abundance CWM changes")
