#!/usr/bin/env Rscript

# Compare each plot's earliest and latest usable forest-community visits.
# This candidate analysis design remains separate from interval-level change.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/build_freshness.R"))

config <- load_config()
fia_cfg <- config$processed$fia
therm_cfg <- config$processed$thermophilization
link_cfg <- config$processed$disturbance_linkage
therm_dir <- here(therm_cfg$output_dir)
summary_dir <- here(fia_cfg$summaries$output_dir)
link_dir <- here(link_cfg$output_dir)
qa_dir <- here("07_thermophilization/qa/outputs")

context_path <- file.path(
  summary_dir,
  fia_cfg$summaries$files$plot_visit_context
)
audit_path <- file.path(
  link_dir,
  link_cfg$files$fia_visit_pairing_audit
)
for (path in c(context_path, audit_path)) {
  if (!file.exists(path)) stop("Required visit product is missing: ", path)
}

layers <- c("seedlings", "saplings", "trees")
cwm_paths <- vapply(layers, function(layer) {
  file.path(therm_dir, therm_cfg$files[[paste0("forest_plot_visit_cwm_", layer)]])
}, character(1))
missing_cwm <- cwm_paths[!file.exists(cwm_paths)]
if (length(missing_cwm) > 0) {
  stop(
    "Forest plot-visit CWM is missing: ", paste(missing_cwm, collapse = ", "), "\n",
    "Run: Rscript 07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R"
  )
}

primary_path <- file.path(therm_dir, therm_cfg$files$forest_first_last_change)
stage_path <- file.path(
  therm_dir,
  therm_cfg$files$forest_first_last_change_by_stage
)

# --force / --force=<product> overrides the freshness check for this run.
options(build_force_rebuild = build_force_from_args())

# Both products come out of one pass, so either being stale rebuilds the pair.
freshness_inputs <- c(context_path, audit_path, unname(cwm_paths))
decisions <- list(
  build_should_rebuild(
    primary_path,
    input_paths = freshness_inputs,
    required_cols = c(
      "stable_plot_id", "first_PLT_CN", "last_PLT_CN", "n_visits_observed"
    ),
    label = "forest_first_last_change"
  ),
  build_should_rebuild(
    stage_path,
    input_paths = freshness_inputs,
    required_cols = c(
      "stable_plot_id", "life_stage", "PLT_CN_first", "PLT_CN_last"
    ),
    label = "forest_first_last_change_by_stage"
  )
)
build_log_decision(basename(primary_path), decisions[[1]])
build_log_decision(basename(stage_path), decisions[[2]])
if (!any(vapply(decisions, function(d) isTRUE(d$rebuild), logical(1)))) {
  cat("Nothing to do. Pass --force to rebuild anyway.\n")
  # Only exit under Rscript, so sourcing this file interactively is harmless.
  if (!interactive()) quit(save = "no", status = 0)
}

metric_cols <- c(
  paste0("mean_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  )),
  paste0("median_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  ))
)

# Use bounded measurement dates rather than relying only on inventory year.
context <- as.data.table(read_parquet(context_path))
context <- context[, .(
  PLT_CN,
  measurement_date_lower,
  measurement_date_upper,
  measurement_date_precision,
  measurement_date_source
)]
if (anyDuplicated(context$PLT_CN)) stop("Visit context must be unique by PLT_CN.")

# Stack life stages temporarily so endpoint selection follows one rule.
communities <- rbindlist(lapply(layers, function(layer) {
  x <- as.data.table(read_parquet(cwm_paths[[layer]]))
  x[, life_stage := layer]
  x
}), fill = TRUE, use.names = TRUE)

communities <- merge(
  communities,
  context,
  by = "PLT_CN",
  all.x = TRUE,
  sort = FALSE
)

# A missing context date makes the direction of change unknowable.
n_undated_visits <- communities[is.na(measurement_date_lower), uniqueN(PLT_CN)]
if (n_undated_visits > 0L) {
  stop(
    format(n_undated_visits, big.mark = ","),
    " plot visit(s) have no measurement date in ", context_path, ".\n",
    "First/last endpoints cannot be ordered. Rebuild the visit context: ",
    "Rscript 05_fia/scripts/07_build_plot_visit_context.R"
  )
}

setorder(
  communities,
  stable_plot_id, life_stage,
  measurement_date_lower, measurement_date_upper, INVYR, PLT_CN
)

available_metrics <- intersect(metric_cols, names(communities))
# Treat a visit as usable when its core community temperature metric exists.
usable <- communities[!is.na(mean_temp)]
usable[, stage_visit_number := seq_len(.N), by = .(stable_plot_id, life_stage)]
usable[, n_stage_visits := .N, by = .(stable_plot_id, life_stage)]

# Select stage-specific endpoints before requiring common three-stage visits.
first_stage <- usable[stage_visit_number == 1L]
last_stage <- usable[stage_visit_number == n_stage_visits]
stage_pairs <- merge(
  first_stage,
  last_stage,
  by = c("stable_plot_id", "life_stage"),
  suffixes = c("_first", "_last"),
  all = FALSE,
  sort = FALSE
)
stage_pairs <- stage_pairs[n_stage_visits_first >= 2L]
stage_pairs[, n_visits_observed := n_stage_visits_first]
# Calculate last minus first so positive change always has the same direction.
for (metric in available_metrics) {
  stage_pairs[, (paste0("change_", metric)) :=
    get(paste0(metric, "_last")) - get(paste0(metric, "_first"))]
}

stage_keep <- c(
  "stable_plot_id", "life_stage",
  "PLT_CN_first", "PLT_CN_last",
  "measurement_date_lower_first", "measurement_date_upper_first",
  "measurement_date_precision_first",
  "measurement_date_lower_last", "measurement_date_upper_last",
  "measurement_date_precision_last",
  "forested_plot_proportion_first", "forested_plot_proportion_last",
  "n_visits_observed",
  unlist(lapply(available_metrics, function(metric) {
    c(
      paste0(metric, "_first"),
      paste0(metric, "_last"),
      paste0("change_", metric)
    )
  }))
)
stage_keep <- intersect(stage_keep, names(stage_pairs))
stage_output <- stage_pairs[, ..stage_keep]
setorder(stage_output, stable_plot_id, life_stage)

# Find visits where seedlings, saplings, and trees are all usable together.
common_visits <- usable[, .(
  n_life_stages = uniqueN(life_stage)
), by = .(
  stable_plot_id, PLT_CN, INVYR,
  measurement_date_lower, measurement_date_upper,
  measurement_date_precision
)][n_life_stages == length(layers)]
setorder(
  common_visits,
  stable_plot_id,
  measurement_date_lower, measurement_date_upper, INVYR, PLT_CN
)
common_visits[, common_visit_number := seq_len(.N), by = stable_plot_id]
common_visits[, n_visits_observed := .N, by = stable_plot_id]
# Keep the first and last shared visits only when at least two exist.
endpoints <- common_visits[
  n_visits_observed >= 2L &
    (common_visit_number == 1L | common_visit_number == n_visits_observed)
]
endpoints[, endpoint := fifelse(
  common_visit_number == 1L,
  "first",
  "last"
)]

endpoint_communities <- merge(
  usable,
  endpoints[, .(stable_plot_id, PLT_CN, endpoint, n_visits_observed)],
  by = c("stable_plot_id", "PLT_CN"),
  all = FALSE,
  sort = FALSE
)

value_cols <- intersect(
  c(
    available_metrics,
    "forested_plot_proportion",
    "forested_condition_weight_with_layer"
  ),
  names(endpoint_communities)
)
# Reshape metrics so life stage, endpoint, and metric become explicit column names.
long_values <- melt(
  endpoint_communities,
  id.vars = c(
    "stable_plot_id", "PLT_CN", "INVYR", "life_stage",
    "endpoint", "n_visits_observed",
    "measurement_date_lower", "measurement_date_upper",
    "measurement_date_precision"
  ),
  measure.vars = value_cols,
  variable.name = "metric",
  value.name = "value"
)
long_values[, output_column := paste(life_stage, endpoint, metric, sep = "_")]
wide_values <- dcast(
  long_values,
  stable_plot_id ~ output_column,
  value.var = "value"
)

endpoint_context <- dcast(
  unique(endpoints[, .(
    stable_plot_id, endpoint, PLT_CN, INVYR,
    measurement_date_lower, measurement_date_upper,
    measurement_date_precision, n_visits_observed
  )]),
  stable_plot_id + n_visits_observed ~ endpoint,
  value.var = c(
    "PLT_CN", "INVYR", "measurement_date_lower",
    "measurement_date_upper", "measurement_date_precision"
  )
)
setnames(
  endpoint_context,
  c("PLT_CN_first", "PLT_CN_last"),
  c("first_PLT_CN", "last_PLT_CN")
)

# Carry pairing evidence without using it to select the first or last visit.
audit <- as.data.table(read_parquet(audit_path))
pairing <- merge(
  common_visits[n_visits_observed >= 2L, .(stable_plot_id, current_PLT_CN = PLT_CN)],
  audit[, .(
    stable_plot_id, current_PLT_CN,
    pairing_class, selected_pair_source, pairing_usable
  )],
  by = c("stable_plot_id", "current_PLT_CN"),
  all.x = TRUE,
  sort = FALSE
)
pairing_summary <- pairing[, .(
  survey_pairing_classes_observed =
    paste(sort(unique(na.omit(pairing_class))), collapse = ";"),
  survey_pair_sources_observed =
    paste(sort(unique(na.omit(selected_pair_source))), collapse = ";"),
  n_structural_matches = sum(pairing_class == "structural_match", na.rm = TRUE),
  n_chronological_fallbacks =
    sum(selected_pair_source == "chronological_sampled_fallback", na.rm = TRUE),
  n_unresolved_visit_links =
    sum(!pairing_usable & pairing_class != "first_observed_visit", na.rm = TRUE),
  all_resolved_links_structural = all(
    pairing_class %in% c("first_observed_visit", "structural_match") |
      is.na(pairing_class)
  )
), by = stable_plot_id]

# Join endpoint context, community values, and pairing QA at stable-plot grain.
primary <- Reduce(
  function(x, y) merge(x, y, by = "stable_plot_id", all = FALSE, sort = FALSE),
  list(endpoint_context, wide_values, pairing_summary)
)
for (stage in layers) {
  for (metric in available_metrics) {
    first_col <- paste(stage, "first", metric, sep = "_")
    last_col <- paste(stage, "last", metric, sep = "_")
    if (all(c(first_col, last_col) %in% names(primary))) {
      primary[, (paste(stage, "change", metric, sep = "_")) :=
        get(last_col) - get(first_col)]
    }
  }
}
setorder(primary, stable_plot_id)

dir_create(therm_dir)
dir_create(qa_dir)
# Keep common-visit and stage-specific candidates as separate products.
write_parquet_atomic(primary, primary_path, compression = "snappy")
write_parquet_atomic(stage_output, stage_path, compression = "snappy")

qa <- data.table(
  metric = c(
    "stable_plots_with_any_forest_community",
    "stable_plots_with_two_stage_specific_visits",
    "stable_plots_with_two_common_three_stage_visits",
    "primary_rows_all_resolved_links_structural",
    "primary_rows_with_chronological_fallback"
  ),
  value = c(
    uniqueN(communities$stable_plot_id),
    uniqueN(stage_output$stable_plot_id),
    nrow(primary),
    primary[all_resolved_links_structural == TRUE, .N],
    primary[n_chronological_fallbacks > 0L, .N]
  )
)
fwrite(qa, file.path(qa_dir, "forest_first_last_change_summary.csv"))

cat(glue("Primary common-visit rows: {format(nrow(primary), big.mark = ',')}\n"))
cat(glue("Stage-specific rows:       {format(nrow(stage_output), big.mark = ',')}\n"))
cat("Done.\n")
