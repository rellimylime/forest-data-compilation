# ==============================================================================
# 00_build_remeasurement_components.R
# Deterministic remeasurement components from FIA's official PREV_PLT_CN links.
#
# One physical FIA plot can hold several separate remeasurement histories. FIA
# supplies a previous-visit link only when it considers two records a genuine
# remeasurement; where that link is absent the visits are not connected, however
# close in time and however certainly they share a location.
#
# Connectivity here is exactly three conditions and nothing else:
#
#   1. an explicit PREV_PLT_CN is present
#   2. it resolves to a visit available in the snapshot
#   3. that visit belongs to the same stable_plot_id
#
# Deliberately NOT part of connectivity: sampled status, date ordering, CWM or
# life-stage availability, damage-agent assessment, or whether the visit can
# serve as a response endpoint. Those are downstream usability criteria and they
# differ per analysis -- a visit can be unusable for trees and usable for
# seedlings. If any of them leaked in here, the same plot could be connected for
# one life stage and disconnected for another, which is incoherent.
#
# Output grain:
#   one row per plot visit (PLT_CN)
#
# Usage:
#   Rscript 09_analysis/scripts/00_build_remeasurement_components.R
#   Rscript 09_analysis/scripts/00_build_remeasurement_components.R --force
#
# Documentation:
#   09_analysis/docs/METHODS.md
# ==============================================================================

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
source(here("scripts/utils/fia_components.R"))

args <- commandArgs(trailingOnly = TRUE)
options(build_force_rebuild = build_force_from_args(args))

config <- load_config()
fia_cfg <- config$processed$fia
analysis_cfg <- config$processed$analysis

visit_context_path <- file.path(
  here(fia_cfg$summaries$output_dir), "plot_visit_context.parquet"
)
out_dir <- here(analysis_cfg$output_dir)
qa_dir <- here(
  "09_analysis/qa/outputs/00_remeasurement_components"
)
dir_create(out_dir)
dir_create(qa_dir)

out_path <- file.path(out_dir, "fia_remeasurement_components.parquet")
qa_path <- file.path(qa_dir, "remeasurement_component_summary.csv")

if (!file.exists(visit_context_path)) {
  stop(
    "FIA plot-visit context is missing: ", visit_context_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R"
  )
}

rebuild <- build_should_rebuild(
  out_path,
  input_paths = visit_context_path,
  required_cols = c("PLT_CN", "remeasurement_component_id",
                    "connectivity_edge_valid"),
  label = "fia_remeasurement_components"
)
build_log_decision("fia_remeasurement_components", rebuild)
if (!rebuild$rebuild) {
  cat("Nothing to do. Pass --force to rebuild anyway.\n")
  if (!interactive()) quit(save = "no", status = 0)
}

cat("FIA Remeasurement Components\n")
cat("============================\n\n")

visits <- as.data.table(read_parquet(
  visit_context_path,
  col_select = c("PLT_CN", "PREV_PLT_CN", "stable_plot_id", "INVYR", "state",
                 "STATECD", "MEASYEAR", "measurement_date_lower",
                 "measurement_date_upper", "is_sampled_plot", "MANUAL")
))

components <- fia_add_remeasurement_components(visits)

# Order visits within their component so first/last selection downstream has a
# single agreed sequence. Measurement date first; INVYR and PLT_CN break ties.
setorder(components, remeasurement_component_id, measurement_date_lower,
         INVYR, PLT_CN)
components[, visit_number_in_component := seq_len(.N),
           by = remeasurement_component_id]

keep <- c(
  "PLT_CN", "PREV_PLT_CN", "stable_plot_id", "remeasurement_component_id",
  "visit_number_in_component", "n_visits_in_component",
  "connectivity_edge_valid", "visit_link_status",
  "official_link_present", "official_target_available",
  "official_target_same_stable_plot",
  "INVYR", "MEASYEAR", "measurement_date_lower", "measurement_date_upper",
  "is_sampled_plot", "MANUAL", "state", "STATECD"
)
components <- components[, intersect(keep, names(components)), with = FALSE]

# ------------------------------------------------------------------------------
# QA
# ------------------------------------------------------------------------------

per_plot <- components[, .(n_components = uniqueN(remeasurement_component_id)),
                       by = stable_plot_id]

# A constant cannot go in `by` alongside a column, so tag the section after
# each aggregation rather than inside it.
tag <- function(dt, section) {
  out <- copy(as.data.table(dt))
  out[, section := section]
  setcolorder(out, c("section", setdiff(names(out), "section")))
  out[]
}

qa <- rbindlist(list(
  data.table(section = "edges", category = "visits", n = nrow(components)),
  tag(components[, .(n = .N), by = .(category = visit_link_status)], "edges"),
  data.table(section = "edges", category = "connectivity_edge_valid",
             n = components[connectivity_edge_valid == TRUE, .N]),
  data.table(section = "components", category = "total_components",
             n = uniqueN(components$remeasurement_component_id)),
  data.table(section = "components", category = "stable_plots",
             n = uniqueN(components$stable_plot_id)),
  tag(per_plot[, .(n = .N), by = .(category = as.character(n_components))],
      "components_per_stable_plot"),
  tag(components[, .(n = uniqueN(remeasurement_component_id)),
                 by = .(category = fifelse(n_visits_in_component >= 5L, "5+",
                                           as.character(n_visits_in_component)))],
      "component_size")
), fill = TRUE)
setorder(qa, section, -n)

write_parquet_atomic(components, out_path, compression = "snappy")
fwrite(qa, qa_path)

cat(glue("Visits:            {format(nrow(components), big.mark = ',')}"), "\n")
cat(glue("Connectivity edges: {format(components[connectivity_edge_valid == TRUE, .N], big.mark = ',')}"), "\n")
cat(glue("Components:        {format(uniqueN(components$remeasurement_component_id), big.mark = ',')}"), "\n")
cat(glue("Stable plots:      {format(uniqueN(components$stable_plot_id), big.mark = ',')}"), "\n")
cat(glue("Plots with >1 component: {format(per_plot[n_components > 1L, .N], big.mark = ',')}"), "\n\n")

cat("--- visit link status ---\n")
print(components[, .N, by = visit_link_status][order(-N)])
cat("\n--- components per stable plot ---\n")
print(per_plot[, .N, by = n_components][order(n_components)])

cat("\nDone.\n")
cat(glue("Components: {out_path}"), "\n")
cat(glue("QA:         {qa_path}"), "\n")
