#!/usr/bin/env Rscript

# Collapse condition climate metrics to one forest-only value per plot visit.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/forest_analysis.R"))

config <- load_config()
fia_cfg <- config$processed$fia
therm_cfg <- config$processed$thermophilization
summary_dir <- here(fia_cfg$summaries$output_dir)
therm_dir <- here(therm_cfg$output_dir)
qa_dir <- here("07_thermophilization/qa/outputs")

foundation_name <- fia_cfg$summaries$files$forested_condition_foundation
foundation_path <- file.path(summary_dir, foundation_name)
if (!file.exists(foundation_path)) {
  stop(
    "Forested-condition foundation is missing: ", foundation_path, "\n",
    "Run: Rscript 05_fia/scripts/08_build_forested_condition_foundation.R"
  )
}
# The condition foundation supplies the authoritative forest flag and area weight.
foundation <- as.data.table(read_parquet(foundation_path))

# Build all life stages unless a focused layer is requested.
layers <- c("seedlings", "saplings", "trees")
args <- commandArgs(trailingOnly = TRUE)
layer_args <- sub("^--layer=", "", args[grepl("^--layer=", args)])
if (length(layer_args) > 0L) {
  layers <- intersect(layers, layer_args)
}
if (length(layers) == 0L) {
  stop("No valid layer selected; use seedlings, saplings, or trees.")
}

metric_cols <- c(
  paste0("mean_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  )),
  paste0("median_", c(
    "temp", "heat", "cold", "temp_seasonality",
    "cwd", "peak_cwd", "pr", "dry_month_pr"
  )),
  "frac_weight_with_niche",
  "frac_species_with_niche",
  "frac_weight_with_study_area_niche",
  "frac_weight_with_global_fallback_niche"
)

dir_create(therm_dir)
dir_create(qa_dir)

for (layer in layers) {
  input_name <- therm_cfg$files[[paste0("plot_community_climate_", layer)]]
  output_key <- paste0("forest_plot_visit_cwm_", layer)
  output_name <- therm_cfg$files[[output_key]]
  input_path <- file.path(therm_dir, input_name)
  output_path <- file.path(therm_dir, output_name)

  if (!file.exists(input_path)) {
    stop(
      "Condition climate product is missing: ", input_path, "\n",
      "Run: Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R ",
      "--layer=", layer
    )
  }

  cat(glue("\n{layer}: {input_path}\n"))
  # Keep only metric columns present in this life-stage product.
  condition_metrics <- as.data.table(read_parquet(input_path))
  available_metrics <- intersect(metric_cols, names(condition_metrics))
  # Exclude nonforest conditions and weight the remainder by forested area share.
  visit_cwm <- aggregate_forested_condition_cwm(
    condition_metrics,
    foundation,
    available_metrics
  )
  join_diagnostics <- attr(visit_cwm, "condition_join_diagnostics")
  visit_cwm[, `:=`(
    community_layer = layer,
    life_stage = layer,
    source_condition_product = input_name
  )]
  setcolorder(
    visit_cwm,
    c(
      "community_layer", "life_stage", "stable_plot_id",
      "PLT_CN", "INVYR", setdiff(names(visit_cwm), c(
        "community_layer", "life_stage", "stable_plot_id", "PLT_CN", "INVYR"
      ))
    )
  )
  write_parquet_atomic(visit_cwm, output_path, compression = "snappy")

  # Use join diagnostics from the aggregation so QA cannot drift from the build.
  summary <- data.table(
    life_stage = layer,
    metric = c(
      "condition_rows_in",
      "condition_rows_aggregated_as_forest",
      "condition_rows_excluded_as_nonforest",
      "condition_rows_missing_from_foundation",
      "forest_plot_visits",
      "stable_plots",
      "median_forested_condition_weight_with_layer",
      "n_visits_with_incomplete_forest_condition_layer_coverage"
    ),
    value = c(
      join_diagnostics$n_condition_rows_in,
      join_diagnostics$n_forested_condition_rows_aggregated,
      join_diagnostics$n_nonforest_condition_rows_excluded,
      join_diagnostics$n_conditions_missing_from_foundation,
      nrow(visit_cwm),
      uniqueN(visit_cwm$stable_plot_id),
      median(visit_cwm$forested_condition_weight_with_layer, na.rm = TRUE),
      visit_cwm[forested_condition_weight_with_layer < 0.999, .N]
    )
  )
  fwrite(
    summary,
    file.path(qa_dir, paste0("forest_plot_visit_cwm_summary_", layer, ".csv"))
  )
  cat(glue(
    "  wrote {format(nrow(visit_cwm), big.mark = ',')} forest plot visits -> ",
    "{output_path}\n"
  ))
}

cat("\nDone.\n")
