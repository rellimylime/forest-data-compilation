# ==============================================================================
# 05_build_fia_summaries.R
# Build analysis-ready FIA summary products from extracted parquet files.
#
# This script is now a lightweight orchestrator. The step-specific code lives in
# 05_fia/scripts/summaries/ so each product can be read, tested, and edited on its own.
#
# Usage:
#   Rscript 05_fia/scripts/05_build_fia_summaries.R
#
# Output: 05_fia/data/processed/summaries/
# ==============================================================================

source("scripts/utils/load_config.R")
source("scripts/utils/fia_year_schema.R")
source("scripts/utils/parquet_atomic.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(arrow)
library(dplyr)
library(tibble)

# Force rebuilds: `--force` rebuilds every product; `--force=<product,product>`
# rebuilds only the named ones. Builders read this via getOption("fia_force_rebuild").
.args <- commandArgs(trailingOnly = TRUE)
if (any(.args == "--force")) {
  options(fia_force_rebuild = TRUE)
} else {
  .force_eq <- grep("^--force=", .args, value = TRUE)
  if (length(.force_eq) > 0) {
    options(fia_force_rebuild = trimws(unlist(strsplit(sub("^--force=", "", .force_eq), ","))))
  }
}

# Source small builder modules after packages are loaded so each step stays focused.
summary_dir <- here("05_fia/scripts/summaries")
source(file.path(summary_dir, "summary_helpers.R"))
source(file.path(summary_dir, "build_tree_metrics.R"))
source(file.path(summary_dir, "build_seedling_metrics.R"))
source(file.path(summary_dir, "build_mortality_metrics.R"))
source(file.path(summary_dir, "build_condition_forest_type.R"))
source(file.path(summary_dir, "build_condition_metadata.R"))
source(file.path(summary_dir, "build_tree_species.R"))
source(file.path(summary_dir, "build_seedling_species.R"))
source(file.path(summary_dir, "build_disturbance_classification.R"))
source(file.path(summary_dir, "build_disturbance_history.R"))
source(file.path(summary_dir, "build_treatment_history.R"))
source(file.path(summary_dir, "build_damage_agents.R"))
source(file.path(summary_dir, "build_exclusion_flags.R"))

# Load FIA config paths and state list once for all summary products.
fia_config <- config$raw$fia
proc_fia   <- config$processed$fia
out_dir    <- here(proc_fia$summaries$output_dir)
states     <- fia_config$states

# Open condition data once because multiple products need plot-condition fields.
# open_cond_dataset() forces TRTYR*/DSTRBYR* to int32 in the national union so a
# Boolean (all-empty) state partition cannot coerce real years to TRUE regardless
# of partition/file order (see scripts/utils/fia_year_schema.R).
cond_ds <- tryCatch(
  {
    ds <- open_cond_dataset(here(proc_fia$cond$output_dir), partitioning = "state")
    assert_fia_year_schema(ds, context = "national cond dataset")
    ds
  },
  error = function(e) {
    # A hard schema-contract violation must not be silently swallowed.
    if (grepl("schema contract violated", conditionMessage(e))) stop(e)
    NULL
  }
)

# Create the summary directory before any builder attempts to write parquet output.
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

cat("FIA Plot-Level Summaries\n")
cat("========================\n\n")
cat(glue("Output: {out_dir}\n\n"))

# Directory of the state cond partitions, used by builders for freshness checks.
cond_dir <- here(proc_fia$cond$output_dir)

# Run products in dependency order; downstream steps receive the paths they need.
out_tree_metrics  <- build_tree_metrics(out_dir, proc_fia, states, cond_ds)
out_seed_metrics  <- build_seedling_metrics(out_dir, proc_fia, states)
out_mort_metrics  <- build_mortality_metrics(out_dir, proc_fia)
out_cond_metrics  <- build_condition_forest_type(out_dir, cond_ds, cond_dir)
out_cond_metadata <- build_condition_metadata(out_dir, cond_ds)
out_tree_species  <- build_tree_species(
  out_dir, proc_fia, states, out_cond_metadata
)
out_seed_species  <- build_seedling_species(out_dir, proc_fia, out_cond_metadata)
out_disturb_class <- build_disturbance_classification(out_dir, out_cond_metadata)
out_disturb       <- build_disturbance_history(out_dir, cond_ds, cond_dir)
out_treat         <- build_treatment_history(out_dir, cond_ds, cond_dir)
out_damage_ag     <- build_damage_agents(out_dir, proc_fia)
out_excl_flags    <- build_exclusion_flags(out_dir, proc_fia, cond_ds, cond_dir)

cat("FIA summaries complete.\n\n")
cat("Outputs:\n")
output_files <- c(
  out_tree_metrics, out_seed_metrics, out_mort_metrics,
  out_cond_metrics, out_cond_metadata, out_tree_species, out_seed_species,
  out_disturb_class, out_disturb, out_treat, out_damage_ag, out_excl_flags
)
for (f in output_files) {
  if (file_exists(f)) cat(glue("  {basename(f)}: {file_size(f)}\n"))
}

cat("\nRead with:\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_tree_metrics.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_tree_species.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_sapling_species.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_seedling_species.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_disturbance_classification.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_disturbance_history.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_treatment_history.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_damage_agents.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_exclusion_flags.parquet')\n")
