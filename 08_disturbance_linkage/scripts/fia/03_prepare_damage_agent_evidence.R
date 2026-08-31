#!/usr/bin/env Rscript

# Prepare neutral FIA TREE damage-agent evidence and candidate condition
# fractions. This script does not select a primary severity measure, aggregate
# conditions to plots, or filter the general evidence to forest land.
#
# Usage:
#   Rscript 08_disturbance_linkage/scripts/fia/03_prepare_damage_agent_evidence.R
#   Rscript 08_disturbance_linkage/scripts/fia/03_prepare_damage_agent_evidence.R WV

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(bit64)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/forest_analysis.R"))
source(here("scripts/utils/fia_damage_agents.R"))
source(here("scripts/utils/build_freshness.R"))

`%||%` <- function(left, right) {
  if (is.null(left) || length(left) == 0L || !nzchar(left)) right else left
}

config <- load_config()
raw_cfg <- config$raw$fia
fia_cfg <- config$processed$fia
link_cfg <- config$processed$disturbance_linkage
raw_dir <- here(raw_cfg$local_dir)
foundation_path <- file.path(
  here(fia_cfg$summaries$output_dir),
  fia_cfg$summaries$files$forested_condition_foundation
)
visit_context_path <- here(link_cfg$inputs$fia_plot_visit_context)
lookup_path <- here("05_fia/lookups/fia_damage_agent_lookup.csv")
out_dir <- here(link_cfg$output_dir)
qa_dir <- here(link_cfg$qa_dir)

output_dirs <- list(
  tree_evidence = file.path(
    out_dir,
    link_cfg$files$fia_tree_damage_agent_evidence %||%
      "fia_tree_damage_agent_evidence"
  ),
  condition_denominators = file.path(
    out_dir,
    link_cfg$files$fia_condition_damage_denominators %||%
      "fia_condition_damage_denominators"
  ),
  condition_agent_candidates = file.path(
    out_dir,
    link_cfg$files$fia_condition_damage_agent_candidates %||%
      "fia_condition_damage_agent_candidates"
  ),
  insect_severity_candidates = file.path(
    out_dir,
    link_cfg$files$fia_insect_severity_candidates %||%
      "fia_insect_severity_candidates"
  )
)

if (!file.exists(foundation_path)) {
  stop(
    "Forested-condition foundation is missing: ", foundation_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/02_build_forested_condition_foundation.R"
  )
}
if (!file.exists(lookup_path)) {
  stop(
    "Reviewed FIA damage-agent lookup is missing: ", lookup_path, "\n",
      "Run: python 05_fia/scripts/reference/01_build_damage_agent_lookup.py"
  )
}
if (!file.exists(visit_context_path)) {
  stop(
    "FIA plot-visit context is missing: ", visit_context_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R"
  )
}

# A state argument supports small validation runs; no argument builds all states.
args <- commandArgs(trailingOnly = TRUE)
state_args <- toupper(args[!grepl("^--", args)])
states <- if (length(state_args) > 0L) state_args else raw_cfg$states

# --force / --force=<state> overrides the freshness check for this run.
options(build_force_rebuild = build_force_from_args(args))
dia_min <- as.numeric(raw_cfg$tree_filters$dia_min_inches)
invyr_min <- as.integer(raw_cfg$invyr_min)
invyr_max <- as.integer(raw_cfg$invyr_max)

# Load all condition classes so the evidence remains general-purpose.
foundation <- as.data.table(read_parquet(
  foundation_path,
  col_select = c(
    "PLT_CN", "INVYR", "CONDID", "stable_plot_id", "state",
    "COND_STATUS_CD", "CONDPROP_UNADJ", "is_forested_condition",
    "forested_plot_proportion", "forested_condition_weight"
  )
))
visit_context <- as.data.table(read_parquet(
  visit_context_path,
  col_select = c(
    "PLT_CN", "INVYR", "MEASYEAR", "MEASMON", "MANUAL",
    "KINDCD", "CYCLE"
  )
))
forest_assert_unique(
  visit_context,
  c("PLT_CN", "INVYR"),
  "FIA plot-visit context"
)
# Add visit protocol fields used to qualify current lookup definitions.
foundation <- merge(
  foundation,
  visit_context,
  by = c("PLT_CN", "INVYR"),
  all.x = TRUE,
  sort = FALSE
)
lookup <- fread(lookup_path, na.strings = c("", "NA"))
lookup[, is_insect_agent := as.logical(is_insect_agent)]

for (path in c(out_dir, qa_dir, unlist(output_dirs))) dir_create(path)

# Process one state at a time to avoid loading the national TREE table in memory.
state_qa <- vector("list", length(states))
qa_path <- file.path(qa_dir, "fia_damage_agent_build_by_state.csv")
prior_qa <- if (file.exists(qa_path)) fread(qa_path) else NULL
for (i in seq_along(states)) {
  state_code <- states[[i]]
  tree_path <- file.path(raw_dir, state_code, paste0(state_code, "_TREE.csv"))
  state_conditions <- foundation[state == state_code]

  if (!file.exists(tree_path)) {
    state_qa[[i]] <- data.table(
      state = state_code,
      status = "raw_tree_missing"
    )
    next
  }
  if (nrow(state_conditions) == 0L) {
    state_qa[[i]] <- data.table(
      state = state_code,
      status = "condition_foundation_missing"
    )
    next
  }

  # Check the inexpensive header before reading a potentially large TREE file.
  header <- names(fread(tree_path, nrows = 0L, showProgress = FALSE))
  missing <- setdiff(fia_damage_required_tree_cols, header)
  if (length(missing) > 0L) {
    state_qa[[i]] <- data.table(
      state = state_code,
      status = paste0("missing_columns:", paste(missing, collapse = ";"))
    )
    next
  }

  # Freshness is decided per state so one refreshed state does not force a
  # national rebuild. The tree evidence stands in for all four products: they
  # come out of one pass and are written together.
  state_evidence_path <- file.path(
    output_dirs$tree_evidence, paste0("state=", state_code), "part.parquet"
  )
  state_decision <- build_should_rebuild(
    state_evidence_path,
    input_paths = c(foundation_path, visit_context_path, lookup_path, tree_path),
    required_cols = c("PLT_CN", "INVYR", "CONDID", "TREE_CN", "DAMAGE_AGENT_CD"),
    label = state_code
  )
  if (!state_decision$rebuild) {
    cat(glue("[{i}/{length(states)}] {state_code} skip ({state_decision$reason})\n"))
    # Carry the previous QA row forward so a skipped state keeps its counts
    # instead of reporting a hole where the numbers used to be.
    prior_row <- if (!is.null(prior_qa) && state_code %in% prior_qa$state) {
      prior_qa[state == state_code]
    } else {
      data.table(state = state_code, status = "skipped_fresh")
    }
    state_qa[[i]] <- prior_row
    next
  }

  cat(glue("[{i}/{length(states)}] {state_code} rebuild ({state_decision$reason})\n"))
  trees <- fread(
    tree_path,
    select = fia_damage_required_tree_cols,
    showProgress = FALSE
  )
  # Build tree evidence, denominators, and all candidate fractions together.
  result <- fia_prepare_damage_agent_state(
    trees,
    state_conditions,
    lookup,
    dia_min_inches = dia_min,
    invyr_min = invyr_min,
    invyr_max = invyr_max
  )

  # Keep state partitions replaceable without rewriting every other state.
  for (product_name in names(output_dirs)) {
    state_dir <- file.path(
      output_dirs[[product_name]],
      paste0("state=", state_code)
    )
    dir_create(state_dir)
    write_parquet_atomic(
      result[[product_name]],
      file.path(state_dir, "part.parquet"),
      compression = "snappy"
    )
  }

  state_qa[[i]] <- cbind(
    data.table(
      state = state_code,
      status = "ok",
      n_condition_rows = nrow(state_conditions),
      n_tree_evidence_rows = nrow(result$tree_evidence),
      n_condition_denominator_rows = nrow(result$condition_denominators),
      n_condition_agent_candidate_rows =
        nrow(result$condition_agent_candidates),
      n_insect_candidate_rows = nrow(result$insect_severity_candidates)
    ),
    result$audit
  )
  rm(trees, result, state_conditions)
  gc(verbose = FALSE)
}

qa <- rbindlist(state_qa, fill = TRUE)
# Preserve QA for states not included in a partial rebuild.
if (file.exists(qa_path) && !setequal(states, raw_cfg$states)) {
  previous_qa <- fread(qa_path)
  previous_qa <- previous_qa[!state %in% states]
  qa <- rbindlist(list(previous_qa, qa), use.names = TRUE, fill = TRUE)
}
setorder(qa, state)
fwrite(qa, qa_path)

cat("\nPrepared FIA damage-agent products:\n")
for (product_name in names(output_dirs)) {
  cat(glue("  {product_name}: {output_dirs[[product_name]]}\n"))
}
cat(glue(
  "QA: {qa_path}\n"
))
