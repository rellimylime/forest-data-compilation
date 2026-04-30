# ==============================================================================
# validate_seedling_products.R
# Validate FIA seedling species products against plot-level seedling summaries
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(glue)
library(data.table)
library(arrow)
library(dplyr)

compute_shannon_h <- function(dt, group_cols, value_col) {
  # Work on a copy so the helper does not mutate the caller's data.table.
  dt <- copy(dt)

  # Compute total abundance within each plot visit before calculating species shares.
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]

  # Convert each species count to relative abundance within its plot visit.
  dt[total > 0, p_i := get(value_col) / total]

  # Calculate the per-species Shannon contribution only for valid proportions.
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]

  # Collapse species contributions back to one expected Shannon value per plot visit.
  dt[, .(shannon_h_count_expected = sum(h_i, na.rm = TRUE)), by = group_cols]
}

cat("FIA Seedling Product Validation\n")
cat("================================\n\n")

# Resolve configured input paths so the validator follows the production layout.
seed_dir <- here(config$processed$fia$seedlings$output_dir)
summary_path <- file.path(
  here(config$processed$fia$summaries$output_dir),
  "plot_seedling_metrics.parquet"
)

# Fail early when the upstream products have not been generated locally.
if (!dir.exists(seed_dir)) {
  stop(glue("Missing seedling species directory: {seed_dir}"))
}
if (!file.exists(summary_path)) {
  stop(glue("Missing plot seedling summary: {summary_path}"))
}

# Open species-level seedlings lazily, but read the smaller plot summary eagerly.
seed_ds <- open_dataset(seed_dir, partitioning = "state")
summary_dt <- as.data.table(read_parquet(summary_path))

# Confirm the species-level product still has the fields needed for composition.
required_seed_cols <- c("PLT_CN", "INVYR", "SPCD", "SFTWD_HRDWD", "treecount_total", "state")
missing_seed_cols <- setdiff(required_seed_cols, names(seed_ds))
if (length(missing_seed_cols) > 0) {
  stop(glue("Seedling species product is missing: {paste(missing_seed_cols, collapse=', ')}"))
}

# Confirm the plot summary has every field we need to reproduce and compare.
required_summary_cols <- c(
  "PLT_CN", "INVYR", "treecount_total", "count_softwood",
  "count_hardwood", "n_species_seedling", "shannon_h_count", "state"
)
missing_summary_cols <- setdiff(required_summary_cols, names(summary_dt))
if (length(missing_summary_cols) > 0) {
  stop(glue("Plot seedling summary is missing: {paste(missing_summary_cols, collapse=', ')}"))
}


# Validate state-by-state to keep memory bounded and make failures easier to locate.
states <- sort(unique(summary_dt$state))
tol <- 1e-8
results <- list()

for (st in states) {
  # Collect one state's species-level seedlings from the partitioned dataset.
  seed_dt <- tryCatch(
    seed_ds |> filter(state == st) |> collect() |> as.data.table(),
    error = function(e) NULL
  )

  # Record missing partitions as validation failures instead of stopping immediately.
  if (is.null(seed_dt) || nrow(seed_dt) == 0) {
    results[[st]] <- data.table(
      state = st,
      seed_species_rows = 0L,
      summary_rows = summary_dt[state == st, .N],
      status = "missing_species_product"
    )
    next
  }

  # Rebuild the plot-level count, functional group, and richness metrics from species rows.
  expected_totals <- seed_dt[, .(
    treecount_total_expected = sum(treecount_total, na.rm = TRUE),
    count_softwood_expected = sum(treecount_total[SFTWD_HRDWD == "S"], na.rm = TRUE),
    count_hardwood_expected = sum(treecount_total[SFTWD_HRDWD == "H"], na.rm = TRUE),
    n_species_seedling_expected = uniqueN(SPCD)
  ), by = .(PLT_CN, INVYR)]

  # Recompute Shannon H from species counts to match the production summary logic.
  expected_shannon <- compute_shannon_h(
    seed_dt,
    c("PLT_CN", "INVYR"),
    "treecount_total"
  )

  # Combine all expected metrics into the same plot-visit grain as the summary.
  expected <- merge(expected_totals, expected_shannon, by = c("PLT_CN", "INVYR"), all = TRUE)

  # Pull the observed plot summary rows for this state only.
  observed <- summary_dt[state == st, .(
    PLT_CN, INVYR, treecount_total, count_softwood,
    count_hardwood, n_species_seedling, shannon_h_count
  )]

  # Join expected and observed values so missing rows and numeric mismatches are both visible.
  cmp <- merge(expected, observed, by = c("PLT_CN", "INVYR"), all = TRUE)

  # Store compact state-level diagnostics for final reporting.
  results[[st]] <- data.table(
    state = st,
    seed_species_rows = nrow(seed_dt),
    summary_rows = nrow(observed),
    missing_from_summary = cmp[is.na(treecount_total), .N],
    missing_from_species = cmp[is.na(treecount_total_expected), .N],
    total_mismatches = cmp[abs(treecount_total_expected - treecount_total) > tol, .N],
    softwood_mismatches = cmp[abs(count_softwood_expected - count_softwood) > tol, .N],
    hardwood_mismatches = cmp[abs(count_hardwood_expected - count_hardwood) > tol, .N],
    richness_mismatches = cmp[n_species_seedling_expected != n_species_seedling, .N],
    shannon_mismatches = cmp[abs(shannon_h_count_expected - shannon_h_count) > tol, .N],
    status = "checked"
  )
}

# Stack all state diagnostics into one validation table.
validation <- rbindlist(results, fill = TRUE)

print(validation)

# Treat any missing rows or metric differences as validation problems.
problems <- validation[
  status != "checked" |
    missing_from_summary > 0 |
    missing_from_species > 0 |
    total_mismatches > 0 |
    softwood_mismatches > 0 |
    hardwood_mismatches > 0 |
    richness_mismatches > 0 |
    shannon_mismatches > 0
]

cat("\nSummary\n")
cat("-------\n")
cat(glue("States checked: {nrow(validation)}\n"))
cat(glue("Problem states: {nrow(problems)}\n"))

# Print the failing states before stopping so the rerun target is obvious.
if (nrow(problems) > 0) {
  cat("\nProblem details:\n")
  print(problems)
  stop("Seedling product validation failed.")
}

cat("\nSeedling product validation passed.\n")
