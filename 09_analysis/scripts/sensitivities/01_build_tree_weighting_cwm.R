#!/usr/bin/env Rscript

# Build paired adult-tree CWMs using individual abundance and basal area.
#
# This is a sensitivity family, not a replacement for canonical 09_analysis
# products. It reuses the canonical abundance-weighted tree condition CWM to
# define exact row membership, then changes only the adult-tree species weight.
# No models are fit.
#
# Run from the repository root:
#   Rscript 09_analysis/scripts/sensitivities/01_build_tree_weighting_cwm.R
#
# Optional:
#   --output-dir=/absolute/or/repo/relative/path

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fs)
  library(here)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("09_analysis/scripts/utils/deterministic_numeric.R"))

condition_keys <- c("PLT_CN", "INVYR", "CONDID")
metric_map <- c(
  temperature = "tmean_annual_mean",
  precipitation = "pr_annual_sum",
  CWD = "cwd_annual_sum"
)

arg_value <- function(args, name, default) {
  hit <- grep(paste0("^", name, "="), args, value = TRUE)
  if (length(hit)) sub(paste0("^", name, "="), "", hit[[1]]) else default
}

finite_max_abs <- function(x) {
  x <- abs(as.numeric(x))
  x <- x[is.finite(x)]
  if (length(x)) max(x) else NA_real_
}

finite_correlation <- function(x, y) {
  ok <- is.finite(x) & is.finite(y)
  if (sum(ok) < 2L || stats::sd(x[ok]) == 0 || stats::sd(y[ok]) == 0) {
    return(NA_real_)
  }
  stats::cor(x[ok], y[ok])
}

add_weight_metadata <- function(x, basis, column, unit) {
  x[, `:=`(
    weighting_basis = basis,
    weight_column = column,
    weight_unit = unit
  )]
  x
}

canonical_abundance_product <- function(canonical_tree) {
  out <- copy(canonical_tree)
  setnames(out, "total_individual_abundance", "community_weight_total")
  for (metric in names(metric_map)) {
    old_denom <- paste0(metric, "_niche_abundance")
    new_denom <- paste0(metric, "_weight_with_niche")
    setnames(out, old_denom, new_denom)
    out[, (paste0(metric, "_weighted_sum")) :=
          get(metric) * get(new_denom)]
    out[, (paste0(metric, "_weight_coverage")) := fifelse(
      community_weight_total > 0,
      get(new_denom) / community_weight_total,
      NA_real_
    )]
  }
  add_weight_metadata(
    out, "individual_abundance", "n_trees_tpa", "trees_per_acre"
  )
}

build_basal_area_product <- function(tree_rows, target_rows, niches) {
  target_identity <- target_rows[, c(
    condition_keys, "state", "forest_type_group", "CONDPROP_UNADJ", "n_species"
  ), with = FALSE]
  target_keys <- unique(target_identity[, ..condition_keys])

  tree_rows <- merge(
    tree_rows, target_keys, by = condition_keys, all = FALSE, sort = FALSE
  )
  tree_rows[, species_key := paste0("fia_spcd:", as.integer(SPCD))]
  species <- tree_rows[, .(
    community_weight = deterministic_sum(ba_per_acre, na.rm = TRUE)
  ), by = c(condition_keys, "species_key")]
  joined <- merge(species, niches, by = "species_key", all.x = TRUE, sort = FALSE)

  calculated <- joined[, .(
    n_species_basal_area = uniqueN(species_key[community_weight > 0]),
    community_weight_total = deterministic_sum(community_weight, na.rm = TRUE),
    temperature_weight_with_niche = deterministic_sum(
      community_weight[!is.na(tmean_annual_mean)], na.rm = TRUE
    ),
    precipitation_weight_with_niche = deterministic_sum(
      community_weight[!is.na(pr_annual_sum)], na.rm = TRUE
    ),
    CWD_weight_with_niche = deterministic_sum(
      community_weight[!is.na(cwd_annual_sum)], na.rm = TRUE
    ),
    temperature_weighted_sum = deterministic_sum(
      community_weight[!is.na(tmean_annual_mean)] *
        tmean_annual_mean[!is.na(tmean_annual_mean)], na.rm = TRUE
    ),
    precipitation_weighted_sum = deterministic_sum(
      community_weight[!is.na(pr_annual_sum)] *
        pr_annual_sum[!is.na(pr_annual_sum)], na.rm = TRUE
    ),
    CWD_weighted_sum = deterministic_sum(
      community_weight[!is.na(cwd_annual_sum)] *
        cwd_annual_sum[!is.na(cwd_annual_sum)], na.rm = TRUE
    )
  ), by = condition_keys]

  out <- merge(
    target_identity, calculated, by = condition_keys, all.x = TRUE, sort = FALSE
  )
  for (metric in names(metric_map)) {
    denom <- paste0(metric, "_weight_with_niche")
    numerator <- paste0(metric, "_weighted_sum")
    out[, (metric) := fifelse(
      is.finite(get(denom)) & get(denom) > 0,
      get(numerator) / get(denom),
      NA_real_
    )]
    out[, (paste0(metric, "_weight_coverage")) := fifelse(
      community_weight_total > 0,
      get(denom) / community_weight_total,
      NA_real_
    )]
  }
  out[, n_species := n_species_basal_area]
  out[, n_species_basal_area := NULL]
  add_weight_metadata(
    out, "basal_area", "ba_per_acre", "square_feet_per_acre"
  )
}

build_change_product <- function(cwm, intervals) {
  value_cols <- c(
    "n_species", "community_weight_total",
    unlist(lapply(names(metric_map), function(metric) c(
      metric,
      paste0(metric, "_weighted_sum"),
      paste0(metric, "_weight_with_niche"),
      paste0(metric, "_weight_coverage")
    )))
  )
  value_cols <- intersect(value_cols, names(cwm))

  t1 <- cwm[, c("PLT_CN", "CONDID", value_cols), with = FALSE]
  setnames(t1, "PLT_CN", "PREV_PLT_CN")
  setnames(t1, value_cols, paste0("T1_", value_cols))
  t2 <- cwm[, c("PLT_CN", "CONDID", value_cols), with = FALSE]
  setnames(t2, "PLT_CN", "T2_PLT_CN")
  setnames(t2, value_cols, paste0("T2_", value_cols))

  out <- merge(
    intervals, t1, by = c("PREV_PLT_CN", "CONDID"),
    all = FALSE, sort = FALSE
  )
  out <- merge(
    out, t2, by = c("T2_PLT_CN", "CONDID"),
    all = FALSE, sort = FALSE
  )
  for (metric in names(metric_map)) {
    out[, (paste0("delta_", metric)) :=
          get(paste0("T2_", metric)) - get(paste0("T1_", metric))]
  }
  out[, `:=`(
    weighting_basis = cwm$weighting_basis[[1]],
    weight_column = cwm$weight_column[[1]],
    weight_unit = cwm$weight_unit[[1]]
  )]
  out
}

compare_products <- function(abundance, basal, keys, value_cols) {
  a <- abundance[, c(keys, value_cols), with = FALSE]
  b <- basal[, c(keys, value_cols), with = FALSE]
  setnames(a, value_cols, paste0(value_cols, "_abundance"))
  setnames(b, value_cols, paste0(value_cols, "_basal_area"))
  out <- merge(a, b, by = keys, all = TRUE, sort = FALSE)
  for (value in value_cols) {
    out[, (paste0(value, "_basal_area_minus_abundance")) :=
          get(paste0(value, "_basal_area")) -
            get(paste0(value, "_abundance"))]
  }
  out
}

summarize_comparison <- function(comparison, level, values) {
  rbindlist(lapply(values, function(value) {
    abundance <- comparison[[paste0(value, "_abundance")]]
    basal <- comparison[[paste0(value, "_basal_area")]]
    difference <- comparison[[paste0(value, "_basal_area_minus_abundance")]]
    ok <- is.finite(abundance) & is.finite(basal)
    data.table(
      product_level = level,
      metric = value,
      n_rows = nrow(comparison),
      n_paired_finite = sum(ok),
      n_missingness_mismatches = sum(is.na(abundance) != is.na(basal)),
      mean_difference = if (any(ok)) mean(difference[ok]) else NA_real_,
      median_difference = if (any(ok)) stats::median(difference[ok]) else NA_real_,
      p95_absolute_difference = if (any(ok)) as.numeric(stats::quantile(
        abs(difference[ok]), 0.95, names = FALSE
      )) else NA_real_,
      maximum_absolute_difference = finite_max_abs(difference[ok]),
      correlation = finite_correlation(abundance, basal)
    )
  }))
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  cfg <- load_config()
  fia_summary_dir <- here(cfg$processed$fia$summaries$output_dir)
  niche_dir <- here(cfg$processed$species_niches$output_dir)
  canonical_dir <- here("09_analysis/data/processed")
  output_dir <- path_abs(arg_value(
    args, "--output-dir", here("09_analysis/data/sensitivity/tree_weighting")
  ))
  qa_dir <- here("09_analysis/qa/outputs/tree_weighting")
  dir_create(c(output_dir, qa_dir))

  canonical <- as.data.table(read_parquet(
    path(canonical_dir, "condition_visit_cwm.parquet")
  ))[layer == "trees"]
  abundance <- canonical_abundance_product(canonical)

  study_niches <- as.data.table(read_parquet(
    path(niche_dir, "species_climate_niches_us_study_area.parquet"),
    col_select = c("species_key", unname(metric_map))
  ))
  global_niches <- as.data.table(read_parquet(
    path(niche_dir, cfg$processed$species_niches$files$species_climate_niches),
    col_select = c("species_key", unname(metric_map))
  ))
  niches <- unique(rbindlist(list(
    study_niches,
    global_niches[!species_key %in% study_niches$species_key]
  )), by = "species_key")

  tree_rows <- as.data.table(read_parquet(
    path(fia_summary_dir, "plot_tree_species.parquet"),
    col_select = c(condition_keys, "SPCD", "ba_per_acre")
  ))
  basal <- build_basal_area_product(tree_rows, abundance, niches)

  common_order <- c(
    condition_keys, "state", "forest_type_group", "CONDPROP_UNADJ",
    "weighting_basis", "weight_column", "weight_unit", "n_species",
    "community_weight_total",
    unlist(lapply(names(metric_map), function(metric) c(
      paste0(metric, "_weighted_sum"),
      paste0(metric, "_weight_with_niche"),
      paste0(metric, "_weight_coverage"), metric
    )))
  )
  common_order <- intersect(common_order, names(abundance))
  setcolorder(abundance, c(common_order, setdiff(names(abundance), common_order)))
  setcolorder(basal, c(common_order, setdiff(names(basal), common_order)))
  setorder(abundance, PLT_CN, CONDID)
  setorder(basal, PLT_CN, CONDID)

  if (!identical(
    abundance[, ..condition_keys], basal[, ..condition_keys]
  )) stop("Abundance and basal-area condition key sets/order differ")

  write_parquet_atomic(
    abundance, path(output_dir, "tree_condition_visit_cwm_abundance.parquet")
  )
  write_parquet_atomic(
    basal, path(output_dir, "tree_condition_visit_cwm_basal_area.parquet")
  )

  intervals <- as.data.table(read_parquet(
    path(canonical_dir, "stable_condition_intervals.parquet")
  ))
  abundance_change <- build_change_product(abundance, intervals)
  basal_change <- build_change_product(basal, intervals)
  setorder(abundance_change, stable_condition_interval_key)
  setorder(basal_change, stable_condition_interval_key)

  if (!identical(
    abundance_change$stable_condition_interval_key,
    basal_change$stable_condition_interval_key
  )) stop("Abundance and basal-area change key sets/order differ")

  write_parquet_atomic(
    abundance_change,
    path(output_dir, "tree_stable_condition_cwm_change_abundance.parquet")
  )
  write_parquet_atomic(
    basal_change,
    path(output_dir, "tree_stable_condition_cwm_change_basal_area.parquet")
  )

  condition_values <- c(
    "community_weight_total",
    names(metric_map),
    paste0(names(metric_map), "_weight_coverage")
  )
  condition_comparison <- compare_products(
    abundance, basal, condition_keys, condition_values
  )
  change_values <- paste0("delta_", names(metric_map))
  change_keys <- intersect(c(
    "stable_condition_interval_key", "stable_plot_id",
    "remeasurement_component_id", "state", "PREV_PLT_CN", "T2_PLT_CN",
    "CONDID", "T1_INVYR", "T2_INVYR", "interval_years"
  ), names(abundance_change))
  change_comparison <- compare_products(
    abundance_change, basal_change, change_keys, change_values
  )
  write_parquet_atomic(
    condition_comparison,
    path(output_dir, "tree_condition_visit_weighting_comparison.parquet")
  )
  write_parquet_atomic(
    change_comparison,
    path(output_dir, "tree_stable_condition_weighting_comparison.parquet")
  )

  summary <- rbindlist(list(
    summarize_comparison(condition_comparison, "condition_visit", names(metric_map)),
    summarize_comparison(change_comparison, "stable_condition_change", change_values)
  ))
  fwrite(summary, path(output_dir, "tree_weighting_summary.csv"))

  reproduction <- rbindlist(lapply(names(metric_map), function(metric) {
    data.table(
      field = metric,
      canonical_rows = nrow(canonical),
      exported_rows = nrow(abundance),
      missingness_mismatches = sum(is.na(canonical[[metric]]) !=
                                     is.na(abundance[[metric]])),
      maximum_absolute_difference = finite_max_abs(
        canonical[[metric]] - abundance[[metric]]
      )
    )
  }))
  reproduction[, passed :=
    canonical_rows == exported_rows & missingness_mismatches == 0 &
      maximum_absolute_difference < 1e-12]
  fwrite(reproduction, path(qa_dir, "abundance_reproduction_checks.csv"))
  if (!all(reproduction$passed)) stop("Canonical abundance reproduction failed")

  checks <- data.table(
    check = c(
      "condition_keys_identical", "change_keys_identical",
      "abundance_weights_nonnegative", "basal_area_weights_nonnegative",
      "condition_comparison_unique", "change_comparison_unique"
    ),
    passed = c(
      identical(abundance[, ..condition_keys], basal[, ..condition_keys]),
      identical(abundance_change$stable_condition_interval_key,
                basal_change$stable_condition_interval_key),
      all(abundance$community_weight_total >= 0, na.rm = TRUE),
      all(basal$community_weight_total >= 0, na.rm = TRUE),
      !anyDuplicated(condition_comparison[, ..condition_keys]),
      !anyDuplicated(change_comparison$stable_condition_interval_key)
    )
  )
  fwrite(checks, path(qa_dir, "tree_weighting_validation_checks.csv"))
  if (!all(checks$passed)) stop("Tree-weighting validation failed")

  readme <- c(
    "# Adult-tree CWM weighting sensitivity", "",
    "Parallel adult-tree condition and stable-condition-change products using",
    "explicit individual-abundance (`n_trees_tpa`) and basal-area",
    "(`ba_per_acre`) weights. Row membership and every method other than the",
    "species weight are held fixed. These files do not replace canonical",
    "`09_analysis/data/processed/` products and no models are fit.", "",
    "Use `tree_weighting_summary.csv` for a compact comparison and the two",
    "`*_weighting_comparison.parquet` files for paired row-level values."
  )
  writeLines(readme, path(output_dir, "README.md"))
  message("Built paired adult-tree abundance and basal-area CWM sensitivity")
}

if (sys.nframe() == 0L) main()
