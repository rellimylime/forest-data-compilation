# ==============================================================================
# 01_build_plot_recruitment_cwm.R
# Build condition-level recruitment climate-affinity metrics.
#
# This script joins FIA seedling species composition to the BIEN/TerraClimate
# species niche table, then summarizes each FIA condition as community-weighted
# means (CWMs). The output is the bridge between:
#
#   05_fia/             observed recruiting species on FIA plots
#   06_species_niches/  fixed species climate niche traits
#   07_thermophilization/ downstream disturbance/control comparisons
#
# Default niche input:
#   06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
#
# Output grain:
#   one row per stable_plot_id x PLT_CN x INVYR x CONDID
#
# Default output:
#   07_thermophilization/data/processed/plot_recruitment_cwm.parquet
#
# Usage:
#   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
#   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --limit=100
#   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --weight=treecount_total
#   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --range-scope=global
#   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --range-scope=us_study_area_with_global_fallback
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

# ------------------------------------------------------------------------------
# Command line options
# ------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  # Supports both --flag=value and --flag value styles.
  eq_hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(eq_hit) > 0) {
    return(sub(paste0("^", flag, "="), "", eq_hit[[1]]))
  }

  flag_pos <- which(args == flag)
  if (length(flag_pos) > 0 && flag_pos[[1]] < length(args)) {
    return(args[[flag_pos[[1]] + 1]])
  }

  default
}

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)

weight_col <- get_arg("--weight", "seedlings_tpa")
range_scope <- get_arg("--range-scope", "us_study_area_with_global_fallback")
analysis_range_scope <- range_scope

allowed_weights <- c("seedlings_tpa", "treecount_calc_total", "treecount_total", "presence")
if (!weight_col %in% allowed_weights) {
  stop(glue("--weight must be one of: {paste(allowed_weights, collapse = ', ')}"))
}

allowed_range_scopes <- c("global", "us_study_area", "us_study_area_with_global_fallback")
if (!range_scope %in% allowed_range_scopes) {
  stop(glue("--range-scope must be one of: {paste(allowed_range_scopes, collapse = ', ')}"))
}

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
fia_summary_dir <- here(config$processed$fia$summaries$output_dir)
niche_dir <- here(config$processed$species_niches$output_dir)
thermo_dir <- here(config$processed$thermophilization$output_dir)

smoke_data_dir <- here("07_thermophilization/data/smoke")
qa_dir <- if (is_smoke_run) {
  here("07_thermophilization/qa/smoke")
} else {
  here("07_thermophilization/qa/outputs")
}

seedling_species_path <- file.path(fia_summary_dir, "plot_seedling_species.parquet")
study_area_niche_path <- file.path(niche_dir, "species_climate_niches_us_study_area.parquet")
global_niche_path <- file.path(niche_dir, config$processed$species_niches$files$species_climate_niches)
availability_path <- file.path(niche_dir, config$processed$species_niches$files$bien_range_availability)

niche_path <- switch(
  range_scope,
  "global" = global_niche_path,
  "us_study_area" = study_area_niche_path,
  "us_study_area_with_global_fallback" = study_area_niche_path
)

out_file <- file.path(
  if (is_smoke_run) smoke_data_dir else thermo_dir,
  if (is_smoke_run) {
    sprintf("plot_recruitment_cwm_%s_limit_%d.parquet", range_scope, limit_arg)
  } else {
    config$processed$thermophilization$files$plot_recruitment_cwm
  }
)

qa_suffix <- if (is_smoke_run) {
  sprintf("%s_limit_%d", range_scope, limit_arg)
} else if (range_scope != "us_study_area_with_global_fallback") {
  range_scope
} else {
  ""
}

qa_name <- function(stem) {
  if (qa_suffix == "") {
    sprintf("%s.csv", stem)
  } else {
    sprintf("%s_%s.csv", stem, qa_suffix)
  }
}

summary_file <- file.path(qa_dir, qa_name("plot_recruitment_cwm_summary"))
state_coverage_file <- file.path(qa_dir, qa_name("plot_recruitment_cwm_coverage_by_state"))
missing_species_file <- file.path(qa_dir, qa_name("plot_recruitment_cwm_missing_species"))

dir_create(thermo_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)

if (!file.exists(seedling_species_path)) {
  stop(glue("FIA seedling species table not found: {seedling_species_path}"))
}
if (!file.exists(niche_path)) {
  stop(glue("Species climate niche table not found: {niche_path}"))
}
if (range_scope == "us_study_area_with_global_fallback" && !file.exists(global_niche_path)) {
  stop(glue("Global species climate niche table not found for fallback mode: {global_niche_path}"))
}
if (range_scope == "us_study_area_with_global_fallback" && !file.exists(availability_path)) {
  stop(glue("BIEN availability table not found for fallback mode: {availability_path}"))
}

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------

write_parquet_safely <- function(df, path, compression = "snappy") {
  # Write through a temp file so interrupted reruns cannot corrupt the output.
  dir_create(dirname(path))
  tmp_path <- tempfile(
    pattern = paste0(path_file(path), "_tmp_"),
    tmpdir = dirname(path),
    fileext = ".parquet"
  )
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)

  write_parquet(df, tmp_path, compression = compression)
  file_copy(tmp_path, path, overwrite = TRUE)
}

write_csv_safely <- function(df, path) {
  dir_create(dirname(path))
  tmp_path <- tempfile(
    pattern = paste0(path_file(path), "_tmp_"),
    tmpdir = dirname(path),
    fileext = ".csv"
  )
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)

  fwrite(df, tmp_path)
  file_copy(tmp_path, path, overwrite = TRUE)
}

weighted_mean_or_na <- function(x, w) {
  ok <- !is.na(x) & !is.na(w) & w > 0
  if (!any(ok)) return(NA_real_)
  stats::weighted.mean(x[ok], w[ok])
}

sum_or_na <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  sum(x, na.rm = TRUE)
}

first_nonmissing <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)
  x[[1]]
}

first_nonmissing_character <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

# ------------------------------------------------------------------------------
# Load inputs
# ------------------------------------------------------------------------------

cat("Plot Recruitment CWM Build\n")
cat("==========================\n\n")
cat(glue("Seedling species input: {seedling_species_path}"), "\n")
cat(glue("Species niche input:    {niche_path}"), "\n")
if (range_scope == "us_study_area_with_global_fallback") {
  # Fallback is limited to species that are still BIEN-available in the current
  # availability table. This prevents stale global niches from re-entering the
  # analysis after a name or range decision changes.
  cat(glue("Global fallback input:  {global_niche_path}"), "\n")
}
cat(glue("Range scope:            {range_scope}"), "\n")
cat(glue("Weighting:              {weight_col}"), "\n")
cat(glue("Output:                 {out_file}"), "\n\n")

seedlings <- as.data.table(read_parquet(seedling_species_path))
niches <- as.data.table(read_parquet(niche_path))

if (range_scope == "us_study_area_with_global_fallback") {
  availability <- as.data.table(read_parquet(availability_path))
  current_bien_available_keys <- availability[
    as.logical(bien_range_available) == TRUE,
    unique(species_key)
  ]

  global_niches <- as.data.table(read_parquet(global_niche_path))
  global_fallback <- global_niches[
    species_key %in% current_bien_available_keys &
      !species_key %in% niches$species_key
  ]

  niches[, `:=`(
    niche_scope_used = "us_study_area",
    niche_fallback_reason = NA_character_
  )]
  global_fallback[, `:=`(
    niche_scope_used = "global_fallback",
    niche_fallback_reason = "no_study_area_niche"
  )]

  niches <- rbindlist(list(niches, global_fallback), fill = TRUE, use.names = TRUE)
} else {
  niches[, `:=`(
    niche_scope_used = range_scope,
    niche_fallback_reason = NA_character_
  )]
}

if (!"SPCD" %in% names(seedlings)) {
  stop("Seedling species table must contain SPCD.")
}
if (!"species_key" %in% names(niches)) {
  stop("Species niche table must contain species_key.")
}

if (is_smoke_run) {
  # Limit by condition rather than raw rows so the smoke output still has
  # complete species composition for each selected plot condition.
  seedlings[, condition_key := paste(PLT_CN, INVYR, CONDID, sep = "|")]
  keep_conditions <- unique(seedlings$condition_key)[seq_len(min(limit_arg, uniqueN(seedlings$condition_key)))]
  seedlings <- seedlings[condition_key %in% keep_conditions]
}

# FIA tree seedling species join to the niche table through species_key, e.g.
# SPCD 531 -> fia_spcd:531.
seedlings[, species_key := paste0("fia_spcd:", as.integer(SPCD))]

# ------------------------------------------------------------------------------
# Collapse seedlings to one species row per FIA condition
# ------------------------------------------------------------------------------

metadata_cols <- intersect(
  c(
    "stable_plot_id", "PLT_CN", "INVYR", "CONDID", "state", "STATECD",
    "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN", "LAT", "LON", "ELEV",
    "FORTYPCD", "forest_type_label", "forest_type_group",
    "COND_STATUS_CD", "CONDPROP_UNADJ", "pct_forested",
    "is_forested_condition", "has_fire_condition", "has_crown_fire_condition",
    "has_insect_condition", "has_disease_condition", "has_wind_condition",
    "has_drought_condition", "has_human_dist_condition",
    "has_cutting_treatment"
  ),
  names(seedlings)
)

species_identity_cols <- intersect(
  c("SPCD", "species_key", "SCIENTIFIC_NAME", "COMMON_NAME", "GENUS", "SPECIES"),
  names(seedlings)
)

# Count subplots at the condition level before collapsing to species. This avoids
# trying to infer condition-level subplot coverage from already summarized
# species rows.
condition_subplot_counts <- seedlings[
  ,
  .(n_subplots_with_seedlings = uniqueN(SUBP)),
  by = metadata_cols
]

seedling_species <- seedlings[
  ,
  .(
    treecount_total = sum_or_na(treecount_total),
    treecount_calc_total = sum_or_na(treecount_calc_total),
    seedlings_tpa = sum_or_na(seedlings_tpa),
    n_seedling_records = sum_or_na(n_seedling_records)
  ),
  by = c(metadata_cols, species_identity_cols)
]

if (weight_col == "presence") {
  seedling_species[, cwm_weight := 1]
} else {
  if (!weight_col %in% names(seedling_species)) {
    stop(glue("Weight column not found after seedling aggregation: {weight_col}"))
  }
  seedling_species[, cwm_weight := as.numeric(get(weight_col))]
}
seedling_species[is.na(cwm_weight) | cwm_weight < 0, cwm_weight := 0]

# ------------------------------------------------------------------------------
# Join species niches
# ------------------------------------------------------------------------------

indicator_cols <- c(
  "tmean_annual_mean",
  "tmean_warmest_month_mean",
  "tmean_coldest_month_mean",
  "temp_seasonality_mean",
  "cwd_annual_sum",
  "cwd_max_month_mean",
  "pr_annual_sum",
  "pr_driest_month_mean"
)

missing_indicator_cols <- setdiff(indicator_cols, names(niches))
if (length(missing_indicator_cols) > 0) {
  stop(glue("Niche table is missing indicator(s): {paste(missing_indicator_cols, collapse = ', ')}"))
}

niche_keep_cols <- intersect(
  c(
    "species_key", "source_code_system", "source_species_code",
    "scientific_name", "common_name", "community_layers",
    "climate_period", "climate_source", "range_source", "range_scope",
    "niche_taxon_name", "niche_taxon_key",
    "niche_scope_used", "niche_fallback_reason", "niche_method", indicator_cols
  ),
  names(niches)
)

fia_niches <- niches[source_code_system == "fia_spcd", ..niche_keep_cols]
joined <- merge(seedling_species, fia_niches, by = "species_key", all.x = TRUE)
joined[, has_niche := !is.na(tmean_annual_mean)]

# ------------------------------------------------------------------------------
# Aggregate to plot/condition CWM
# ------------------------------------------------------------------------------

condition_cols <- metadata_cols

# For each condition, retain community totals and calculate eight weighted
# means. Species without a niche contribute to coverage denominators but not to
# a CWM numerator.
cwm <- joined[
  ,
  .(
    n_seedling_species_total = uniqueN(SPCD),
    n_seedling_species_with_niche = uniqueN(SPCD[has_niche]),
    n_seedling_records = as.numeric(sum(n_seedling_records, na.rm = TRUE)),
    treecount_total = as.numeric(sum(treecount_total, na.rm = TRUE)),
    treecount_with_niche = as.numeric(sum(ifelse(has_niche, treecount_total, 0.0), na.rm = TRUE)),
    treecount_calc_total = as.numeric(sum(treecount_calc_total, na.rm = TRUE)),
    treecount_calc_with_niche = as.numeric(sum(ifelse(has_niche, treecount_calc_total, 0.0), na.rm = TRUE)),
    seedlings_tpa_total = as.numeric(sum(seedlings_tpa, na.rm = TRUE)),
    seedlings_tpa_with_niche = as.numeric(sum(ifelse(has_niche, seedlings_tpa, 0.0), na.rm = TRUE)),
    seedlings_tpa_with_study_area_niche = as.numeric(sum(ifelse(has_niche & niche_scope_used == "us_study_area", seedlings_tpa, 0.0), na.rm = TRUE)),
    seedlings_tpa_with_global_fallback_niche = as.numeric(sum(ifelse(has_niche & niche_scope_used == "global_fallback", seedlings_tpa, 0.0), na.rm = TRUE)),
    cwm_weight_total = as.numeric(sum(cwm_weight, na.rm = TRUE)),
    cwm_weight_with_niche = as.numeric(sum(ifelse(has_niche, cwm_weight, 0.0), na.rm = TRUE)),
    cwm_weight_with_study_area_niche = as.numeric(sum(ifelse(has_niche & niche_scope_used == "us_study_area", cwm_weight, 0.0), na.rm = TRUE)),
    cwm_weight_with_global_fallback_niche = as.numeric(sum(ifelse(has_niche & niche_scope_used == "global_fallback", cwm_weight, 0.0), na.rm = TRUE)),
    cwm_temp = weighted_mean_or_na(tmean_annual_mean, cwm_weight),
    cwm_heat = weighted_mean_or_na(tmean_warmest_month_mean, cwm_weight),
    cwm_cold = weighted_mean_or_na(tmean_coldest_month_mean, cwm_weight),
    cwm_temp_seasonality = weighted_mean_or_na(temp_seasonality_mean, cwm_weight),
    cwm_cwd = weighted_mean_or_na(cwd_annual_sum, cwm_weight),
    cwm_peak_cwd = weighted_mean_or_na(cwd_max_month_mean, cwm_weight),
    cwm_pr = weighted_mean_or_na(pr_annual_sum, cwm_weight),
    cwm_dry_month_pr = weighted_mean_or_na(pr_driest_month_mean, cwm_weight),
    climate_period = first_nonmissing_character(climate_period),
    climate_source = first_nonmissing_character(climate_source),
    range_source = first_nonmissing_character(range_source),
    range_scope = analysis_range_scope,
    niche_scopes_used = paste(sort(unique(niche_scope_used[has_niche])), collapse = ";"),
    niche_method = first_nonmissing_character(niche_method)
  ),
  by = condition_cols
]

cwm <- merge(cwm, condition_subplot_counts, by = condition_cols, all.x = TRUE)

cwm[, frac_weight_with_niche := fifelse(
  cwm_weight_total > 0,
  cwm_weight_with_niche / cwm_weight_total,
  NA_real_
)]
cwm[, frac_seedling_species_with_niche := fifelse(
  n_seedling_species_total > 0,
  n_seedling_species_with_niche / n_seedling_species_total,
  NA_real_
)]
cwm[, weight_column := weight_col]
cwm[, frac_weight_with_global_fallback_niche := fifelse(
  cwm_weight_total > 0,
  cwm_weight_with_global_fallback_niche / cwm_weight_total,
  NA_real_
)]
cwm[, frac_weight_with_study_area_niche := fifelse(
  cwm_weight_total > 0,
  cwm_weight_with_study_area_niche / cwm_weight_total,
  NA_real_
)]

setcolorder(
  cwm,
  c(
    condition_cols,
    "weight_column",
    "range_scope",
    "niche_scopes_used",
    "climate_period",
    "climate_source",
    "range_source",
    "niche_method",
    setdiff(names(cwm), c(
      condition_cols, "weight_column", "range_scope", "niche_scopes_used", "climate_period",
      "climate_source", "range_source", "niche_method"
    ))
  )
)

# ------------------------------------------------------------------------------
# QA outputs
# ------------------------------------------------------------------------------

summary <- data.table(
  metric = c(
    "n_condition_rows",
    "n_condition_rows_with_cwm",
    "n_seedling_species_rows",
    "n_unique_seedling_species",
    "n_unique_seedling_species_missing_niche",
    "median_frac_weight_with_niche",
    "p10_frac_weight_with_niche",
    "n_condition_rows_below_95pct_niche_coverage",
    "n_condition_rows_using_global_fallback",
    "median_frac_weight_with_global_fallback_niche"
  ),
  value = c(
    nrow(cwm),
    sum(!is.na(cwm$cwm_temp)),
    nrow(seedling_species),
    uniqueN(seedling_species$SPCD),
    uniqueN(joined[has_niche == FALSE, SPCD]),
    stats::median(cwm$frac_weight_with_niche, na.rm = TRUE),
    as.numeric(stats::quantile(cwm$frac_weight_with_niche, 0.10, na.rm = TRUE)),
    sum(cwm$frac_weight_with_niche < 0.95, na.rm = TRUE),
    sum(cwm$cwm_weight_with_global_fallback_niche > 0, na.rm = TRUE),
    stats::median(cwm$frac_weight_with_global_fallback_niche, na.rm = TRUE)
  )
)
summary[, `:=`(
  range_scope = range_scope,
  weight_column = weight_col,
  smoke_limit = if (is_smoke_run) limit_arg else NA_integer_
)]

state_coverage <- cwm[
  ,
  .(
    n_condition_rows = .N,
    n_condition_rows_with_cwm = sum(!is.na(cwm_temp)),
    median_frac_weight_with_niche = stats::median(frac_weight_with_niche, na.rm = TRUE),
    p10_frac_weight_with_niche = as.numeric(stats::quantile(frac_weight_with_niche, 0.10, na.rm = TRUE)),
    n_condition_rows_below_95pct_niche_coverage = sum(frac_weight_with_niche < 0.95, na.rm = TRUE),
    n_condition_rows_using_global_fallback = sum(cwm_weight_with_global_fallback_niche > 0, na.rm = TRUE),
    median_frac_weight_with_global_fallback_niche = stats::median(frac_weight_with_global_fallback_niche, na.rm = TRUE)
  ),
  by = state
][order(state)]

missing_species <- joined[
  has_niche == FALSE,
  .(
    scientific_name = first_nonmissing(SCIENTIFIC_NAME),
    common_name = first_nonmissing(COMMON_NAME),
    n_condition_rows = uniqueN(paste(PLT_CN, INVYR, CONDID, sep = "|")),
    treecount_total = sum(treecount_total, na.rm = TRUE),
    treecount_calc_total = sum(treecount_calc_total, na.rm = TRUE),
    seedlings_tpa = sum(seedlings_tpa, na.rm = TRUE),
    cwm_weight_total = sum(cwm_weight, na.rm = TRUE)
  ),
  by = .(species_key, SPCD)
][order(-cwm_weight_total, species_key)]

# ------------------------------------------------------------------------------
# Write outputs
# ------------------------------------------------------------------------------

write_parquet_safely(cwm, out_file)
write_csv_safely(summary, summary_file)
write_csv_safely(state_coverage, state_coverage_file)
write_csv_safely(missing_species, missing_species_file)

cat("\nDone.\n")
cat(glue("CWM parquet:       {out_file}"), "\n")
cat(glue("QA summary:        {summary_file}"), "\n")
cat(glue("QA by state:       {state_coverage_file}"), "\n")
cat(glue("Missing species:   {missing_species_file}"), "\n\n")
print(summary)
