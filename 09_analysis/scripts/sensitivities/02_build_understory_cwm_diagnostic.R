#!/usr/bin/env Rscript

# Build an exploratory P2VEG understory CWM diagnostic.
#
# This script does not choose a final coverage threshold and does not modify the
# active analysis. It preserves cover numerators/denominators, distinguishes
# survey and data-availability states, and reports several candidate thresholds.
#
# Run from the repository root:
#   Rscript 09_analysis/scripts/sensitivities/02_build_understory_cwm_diagnostic.R

suppressPackageStartupMessages({
  library(arrow)
  library(bit64)
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
understory_groups <- c(
  "understory_combined", "understory_shrubs", "understory_forbs",
  "understory_graminoids", "understory_tree_layers"
)
coverage_thresholds <- c(0.50, 0.70, 0.80, 0.90)

arg_value <- function(args, name, default) {
  hit <- grep(paste0("^", name, "="), args, value = TRUE)
  if (length(hit)) sub(paste0("^", name, "="), "", hit[[1]]) else default
}

p2_group <- function(growth_habit) {
  habit <- tolower(fifelse(is.na(growth_habit), "unknown", growth_habit))
  fcase(
    grepl("shrub|subshrub", habit), "understory_shrubs",
    grepl("forb|herb", habit), "understory_forbs",
    grepl("graminoid|grass|sedge|rush", habit), "understory_graminoids",
    grepl("tree", habit), "understory_tree_layers",
    default = "understory_other"
  )
}

structure_group <- function(code) {
  fcase(
    code %chin% c("SH", "SS"), "understory_shrubs",
    code %chin% c("FB"), "understory_forbs",
    code %chin% c("GR"), "understory_graminoids",
    code %chin% c("TT", "NT", "LT", "SD", "ST"),
      "understory_tree_layers",
    default = "understory_other"
  )
}

cwm_status <- function(n_structure_subplots, structure_cover,
                       n_species_rows, total_cover, species_level_cover,
                       niche_cover) {
  fcase(
    is.na(n_structure_subplots) | n_structure_subplots <= 0,
      "NU_NO_P2VEG_STRUCTURE_SURVEY",
    is.na(structure_cover) | structure_cover <= 0,
      "NU_NO_RECORDED_GROUP_COVER",
    is.na(n_species_rows) | n_species_rows <= 0,
      "NU_STRUCTURE_COVER_WITHOUT_SPECIES_RECORDS",
    is.na(total_cover) | total_cover <= 0,
      "NU_NO_POSITIVE_SPECIES_COVER",
    is.na(species_level_cover) | species_level_cover <= 0,
      "NU_NO_SPECIES_LEVEL_COVER",
    is.na(niche_cover) | niche_cover <= 0,
      "NU_NO_USABLE_NICHE_COVER",
    default = "CWM_AVAILABLE"
  )
}

make_target_endpoints <- function(intervals) {
  t1 <- intervals[, .(
    stable_plot_id, state, PLT_CN = PREV_PLT_CN, INVYR = T1_INVYR, CONDID
  )]
  t2 <- intervals[, .(
    stable_plot_id, state, PLT_CN = T2_PLT_CN, INVYR = T2_INVYR, CONDID
  )]
  unique(rbindlist(list(t1, t2)), by = condition_keys)
}

make_target_grid <- function(targets) {
  targets <- copy(targets)
  targets[, join__ := 1L]
  groups <- data.table(understory_group = understory_groups, join__ = 1L)
  out <- merge(targets, groups, by = "join__", allow.cartesian = TRUE)
  out[, join__ := NULL]
  out
}

read_structure_support <- function(raw_dir, targets) {
  parts <- list()
  for (state_name in sort(unique(targets$state))) {
    source_path <- path(
      raw_dir, state_name, paste0(state_name, "_P2VEG_SUBP_STRUCTURE.csv")
    )
    if (!file_exists(source_path)) next
    state_targets <- targets[state == state_name]
    source <- fread(
      source_path,
      select = c("PLT_CN", "INVYR", "SUBP", "CONDID", "GROWTH_HABIT_CD",
                 "COVER_PCT"),
      integer64 = "integer64", showProgress = FALSE
    )
    source <- merge(
      source, state_targets[, ..condition_keys],
      by = condition_keys, all = FALSE, sort = FALSE
    )
    if (!nrow(source)) next
    source[, `:=`(
      understory_group = structure_group(GROWTH_HABIT_CD),
      structure_cover = as.numeric(COVER_PCT)
    )]
    combined <- copy(source)
    combined[, understory_group := "understory_combined"]
    source <- rbindlist(list(
      source[understory_group %chin% understory_groups], combined
    ), use.names = TRUE)
    parts[[state_name]] <- source[, .(
      n_structure_records = .N,
      n_structure_subplots_surveyed = uniqueN(SUBP),
      structure_cover_sum = deterministic_sum(structure_cover, na.rm = TRUE),
      n_structure_subplots_positive_cover = uniqueN(
        SUBP[is.finite(structure_cover) & structure_cover > 0]
      )
    ), by = c(condition_keys, "understory_group")]
  }
  if (!length(parts)) {
    return(data.table(
      PLT_CN = integer64(), INVYR = integer(), CONDID = integer(),
      understory_group = character(), n_structure_records = integer(),
      n_structure_subplots_surveyed = integer(),
      structure_cover_sum = numeric(),
      n_structure_subplots_positive_cover = integer()
    ))
  }
  rbindlist(parts, fill = TRUE)
}

build_species_summary <- function(veg, universe, niches) {
  veg[, species_code__ := fcoalesce(accepted_symbol, plant_symbol, VEG_SPCD)]
  veg[, species_key := paste0("p2veg:", species_code__)]
  veg[, understory_group := p2_group(fcoalesce(
    growth_habit, plant_growth_habit, "unknown"
  ))]
  veg[, cover_weight := as.numeric(cover_pct_subpcond)]
  veg <- veg[is.finite(cover_weight) & cover_weight > 0]

  combined <- copy(veg)
  combined[, understory_group := "understory_combined"]
  veg <- rbindlist(list(
    veg[understory_group %chin% understory_groups], combined
  ), use.names = TRUE, fill = TRUE)

  veg <- merge(
    veg,
    universe[, .(species_key, needs_niche, is_pseudo_taxon)],
    by = "species_key", all.x = TRUE, sort = FALSE
  )
  veg <- merge(veg, niches, by = "species_key", all.x = TRUE, sort = FALSE)

  veg[, .(
    n_species_rows = .N,
    n_species_recorded = uniqueN(species_key),
    n_species_level_taxa = uniqueN(species_key[needs_niche %in% TRUE]),
    n_species_pseudo_or_aggregate = uniqueN(
      species_key[is_pseudo_taxon %in% TRUE | needs_niche %in% FALSE]
    ),
    n_species_subplots_with_records = uniqueN(SUBP),
    total_recorded_cover_sum = deterministic_sum(cover_weight, na.rm = TRUE),
    species_level_cover_sum = deterministic_sum(
      cover_weight[needs_niche %in% TRUE], na.rm = TRUE
    ),
    temperature_niche_cover = deterministic_sum(
      cover_weight[!is.na(tmean_annual_mean)], na.rm = TRUE
    ),
    precipitation_niche_cover = deterministic_sum(
      cover_weight[!is.na(pr_annual_sum)], na.rm = TRUE
    ),
    CWD_niche_cover = deterministic_sum(
      cover_weight[!is.na(cwd_annual_sum)], na.rm = TRUE
    ),
    temperature_weighted_sum = deterministic_sum(
      cover_weight[!is.na(tmean_annual_mean)] *
        tmean_annual_mean[!is.na(tmean_annual_mean)], na.rm = TRUE
    ),
    precipitation_weighted_sum = deterministic_sum(
      cover_weight[!is.na(pr_annual_sum)] *
        pr_annual_sum[!is.na(pr_annual_sum)], na.rm = TRUE
    ),
    CWD_weighted_sum = deterministic_sum(
      cover_weight[!is.na(cwd_annual_sum)] *
        cwd_annual_sum[!is.na(cwd_annual_sum)], na.rm = TRUE
    )
  ), by = c(condition_keys, "understory_group")]
}

finalize_condition_diagnostic <- function(grid, support, species_summary) {
  out <- merge(
    grid, support,
    by = c(condition_keys, "understory_group"), all.x = TRUE, sort = FALSE
  )
  out <- merge(
    out, species_summary,
    by = c(condition_keys, "understory_group"), all.x = TRUE, sort = FALSE
  )
  out[, `:=`(
    total_recorded_cover_per_surveyed_subplot = fifelse(
      n_structure_subplots_surveyed > 0,
      total_recorded_cover_sum / n_structure_subplots_surveyed,
      NA_real_
    ),
    species_level_cover_fraction = fifelse(
      total_recorded_cover_sum > 0,
      species_level_cover_sum / total_recorded_cover_sum,
      NA_real_
    )
  )]

  for (metric in names(metric_map)) {
    denom <- paste0(metric, "_niche_cover")
    numerator <- paste0(metric, "_weighted_sum")
    fraction <- paste0(metric, "_niche_cover_fraction")
    status <- paste0(metric, "_status")
    out[, (metric) := fifelse(
      is.finite(get(denom)) & get(denom) > 0,
      get(numerator) / get(denom),
      NA_real_
    )]
    out[, (fraction) := fifelse(
      total_recorded_cover_sum > 0,
      get(denom) / total_recorded_cover_sum,
      NA_real_
    )]
    out[, (status) := cwm_status(
      n_structure_subplots_surveyed, structure_cover_sum,
      n_species_rows, total_recorded_cover_sum, species_level_cover_sum,
      get(denom)
    )]
    # A species record can exist even when the condition lacks comparable
    # structure-survey support. Retain its numerator and cover diagnostics, but
    # never expose a numeric CWM when the explicit availability status is NU.
    out[get(status) != "CWM_AVAILABLE", (metric) := NA_real_]
    for (threshold in coverage_thresholds) {
      label <- sprintf("%02d", round(threshold * 100))
      out[, (paste0(metric, "_meets_coverage_", label)) :=
            get(status) == "CWM_AVAILABLE" &
              is.finite(get(fraction)) & get(fraction) >= threshold]
    }
  }
  setorder(out, stable_plot_id, INVYR, PLT_CN, CONDID, understory_group)
  out
}

build_interval_diagnostic <- function(condition, intervals) {
  values <- c(
    names(metric_map),
    paste0(names(metric_map), "_niche_cover_fraction"),
    paste0(names(metric_map), "_status")
  )
  values <- c(values, unlist(lapply(names(metric_map), function(metric) {
    paste0(metric, "_meets_coverage_", sprintf("%02d", coverage_thresholds * 100))
  })))
  values <- intersect(values, names(condition))

  t1 <- condition[, c("PLT_CN", "CONDID", "understory_group", values), with = FALSE]
  setnames(t1, "PLT_CN", "PREV_PLT_CN")
  setnames(t1, values, paste0("T1_", values))
  t2 <- condition[, c("PLT_CN", "CONDID", "understory_group", values), with = FALSE]
  setnames(t2, "PLT_CN", "T2_PLT_CN")
  setnames(t2, values, paste0("T2_", values))

  groups <- data.table(understory_group = understory_groups, join__ = 1L)
  base <- copy(intervals)
  base[, join__ := 1L]
  base <- merge(base, groups, by = "join__", allow.cartesian = TRUE)
  base[, join__ := NULL]
  out <- merge(
    base, t1, by = c("PREV_PLT_CN", "CONDID", "understory_group"),
    all.x = TRUE, sort = FALSE
  )
  out <- merge(
    out, t2, by = c("T2_PLT_CN", "CONDID", "understory_group"),
    all.x = TRUE, sort = FALSE
  )
  for (metric in names(metric_map)) {
    out[, (paste0("delta_", metric)) := fifelse(
      get(paste0("T1_", metric, "_status")) == "CWM_AVAILABLE" &
        get(paste0("T2_", metric, "_status")) == "CWM_AVAILABLE",
      get(paste0("T2_", metric)) - get(paste0("T1_", metric)),
      NA_real_
    )]
    for (threshold in coverage_thresholds) {
      label <- sprintf("%02d", round(threshold * 100))
      out[, (paste0(metric, "_both_endpoints_meet_coverage_", label)) :=
            get(paste0("T1_", metric, "_meets_coverage_", label)) %in% TRUE &
              get(paste0("T2_", metric, "_meets_coverage_", label)) %in% TRUE]
    }
  }
  setorder(out, stable_condition_interval_key, understory_group)
  out
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  cfg <- load_config()
  raw_dir <- here(cfg$raw$fia$local_dir)
  fia_summary_dir <- here(cfg$processed$fia$summaries$output_dir)
  veg_dir <- here(cfg$processed$fia$understory_veg$output_dir)
  niche_dir <- here(cfg$processed$species_niches$output_dir)
  canonical_dir <- here("09_analysis/data/processed")
  output_dir <- path_abs(arg_value(
    args, "--output-dir",
    here("09_analysis/data/sensitivity/understory_cwm_diagnostic")
  ))
  qa_dir <- here("09_analysis/qa/outputs/understory_cwm_diagnostic")
  dir_create(c(output_dir, qa_dir))

  intervals <- as.data.table(read_parquet(
    path(canonical_dir, "stable_condition_intervals.parquet")
  ))
  targets <- make_target_endpoints(intervals)
  grid <- make_target_grid(targets)
  support <- read_structure_support(raw_dir, targets)

  p2_cols <- c(
    "PLT_CN", "INVYR", "CONDID", "SUBP", "accepted_symbol",
    "plant_symbol", "VEG_SPCD", "growth_habit", "plant_growth_habit",
    "cover_pct_subpcond"
  )
  veg <- as.data.table(open_dataset(veg_dir) |>
                         dplyr::select(dplyr::all_of(p2_cols)) |>
                         dplyr::collect())
  veg <- merge(veg, targets[, ..condition_keys],
               by = condition_keys, all = FALSE, sort = FALSE)

  universe <- as.data.table(read_parquet(
    path(niche_dir, "species_universe.parquet"),
    col_select = c("species_key", "needs_niche", "is_pseudo_taxon")
  ))
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

  species_summary <- build_species_summary(veg, universe, niches)
  condition <- finalize_condition_diagnostic(grid, support, species_summary)
  interval <- build_interval_diagnostic(condition, intervals)

  write_parquet_atomic(
    condition, path(output_dir, "understory_condition_cwm_diagnostic.parquet")
  )
  write_parquet_atomic(
    interval,
    path(output_dir, "understory_stable_condition_change_diagnostic.parquet")
  )

  status_summary <- rbindlist(lapply(names(metric_map), function(metric) {
    condition[, .N, by = .(
      understory_group,
      status = get(paste0(metric, "_status"))
    )][, metric := metric]
  }), fill = TRUE)
  setcolorder(status_summary, c("metric", "understory_group", "status", "N"))
  fwrite(status_summary, path(output_dir, "understory_status_summary.csv"))

  threshold_summary <- rbindlist(lapply(names(metric_map), function(metric) {
    rbindlist(lapply(coverage_thresholds, function(threshold) {
      label <- sprintf("%02d", round(threshold * 100))
      data.table(
        metric = metric,
        understory_group = understory_groups,
        threshold = threshold,
        n_condition_visits_meeting = vapply(understory_groups, function(group) {
          condition[understory_group == group,
                    sum(get(paste0(metric, "_meets_coverage_", label)) %in% TRUE)]
        }, integer(1)),
        n_intervals_both_endpoints_meeting = vapply(
          understory_groups, function(group) {
            interval[understory_group == group,
                     sum(get(paste0(
                       metric, "_both_endpoints_meet_coverage_", label
                     )) %in% TRUE)]
          }, integer(1)
        )
      )
    }))
  }))
  fwrite(threshold_summary, path(output_dir, "coverage_threshold_summary.csv"))

  checks <- data.table(
    check = c(
      "condition_key_group_unique", "interval_key_group_unique",
      "coverage_fractions_bounded", "no_numeric_zero_for_unavailable_cwm"
    ),
    passed = c(
      !anyDuplicated(condition[, .(PLT_CN, INVYR, CONDID, understory_group)]),
      !anyDuplicated(interval[, .(stable_condition_interval_key, understory_group)]),
      all(unlist(lapply(names(metric_map), function(metric) {
        x <- condition[[paste0(metric, "_niche_cover_fraction")]]
        is.na(x) | (x >= 0 & x <= 1 + 1e-10)
      }))),
      all(unlist(lapply(names(metric_map), function(metric) {
        status <- condition[[paste0(metric, "_status")]]
        value <- condition[[metric]]
        status == "CWM_AVAILABLE" | is.na(value)
      })))
    )
  )
  fwrite(checks, path(qa_dir, "understory_diagnostic_validation_checks.csv"))
  if (!all(checks$passed)) stop("Understory diagnostic validation failed")

  readme <- c(
    "# Understory CWM diagnostic pilot", "",
    "Exploratory P2VEG cover-weighted CWM products at eligible stable-condition",
    "endpoints. These products retain weighted sums, total recorded cover,",
    "species-level cover, niche-covered cover, coverage fractions, structure-",
    "survey support, and explicit NU status codes. They do not choose a final",
    "coverage threshold and are not active model inputs.", "",
    "`coverage_threshold_summary.csv` reports 50%, 70%, 80%, and 90% candidate",
    "niche-cover thresholds for decision-making. Missing/no-survey cases remain",
    "missing and are never encoded as numeric zero."
  )
  writeLines(readme, path(output_dir, "README.md"))
  message("Built exploratory understory CWM diagnostic")
}

if (sys.nframe() == 0L) main()
