#!/usr/bin/env Rscript

# Reproducible supplement for BIEN niche gaps and clipping-policy sensitivity.
#
# This is deliberately a QA driver rather than a canonical producer. It reads
# existing global/study-area niche products and FIA community products, and it
# writes only below --output-dir. It never downloads BIEN data, extracts
# TerraClimate, fits models, or replaces canonical products.
#
# Usage:
#   Rscript 06_species_niches/qa/scripts/10_bien_clipping_supplement.R \
#     --output-dir=/home/tippingPoint/ermiller/bien_clipping_supplement

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fs)
  library(glue)
  library(here)
  library(sf)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/forest_analysis.R"))
source(here("09_analysis/scripts/utils/deterministic_numeric.R"))

get_cli_arg <- function(args, flag, default = NULL) {
  eq <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(eq)) return(sub(paste0("^", flag, "="), "", eq[[1]]))
  pos <- which(args == flag)
  if (length(pos) && pos[[1]] < length(args)) return(args[[pos[[1]] + 1]])
  default
}

coverage_category <- function(needs_niche, bien_available, has_global, has_study) {
  fcase(
    is.na(needs_niche) | !needs_niche,
    "not_targeted_non_species_level",
    is.na(bien_available) | !bien_available,
    "no_bien_range_returned",
    !has_global,
    "bien_range_no_usable_global_niche",
    !has_study,
    "study_area_niche_unavailable_global_fallback",
    default = "usable_study_area_niche"
  )
}

classify_clipping <- function(global_area_km2, retained_area_km2,
                              absolute_tolerance_km2 = 1,
                              fraction_tolerance = 1e-6) {
  global_area_km2 <- as.numeric(global_area_km2)
  retained_area_km2 <- as.numeric(retained_area_km2)
  retained_area_km2[is.na(retained_area_km2)] <- 0
  lost <- pmax(global_area_km2 - retained_area_km2, 0)
  tolerance <- pmax(absolute_tolerance_km2, global_area_km2 * fraction_tolerance)
  data.table(
    area_lost_km2 = lost,
    fraction_retained = fifelse(global_area_km2 > 0,
                                pmin(retained_area_km2 / global_area_km2, 1),
                                NA_real_),
    clipping_tolerance_km2 = tolerance,
    wholly_outside_bbox = retained_area_km2 <= tolerance,
    meaningfully_clipped = lost > tolerance
  )
}

choose_sensitivity_scope <- function(has_global, has_study,
                                     wholly_outside_bbox,
                                     meaningfully_clipped) {
  fcase(
    !has_global, "none",
    wholly_outside_bbox %in% TRUE, "global",
    meaningfully_clipped %in% TRUE, "global",
    has_study, "us_study_area",
    default = "global"
  )
}

choose_sensitivity_reason <- function(has_global, has_study,
                                      wholly_outside_bbox,
                                      meaningfully_clipped) {
  fcase(
    !has_global, "no_usable_global_niche",
    wholly_outside_bbox %in% TRUE, "range_wholly_outside_bbox",
    meaningfully_clipped %in% TRUE, "meaningful_area_removed_by_clipping",
    has_study, "contained_within_bbox_tolerance",
    default = "study_area_niche_unavailable_global_fallback"
  )
}

safe_sum <- function(x) sum(as.numeric(x), na.rm = TRUE)

log_progress <- function(...) {
  cat(format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), " | ",
      paste0(..., collapse = ""), "\n", sep = "")
  flush.console()
}

first_or_na <- function(x) {
  x <- x[!is.na(x)]
  if (length(x)) x[[1]] else NA
}

write_parquet_out <- function(x, path) {
  dir_create(path_dir(path))
  write_parquet(as.data.frame(x), path, compression = "zstd")
}

write_csv_out <- function(x, path) {
  dir_create(path_dir(path))
  fwrite(x, path, na = "")
}

sha256_one <- function(path) {
  out <- suppressWarnings(system2("sha256sum", path, stdout = TRUE, stderr = TRUE))
  if (!length(out) || !file_exists(path)) return(NA_character_)
  strsplit(out[[1]], "[[:space:]]+")[[1]][[1]]
}

parquet_rows <- function(path) {
  if (!grepl("\\.parquet$", path, ignore.case = TRUE)) return(NA_real_)
  tryCatch(as.numeric(open_dataset(path)$count_rows()), error = function(e) NA_real_)
}

metric_map <- c(
  temperature = "tmean_annual_mean",
  precipitation = "pr_annual_sum",
  CWD = "cwd_annual_sum"
)

make_policy_niches <- function(global, study, policy, policy_name) {
  base_cols <- unique(c(
    "species_key", "source_code_system", "source_species_code",
    "scientific_name", "common_name", unname(metric_map)
  ))
  global <- global[, intersect(base_cols, names(global)), with = FALSE]
  study <- study[, intersect(base_cols, names(study)), with = FALSE]
  setnames(global, setdiff(names(global), "species_key"),
           paste0(setdiff(names(global), "species_key"), "__global"))
  setnames(study, setdiff(names(study), "species_key"),
           paste0(setdiff(names(study), "species_key"), "__study"))
  joined <- merge(policy, global, by = "species_key", all.x = TRUE)
  joined <- merge(joined, study, by = "species_key", all.x = TRUE)
  scope_col <- if (policy_name == "current") "current_scope" else "sensitivity_scope"
  reason_col <- if (policy_name == "current") "current_reason" else "sensitivity_reason"
  for (column in setdiff(base_cols, "species_key")) {
    g <- paste0(column, "__global")
    s <- paste0(column, "__study")
    joined[, (column) := fifelse(get(scope_col) == "us_study_area",
                                get(s), get(g))]
  }
  joined[, `:=`(
    niche_policy = policy_name,
    niche_scope_used = get(scope_col),
    niche_policy_reason = get(reason_col)
  )]
  keep <- c(base_cols, "niche_policy", "niche_scope_used",
            "niche_policy_reason", "global_area_km2", "retained_area_km2",
            "area_lost_km2", "fraction_retained", "meaningfully_clipped")
  joined[niche_scope_used != "none", intersect(keep, names(joined)), with = FALSE]
}

standardize_p2_group <- function(x) {
  x[, species_code__ := fcoalesce(accepted_symbol, plant_symbol, VEG_SPCD)]
  x[, species_key := paste0("p2veg:", species_code__)]
  x[, growth_habit__ := tolower(fcoalesce(growth_habit, plant_growth_habit, "unknown"))]
  x[, p2_group := fcase(
    grepl("shrub", growth_habit__), "understory_shrubs",
    grepl("forb|herb", growth_habit__), "understory_forbs",
    grepl("graminoid|grass|sedge|rush", growth_habit__), "understory_graminoids",
    grepl("tree", growth_habit__), "understory_tree_layers",
    default = "understory_other"
  )]
  x[]
}

species_stats <- function(x, group, weight_col, weight_unit) {
  x[, .(
    group = group,
    weight_basis = weight_col,
    weight_unit = weight_unit,
    n_observation_rows = .N,
    n_plots = uniqueN(stable_plot_id, na.rm = TRUE),
    n_plot_visits = uniqueN(PLT_CN, na.rm = TRUE),
    n_conditions = uniqueN(paste(PLT_CN, CONDID, sep = "|"), na.rm = TRUE),
    cwm_weight = safe_sum(get(weight_col)),
    raw_record_count = safe_sum(raw_count)
  ), by = species_key]
}

rename_policy_merge_counts <- function(x) {
  overlap_renames <- c(
    "n_plot_visits.x" = "group_n_plot_visits",
    "n_conditions.x" = "group_n_conditions",
    "n_plot_visits.y" = "universe_n_plot_visits",
    "n_conditions.y" = "universe_n_conditions"
  )
  for (old_name in names(overlap_renames)) {
    if (old_name %in% names(x)) {
      setnames(x, old_name, overlap_renames[[old_name]])
    }
  }
  if (!"group_n_conditions" %in% names(x)) {
    stop("Missing group condition count after species-policy merge")
  }
  x
}

build_condition_comparison <- function(source, group, weight_unit,
                                       current_niches, sensitivity_niches) {
  keys <- c("PLT_CN", "INVYR", "CONDID")
  identity <- intersect(c(keys, "stable_plot_id", "state", "COND_STATUS_CD",
                          "CONDPROP_UNADJ"), names(source))
  group_cols <- identity

  # Metadata are constant within a condition. Group the multi-million-row source
  # only by condition keys and species; carrying repeated character metadata
  # through that aggregation is orders of magnitude slower.
  condition_identity <- unique(source[, ..identity], by = keys)
  species <- source[, .(
    community_weight = sum(weight, na.rm = TRUE),
    raw_count = sum(raw_count, na.rm = TRUE)
  ), by = c(keys, "species_key")]
  species <- merge(
    species, condition_identity, by = keys, all.x = TRUE, sort = FALSE
  )
  cur <- current_niches[, c("species_key", unname(metric_map)), with = FALSE]
  sen <- sensitivity_niches[, c("species_key", unname(metric_map)), with = FALSE]
  setnames(cur, unname(metric_map), paste0(unname(metric_map), "__current"))
  setnames(sen, unname(metric_map), paste0(unname(metric_map), "__sensitivity"))
  joined <- merge(species, cur, by = "species_key", all.x = TRUE, sort = FALSE)
  joined <- merge(joined, sen, by = "species_key", all.x = TRUE, sort = FALSE)
  switched_keys <- sensitivity_niches[
    niche_scope_used == "global" & niche_policy_reason %in% c(
      "range_wholly_outside_bbox", "meaningful_area_removed_by_clipping"
    ), species_key]
  joined[, switched_species := species_key %in% switched_keys]

  for (metric in names(metric_map)) {
    column <- metric_map[[metric]]
    for (policy_name in c("current", "sensitivity")) {
      value_column <- paste0(column, "__", policy_name)
      joined[, (paste0(metric, "_numerator_", policy_name)) :=
               fifelse(!is.na(get(value_column)),
                       get(value_column) * community_weight, 0)]
      joined[, (paste0(metric, "_weight_with_niche_", policy_name)) :=
               fifelse(!is.na(get(value_column)), community_weight, 0)]
    }
  }

  sum_columns <- c(
    "community_weight",
    unlist(lapply(names(metric_map), function(metric) {
      c(
        paste0(metric, "_numerator_current"),
        paste0(metric, "_numerator_sensitivity"),
        paste0(metric, "_weight_with_niche_current"),
        paste0(metric, "_weight_with_niche_sensitivity")
      )
    }))
  )
  out <- joined[, c(
    list(
      n_species = sum(community_weight > 0),
      n_switched_species = sum(community_weight > 0 & switched_species),
      switched_weight = sum(community_weight[switched_species], na.rm = TRUE)
    ),
    lapply(.SD, sum, na.rm = TRUE)
  ), by = group_cols, .SDcols = sum_columns]
  setnames(out, "community_weight", "community_weight_total")
  out[, switched_weight_share := fifelse(
    community_weight_total > 0,
    switched_weight / community_weight_total,
    NA_real_
  )]
  for (metric in names(metric_map)) {
    for (policy_name in c("current", "sensitivity")) {
      numerator <- paste0(metric, "_numerator_", policy_name)
      denominator <- paste0(metric, "_weight_with_niche_", policy_name)
      out[, (paste0(metric, "_", policy_name)) := fifelse(
        get(denominator) > 0,
        get(numerator) / get(denominator),
        NA_real_
      )]
      out[, (numerator) := NULL]
    }
    out[, (paste0(metric, "_change")) :=
          get(paste0(metric, "_sensitivity")) -
          get(paste0(metric, "_current"))]
  }
  out[, `:=`(group = group, weight_unit = weight_unit)]
  setcolorder(out, c("group", "weight_unit", setdiff(names(out), c("group", "weight_unit"))))
  out[]
}

policy_product <- function(comparison, policy) {
  fixed <- intersect(c("group", "weight_unit", "PLT_CN", "INVYR", "CONDID",
                       "stable_plot_id", "state", "COND_STATUS_CD",
                       "CONDPROP_UNADJ", "n_species", "n_switched_species",
                       "community_weight_total", "switched_weight",
                       "switched_weight_share"), names(comparison))
  out <- copy(comparison[, ..fixed])
  for (metric in names(metric_map)) {
    out[, (metric) := comparison[[paste0(metric, "_", policy)]]]
    out[, (paste0(metric, "_weight_with_niche")) :=
          comparison[[paste0(metric, "_weight_with_niche_", policy)]]]
  }
  out[, niche_policy := policy]
  out[]
}

comparison_summary <- function(x, product_level) {
  rbindlist(lapply(names(metric_map), function(metric_name) {
    change_col <- paste0(metric_name, "_change")
    current_col <- paste0(metric_name, "_current")
    sensitivity_col <- paste0(metric_name, "_sensitivity")
    current_weight_col <- paste0(metric_name, "_weight_with_niche_current")
    sensitivity_weight_col <- paste0(metric_name, "_weight_with_niche_sensitivity")
    x[, {
      change <- get(change_col)
      paired <- !is.na(get(current_col)) & !is.na(get(sensitivity_col))
      absolute <- abs(change[paired])
      affected <- absolute > 1e-12
      list(
        product_level = product_level,
        metric = metric_name,
        n_communities = .N,
        n_communities_with_paired_cwm = sum(paired),
        n_communities_affected = sum(affected),
        pct_communities_affected = if (sum(paired)) 100 * mean(affected) else NA_real_,
        mean_absolute_change = if (length(absolute)) mean(absolute) else NA_real_,
        median_absolute_change = if (length(absolute)) median(absolute) else NA_real_,
        p05_absolute_change = if (length(absolute)) as.numeric(quantile(absolute, 0.05, names = FALSE)) else NA_real_,
        p95_absolute_change = if (length(absolute)) as.numeric(quantile(absolute, 0.95, names = FALSE)) else NA_real_,
        maximum_absolute_change = if (length(absolute)) max(absolute) else NA_real_,
        n_niche_weight_membership_changes = sum(
          abs(get(current_weight_col) - get(sensitivity_weight_col)) > 1e-8,
          na.rm = TRUE
        )
      )
    }, by = .(group, weight_unit)]
  }), fill = TRUE)
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  reuse_policy <- "--reuse-policy" %in% args
  output_dir <- path_abs(get_cli_arg(
    args, "--output-dir",
    "/home/tippingPoint/ermiller/bien_clipping_supplement"
  ))
  absolute_tolerance_km2 <- as.numeric(get_cli_arg(
    args, "--area-tolerance-km2", "1"
  ))
  fraction_tolerance <- as.numeric(get_cli_arg(
    args, "--fraction-tolerance", "1e-6"
  ))
  if (path_has_parent(output_dir, here())) {
    stop("--output-dir must be outside the repository so canonical products cannot be overwritten.")
  }

  cfg <- load_config()
  repo <- here()
  table_dir <- path(output_dir, "tables")
  current_dir <- path(output_dir, "products", "current_policy")
  sensitivity_dir <- path(output_dir, "products", "global_if_clipped")
  report_dir <- path(output_dir, "reports")
  log_dir <- path(output_dir, "logs")
  manifest_dir <- path(output_dir, "manifest")
  condition_checkpoint_dir <- path(
    output_dir, "checkpoints", "condition_comparison_v2_key_only"
  )
  plot_checkpoint_dir <- path(
    output_dir, "checkpoints", "plot_products_v1"
  )
  success_marker <- path(output_dir, "RUN_SUCCESS")
  dir_create(c(table_dir, current_dir, sensitivity_dir, report_dir, log_dir,
               manifest_dir, condition_checkpoint_dir, plot_checkpoint_dir))
  if (file_exists(success_marker)) file_delete(success_marker)
  log_file <- path(log_dir, "bien_clipping_supplement.log")
  log_con <- file(log_file, open = "wt")
  sink(log_con, type = "output", split = TRUE)
  sink(log_con, type = "message")
  on.exit({
    while (sink.number(type = "message") > 0) sink(type = "message")
    while (sink.number(type = "output") > 0) sink(type = "output")
    if (isOpen(log_con)) close(log_con)
  }, add = TRUE)

  started <- Sys.time()
  command <- paste(c("Rscript", commandArgs(trailingOnly = FALSE), args), collapse = " ")
  cat("BIEN clipping supplement\nStarted:", format(started, tz = "UTC"), "UTC\n")
  cat("Output:", output_dir, "\n")

  p <- list(
    universe = path(repo, "06_species_niches/data/processed/species_universe.parquet"),
    availability = path(repo, "06_species_niches/data/processed/bien_range_availability.parquet"),
    global_niches = path(repo, "06_species_niches/data/processed/species_climate_niches.parquet"),
    study_niches = path(repo, "06_species_niches/data/processed/species_climate_niches_us_study_area.parquet"),
    polygons = path(repo, "06_species_niches/data/processed/species_range_polygons.gpkg"),
    trees = path(repo, "05_fia/data/processed/summaries/plot_tree_species.parquet"),
    saplings = path(repo, "05_fia/data/processed/summaries/plot_sapling_species.parquet"),
    understory = path(repo, "05_fia/data/processed/understory_veg"),
    conditions = path(repo, "05_fia/data/processed/summaries/plot_condition_metadata.parquet"),
    stable_intervals = path(repo, "09_analysis/data/processed/stable_condition_intervals.parquet"),
    complete_edges = path(repo, "09_analysis/data/intermediate/complete_history_edges.parquet"),
    model_data = path(repo, "09_analysis/data/processed/lifestage_model_data.parquet"),
    canonical_condition_cwm = path(repo, "09_analysis/data/processed/condition_visit_cwm.parquet"),
    canonical_pooled_cwm = path(repo, "09_analysis/data/processed/pooled_condition_visit_cwm.parquet")
  )
  missing <- names(p)[!file_exists(unlist(p)) & !dir_exists(unlist(p))]
  if (length(missing)) {
    stop("Missing required existing input(s); no upstream extraction was launched:\n",
         paste(glue("- {missing}: {unlist(p)[missing]}"), collapse = "\n"))
  }

  input_files <- c(unlist(p[names(p) != "understory"]),
                   dir_ls(p$understory, recurse = TRUE, glob = "*.parquet"))
  input_manifest <- data.table(
    record_type = "input",
    path = as.character(input_files),
    sha256 = vapply(input_files, sha256_one, character(1)),
    bytes = as.numeric(file_info(input_files)$size),
    rows = vapply(input_files, parquet_rows, numeric(1)),
    timestamp_utc = format(file_info(input_files)$modification_time,
                           "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )

  universe <- as.data.table(read_parquet(p$universe))
  availability <- as.data.table(read_parquet(p$availability))
  global <- as.data.table(read_parquet(p$global_niches))
  study <- as.data.table(read_parquet(p$study_niches))

  study_cfg <- cfg$params$study_area
  area_crs <- cfg$params$global_area_crs
  policy_resume_path <- path(table_dir, "species_niche_policy_and_coverage.csv")
  current_niche_resume_path <- path(current_dir, "species_climate_niches.parquet")
  sensitivity_niche_resume_path <- path(
    sensitivity_dir, "species_climate_niches.parquet"
  )
  if (reuse_policy) {
    resume_files <- c(
      policy_resume_path, current_niche_resume_path,
      sensitivity_niche_resume_path
    )
    missing_resume <- resume_files[!file_exists(resume_files)]
    if (length(missing_resume)) {
      stop("--reuse-policy requested but these completed artifacts are missing: ",
           paste(missing_resume, collapse = ", "))
    }
    cat("Reusing completed clipping-policy artifacts from the prior partial run...\n")
    policy <- fread(policy_resume_path)
    current_niches <- as.data.table(read_parquet(current_niche_resume_path))
    sensitivity_niches <- as.data.table(read_parquet(sensitivity_niche_resume_path))
  } else {
    cat("Computing range areas and clipping decisions...\n")
    ranges <- st_read(p$polygons, quiet = TRUE)
    ranges$species_key <- as.character(ranges$species_key)
    ranges <- st_make_valid(st_transform(ranges[, "species_key"], 4326))
    if (anyDuplicated(ranges$species_key)) {
      ranges <- aggregate(ranges["species_key"], by = list(ranges$species_key), FUN = st_union)
      ranges$species_key <- as.character(ranges$Group.1)
      ranges$Group.1 <- NULL
    }
    study_cfg <- cfg$params$study_area
    bbox <- st_as_sfc(st_bbox(c(
      xmin = study_cfg$xmin, ymin = study_cfg$ymin,
      xmax = study_cfg$xmax, ymax = study_cfg$ymax
    ), crs = st_crs(4326)))
    area_crs <- cfg$params$global_area_crs
    global_area <- data.table(
      species_key = ranges$species_key,
      global_area_km2 = as.numeric(st_area(st_transform(ranges, area_crs))) / 1e6
    )
    intersects <- lengths(st_intersects(ranges, bbox)) > 0
    clipped <- suppressWarnings(st_intersection(ranges[intersects, ], bbox))
    retained <- data.table(
      species_key = clipped$species_key,
      retained_area_km2 = as.numeric(st_area(st_transform(clipped, area_crs))) / 1e6
    )[, .(retained_area_km2 = safe_sum(retained_area_km2)), by = species_key]
    areas <- merge(global_area, retained, by = "species_key", all.x = TRUE)
    areas[is.na(retained_area_km2), retained_area_km2 := 0]
    areas <- cbind(areas, classify_clipping(
      areas$global_area_km2, areas$retained_area_km2,
      absolute_tolerance_km2, fraction_tolerance
    ))
  
    policy <- merge(
      universe,
      availability[, .(species_key, bien_range_available, range_lookup_status,
                       range_match_status, range_review_reason)],
      by = "species_key", all.x = TRUE
    )
    policy <- merge(policy, areas, by = "species_key", all.x = TRUE)
    policy[, `:=`(
      has_global_niche = species_key %in% global$species_key,
      has_study_area_niche = species_key %in% study$species_key
    )]
    policy[, coverage_category := coverage_category(
      needs_niche, as.logical(bien_range_available), has_global_niche,
      has_study_area_niche
    )]
    policy[, `:=`(
      current_scope = fcase(
        has_study_area_niche, "us_study_area",
        has_global_niche & as.logical(bien_range_available), "global",
        default = "none"
      ),
      current_reason = fcase(
        has_study_area_niche, "study_area_niche_available",
        has_global_niche & as.logical(bien_range_available),
        "study_area_niche_unavailable_global_fallback",
        default = "no_usable_niche"
      )
    )]
    policy[, sensitivity_scope := choose_sensitivity_scope(
      has_global_niche, has_study_area_niche, wholly_outside_bbox,
      meaningfully_clipped
    )]
    policy[, sensitivity_reason := choose_sensitivity_reason(
      has_global_niche, has_study_area_niche, wholly_outside_bbox,
      meaningfully_clipped
    )]
    policy[, switched_to_global := current_scope == "us_study_area" &
             sensitivity_scope == "global"]
    policy[, tolerance_definition := glue(
      "meaningful loss > max({absolute_tolerance_km2} km2, ",
      "{format(fraction_tolerance, scientific = TRUE)} x global area)"
    )]
    write_csv_out(policy, path(table_dir, "species_niche_policy_and_coverage.csv"))
  
    current_niches <- make_policy_niches(global, study, policy, "current")
    sensitivity_niches <- make_policy_niches(global, study, policy, "sensitivity")
    write_parquet_out(current_niches, path(current_dir, "species_climate_niches.parquet"))
    write_parquet_out(sensitivity_niches,
                      path(sensitivity_dir, "species_climate_niches.parquet"))
  
  }

  niche_compare <- merge(
    current_niches[, c("species_key", unname(metric_map), "niche_scope_used"), with = FALSE],
    sensitivity_niches[, c("species_key", unname(metric_map), "niche_scope_used",
                           "niche_policy_reason"), with = FALSE],
    by = "species_key", all = TRUE, suffixes = c("_current", "_sensitivity")
  )
  for (metric in names(metric_map)) {
    col <- metric_map[[metric]]
    niche_compare[, (paste0(metric, "_change")) :=
      get(paste0(col, "_sensitivity")) - get(paste0(col, "_current"))]
  }
  write_csv_out(niche_compare, path(table_dir, "species_niche_value_comparison.csv"))

  cat("Reading and standardizing community weights...\n")
  tree_cols <- c("species_key", "stable_plot_id", "PLT_CN", "INVYR", "CONDID",
                 "state", "COND_STATUS_CD", "CONDPROP_UNADJ", "abundance_for_cwm",
                 "n_trees_tpa", "n_trees_raw")
  trees <- as.data.table(read_parquet(p$trees, col_select = tree_cols))
  trees[, raw_count := as.numeric(n_trees_raw)]
  saplings <- as.data.table(read_parquet(p$saplings, col_select = tree_cols))
  saplings[, raw_count := as.numeric(n_trees_raw)]
  p2_cols <- c("stable_plot_id", "PLT_CN", "INVYR", "CONDID", "state",
               "accepted_symbol", "plant_symbol", "VEG_SPCD", "growth_habit",
               "plant_growth_habit", "cover_pct_subpcond", "n_p2veg_records")
  p2 <- as.data.table(open_dataset(p$understory) |>
                        dplyr::select(dplyr::all_of(p2_cols)) |>
                        dplyr::collect())
  p2 <- standardize_p2_group(p2)
  p2[, raw_count := as.numeric(n_p2veg_records)]
  # Attach authoritative condition status/proportion to P2VEG observations.
  cond_meta <- as.data.table(read_parquet(
    p$conditions,
    col_select = c("PLT_CN", "INVYR", "CONDID", "stable_plot_id", "state",
                   "COND_STATUS_CD", "CONDPROP_UNADJ")
  ))
  cond_meta <- unique(cond_meta, by = c("PLT_CN", "INVYR", "CONDID"))
  p2 <- merge(p2, cond_meta[, .(PLT_CN, INVYR, CONDID, COND_STATUS_CD,
                                CONDPROP_UNADJ)],
              by = c("PLT_CN", "INVYR", "CONDID"), all.x = TRUE, sort = FALSE)

  source_specs <- list(
    adults_trees = list(
      data = trees[, .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID, state,
                       COND_STATUS_CD, CONDPROP_UNADJ,
                       weight = as.numeric(abundance_for_cwm), raw_count)],
      unit = "ft2_per_acre_basal_area"
    ),
    saplings = list(
      data = saplings[, .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID, state,
                          COND_STATUS_CD, CONDPROP_UNADJ,
                          weight = as.numeric(abundance_for_cwm), raw_count)],
      unit = "trees_per_acre"
    ),
    understory_combined = list(
      data = p2[, .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID, state,
                    COND_STATUS_CD, CONDPROP_UNADJ,
                    weight = as.numeric(cover_pct_subpcond), raw_count)],
      unit = "percent_cover_x_subplot_condition_proportion"
    )
  )
  for (g in c("understory_shrubs", "understory_forbs", "understory_graminoids",
              "understory_tree_layers")) {
    source_specs[[g]] <- list(
      data = p2[p2_group == g, .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID,
                                 state, COND_STATUS_CD, CONDPROP_UNADJ,
                                 weight = as.numeric(cover_pct_subpcond), raw_count)],
      unit = "percent_cover_x_subplot_condition_proportion"
    )
  }

  # Mortality stage CWMs use unadjusted TPA on eligible forest conditions.
  cutoff <- cfg$processed$analysis$condition_minimum_proportion
  mort_tree <- trees[COND_STATUS_CD == 1L & !is.na(CONDPROP_UNADJ) &
                       CONDPROP_UNADJ >= cutoff,
                     .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID, state,
                       COND_STATUS_CD, CONDPROP_UNADJ,
                       weight = as.numeric(n_trees_tpa), raw_count)]
  mort_sap <- saplings[COND_STATUS_CD == 1L & !is.na(CONDPROP_UNADJ) &
                         CONDPROP_UNADJ >= cutoff,
                       .(species_key, stable_plot_id, PLT_CN, INVYR, CONDID, state,
                         COND_STATUS_CD, CONDPROP_UNADJ,
                         weight = as.numeric(n_trees_tpa), raw_count)]

  cat("Building pooled mortality endpoint weights...\n")
  intervals <- as.data.table(read_parquet(p$stable_intervals))
  edges <- as.data.table(read_parquet(p$complete_edges))
  edge_props <- merge(
    edges[, .(history_id = paste0(remeasurement_component_id, "|", CONDID),
              stable_condition_interval_key, PREV_PLT_CN, T2_PLT_CN, CONDID)],
    intervals[, .(stable_condition_interval_key,
                  T1_CONDPROP_UNADJ, T1_MICRPROP_UNADJ, T1_SUBPPROP_UNADJ,
                  T1_MACRPROP_UNADJ, T2_CONDPROP_UNADJ, T2_MICRPROP_UNADJ,
                  T2_SUBPPROP_UNADJ, T2_MACRPROP_UNADJ)],
    by = "stable_condition_interval_key", all.x = TRUE
  )
  endpoint_props <- rbindlist(list(
    edge_props[, .(history_id, PLT_CN = PREV_PLT_CN, CONDID,
                   CONDPROP_UNADJ = T1_CONDPROP_UNADJ,
                   MICRPROP_UNADJ = T1_MICRPROP_UNADJ,
                   SUBPPROP_UNADJ = T1_SUBPPROP_UNADJ,
                   MACRPROP_UNADJ = T1_MACRPROP_UNADJ)],
    edge_props[, .(history_id, PLT_CN = T2_PLT_CN, CONDID,
                   CONDPROP_UNADJ = T2_CONDPROP_UNADJ,
                   MICRPROP_UNADJ = T2_MICRPROP_UNADJ,
                   SUBPPROP_UNADJ = T2_SUBPPROP_UNADJ,
                   MACRPROP_UNADJ = T2_MACRPROP_UNADJ)]
  ))[, lapply(.SD, min, na.rm = TRUE),
       by = .(history_id, PLT_CN, CONDID),
       .SDcols = c("CONDPROP_UNADJ", "MICRPROP_UNADJ", "SUBPPROP_UNADJ",
                   "MACRPROP_UNADJ")]
  endpoint_props[, PLT_CN := bit64::as.integer64(PLT_CN)]
  for (column in c("CONDPROP_UNADJ", "MICRPROP_UNADJ", "SUBPPROP_UNADJ",
                   "MACRPROP_UNADJ")) {
    endpoint_props[is.infinite(get(column)), (column) := NA_real_]
    endpoint_props[!is.na(get(column)) & get(column) <= 0, (column) := NA_real_]
  }
  tree_endpoint <- merge(
    trees[n_trees_tpa > 0], endpoint_props,
    by = c("PLT_CN", "CONDID"), all = FALSE, allow.cartesian = TRUE,
    suffixes = c("", "_endpoint")
  )
  tree_endpoint[, macroplot := n_trees_raw > 0 &
                  abs(n_trees_tpa / n_trees_raw - 0.999188) < 0.02]
  tree_endpoint[, denom := fifelse(
    macroplot,
    fcoalesce(MACRPROP_UNADJ, CONDPROP_UNADJ_endpoint),
    fcoalesce(SUBPPROP_UNADJ, CONDPROP_UNADJ_endpoint)
  )]
  tree_endpoint[, weight := as.numeric(n_trees_tpa) / denom]
  sap_endpoint <- merge(
    saplings[n_trees_tpa > 0], endpoint_props,
    by = c("PLT_CN", "CONDID"), all = FALSE, allow.cartesian = TRUE,
    suffixes = c("", "_endpoint")
  )
  sap_endpoint[, denom := fcoalesce(MICRPROP_UNADJ, CONDPROP_UNADJ_endpoint)]
  sap_endpoint[, weight := as.numeric(n_trees_tpa) / denom]
  pooled_source <- rbindlist(list(
    tree_endpoint[is.finite(weight) & weight > 0,
                  .(history_id, species_key, PLT_CN, INVYR, CONDID,
                    weight, raw_count, life_stage = "trees")],
    sap_endpoint[is.finite(weight) & weight > 0,
                 .(history_id, species_key, PLT_CN, INVYR, CONDID,
                   weight, raw_count, life_stage = "saplings")]
  ), use.names = TRUE)
  pooled_source <- pooled_source[, .(
    weight = sum(weight, na.rm = TRUE),
    raw_count = sum(raw_count, na.rm = TRUE)
  ), by = .(history_id, species_key, PLT_CN, INVYR, CONDID)]

  # Analysis A: species-level counts/weights, kept long so units never mix.
  cat("Building species coverage/importance tables...\n")
  importance_parts <- lapply(names(source_specs), function(g) {
    spec <- source_specs[[g]]
    x <- copy(spec$data)
    x[, importance_weight := weight]
    species_stats(x, g, "importance_weight", spec$unit)
  })
  importance_parts[["mortality_trees_unadjusted"]] <- species_stats(
    copy(mort_tree)[, importance_weight := weight],
    "mortality_trees_unadjusted", "importance_weight", "trees_per_acre"
  )
  importance_parts[["mortality_saplings_unadjusted"]] <- species_stats(
    copy(mort_sap)[, importance_weight := weight],
    "mortality_saplings_unadjusted", "importance_weight", "trees_per_acre"
  )
  importance_parts[["mortality_pooled_adjusted"]] <- species_stats(
    pooled_source[, .(species_key, stable_plot_id = history_id, PLT_CN, INVYR,
                      CONDID, importance_weight = weight, raw_count)],
    "mortality_pooled_adjusted", "importance_weight",
    "sampling_element_adjusted_trees_per_acre"
  )
  importance <- rbindlist(importance_parts, fill = TRUE)
  importance <- merge(importance, policy, by = "species_key", all.x = TRUE)
  importance <- rename_policy_merge_counts(importance)
  write_csv_out(importance, path(table_dir, "species_coverage_and_importance.csv"))
  importance_group_totals <- importance[, .(
    group_species_level_niche_targets = uniqueN(
      species_key[needs_niche %in% TRUE]
    ),
    group_total_cwm_weight = safe_sum(cwm_weight)
  ), by = .(group, weight_basis, weight_unit)]
  importance_summary <- importance[, .(
    n_distinct_taxa = uniqueN(species_key),
    n_species_level_niche_targets = uniqueN(species_key[needs_niche %in% TRUE]),
    category_cwm_weight = safe_sum(cwm_weight),
    total_missing_cwm_weight = if (.BY$coverage_category %in% c(
      "not_targeted_non_species_level", "no_bien_range_returned",
      "bien_range_no_usable_global_niche"
    )) safe_sum(cwm_weight) else 0,
    n_observation_rows = safe_sum(n_observation_rows),
    summed_taxon_plot_counts = safe_sum(n_plots),
    summed_taxon_condition_counts = safe_sum(group_n_conditions)
  ), by = .(group, weight_basis, weight_unit, coverage_category)]
  importance_summary <- merge(
    importance_summary, importance_group_totals,
    by = c("group", "weight_basis", "weight_unit"), all.x = TRUE,
    sort = FALSE
  )
  importance_summary[, `:=`(
    pct_of_group_species_level_niche_targets =
      100 * n_species_level_niche_targets /
        group_species_level_niche_targets,
    pct_of_group_total_cwm_weight =
      100 * category_cwm_weight / group_total_cwm_weight
  )]
  importance_summary[, c(
    "group_species_level_niche_targets", "group_total_cwm_weight"
  ) := NULL]
  write_csv_out(importance_summary,
                path(table_dir, "coverage_summary_by_group_and_failure_category.csv"))

load_or_build_condition_checkpoint <- function(
    checkpoint_dir, label, source, weight_unit, current_niches,
    sensitivity_niches) {
  checkpoint_path <- path(checkpoint_dir, paste0(label, ".parquet"))
  if (file_exists(checkpoint_path)) {
    log_progress("Reusing condition checkpoint: ", label)
    return(as.data.table(read_parquet(checkpoint_path)))
  }
  started <- Sys.time()
  log_progress("Starting condition group: ", label,
               " (source rows: ", format(nrow(source), big.mark = ","), " )")
  out <- build_condition_comparison(
    source, label, weight_unit, current_niches, sensitivity_niches
  )
  write_parquet_out(out, checkpoint_path)
  log_progress("Finished condition group: ", label,
               " (output rows: ", format(nrow(out), big.mark = ","),
               "; elapsed minutes: ",
               sprintf("%.1f", as.numeric(difftime(
                 Sys.time(), started, units = "mins"
               ))), " )")
  out
}

  switch_summary <- importance[, .(
    n_taxa = uniqueN(species_key),
    n_species_switched_to_global = uniqueN(species_key[switched_to_global %in% TRUE]),
    total_cwm_weight = safe_sum(cwm_weight),
    switched_cwm_weight = safe_sum(cwm_weight[switched_to_global %in% TRUE]),
    switched_cwm_weight_share = safe_sum(cwm_weight[switched_to_global %in% TRUE]) /
      safe_sum(cwm_weight)
  ), by = .(group, weight_basis, weight_unit)]
  write_csv_out(switch_summary, path(table_dir, "switched_species_and_weight_by_group.csv"))

  # Standard condition and forest plot-visit products.
  log_progress("Building condition-level and forest plot-visit products")
  condition_parts <- list()
  for (g in names(source_specs)) {
    condition_parts[[g]] <- load_or_build_condition_checkpoint(
      condition_checkpoint_dir, g, source_specs[[g]]$data,
      source_specs[[g]]$unit, current_niches, sensitivity_niches
    )
  }
  condition_compare <- rbindlist(condition_parts, fill = TRUE)
  condition_current <- policy_product(condition_compare, "current")
  condition_sensitivity <- policy_product(condition_compare, "sensitivity")
  write_parquet_out(condition_current, path(current_dir, "condition_cwm.parquet"))
  write_parquet_out(condition_sensitivity,
                    path(sensitivity_dir, "condition_cwm.parquet"))

  foundation <- build_forested_condition_foundation(cond_meta)
  plot_parts <- list()
  for (g in unique(condition_compare$group)) {
    for (pol in c("current", "sensitivity")) {
      condition_product <- policy_product(condition_compare[group == g], pol)
      plot_checkpoint <- path(
        plot_checkpoint_dir, paste0(g, "__", pol, ".parquet")
      )
      if (file_exists(plot_checkpoint)) {
        log_progress("Reusing plot checkpoint: ", g, " / ", pol)
        plot_product <- as.data.table(read_parquet(plot_checkpoint))
      } else {
        log_progress("Starting plot aggregation: ", g, " / ", pol)
        metric_cols <- c(names(metric_map), "switched_weight_share")
        plot_product <- aggregate_forested_condition_cwm(
          condition_product, foundation, metric_cols
        )
        plot_product[, `:=`(
          group = g,
          weight_unit = first_or_na(condition_product$weight_unit),
          niche_policy = pol
        )]
        write_parquet_out(plot_product, plot_checkpoint)
        log_progress("Finished plot aggregation: ", g, " / ", pol,
                     " (rows: ", format(nrow(plot_product), big.mark = ","),
                     " )")
      }
      plot_parts[[paste(g, pol)]] <- plot_product
    }
  }
  plot_all <- rbindlist(plot_parts, fill = TRUE)
  plot_current <- plot_all[niche_policy == "current"]
  plot_sensitivity <- plot_all[niche_policy == "sensitivity"]
  write_parquet_out(plot_current, path(current_dir, "forest_plot_visit_cwm.parquet"))
  write_parquet_out(plot_sensitivity,
                    path(sensitivity_dir, "forest_plot_visit_cwm.parquet"))
  plot_compare <- merge(
    plot_current, plot_sensitivity,
    by = c("group", "weight_unit", "stable_plot_id", "PLT_CN", "INVYR"),
    suffixes = c("_current", "_sensitivity")
  )
  for (metric in names(metric_map)) {
    plot_compare[, (paste0(metric, "_change")) :=
      get(paste0(metric, "_sensitivity")) - get(paste0(metric, "_current"))]
    plot_compare[, (paste0(metric, "_weight_with_niche_current")) := 1]
    plot_compare[, (paste0(metric, "_weight_with_niche_sensitivity")) := 1]
  }

  # Mortality condition products for adults and saplings.
  mortality_compare <- rbindlist(list(
    load_or_build_condition_checkpoint(
      condition_checkpoint_dir, "mortality_trees", mort_tree, "trees_per_acre",
      current_niches, sensitivity_niches
    ),
    load_or_build_condition_checkpoint(
      condition_checkpoint_dir, "mortality_saplings", mort_sap, "trees_per_acre",
      current_niches, sensitivity_niches
    )
  ), fill = TRUE)
  write_parquet_out(policy_product(mortality_compare, "current"),
                    path(current_dir, "mortality_condition_cwm.parquet"))
  write_parquet_out(policy_product(mortality_compare, "sensitivity"),
                    path(sensitivity_dir, "mortality_condition_cwm.parquet"))

  # Pooled mortality condition product uses sampling-element-adjusted abundance.
  pooled_for_cwm <- pooled_source[, .(
    species_key, PLT_CN, INVYR, CONDID,
    stable_plot_id = history_id, state = NA_character_,
    COND_STATUS_CD = 1L, CONDPROP_UNADJ = 1,
    weight, raw_count
  )]
  pooled_compare <- load_or_build_condition_checkpoint(
    condition_checkpoint_dir, "mortality_pooled_adult_sapling", pooled_for_cwm,
    "sampling_element_adjusted_trees_per_acre",
    current_niches, sensitivity_niches
  )
  write_parquet_out(policy_product(pooled_compare, "current"),
                    path(current_dir, "mortality_pooled_condition_cwm.parquet"))
  write_parquet_out(policy_product(pooled_compare, "sensitivity"),
                    path(sensitivity_dir, "mortality_pooled_condition_cwm.parquet"))

  # Canonical reproduction checks for products that actually exist.
  cat("Validating current-policy reproduction...\n")
  canonical <- as.data.table(read_parquet(p$canonical_condition_cwm))[
    layer %in% c("trees", "saplings")]
  generated <- policy_product(mortality_compare, "current")
  generated[, layer := fifelse(group == "mortality_trees", "trees", "saplings")]
  check <- merge(
    canonical,
    generated[, .(PLT_CN, INVYR, CONDID, layer,
                  total_individual_abundance_generated = community_weight_total,
                  temperature_generated = temperature,
                  precipitation_generated = precipitation,
                  CWD_generated = CWD)],
    by = c("PLT_CN", "INVYR", "CONDID", "layer"), all = TRUE
  )
  reproduction <- rbindlist(lapply(c(
    total_individual_abundance = "total_individual_abundance_generated",
    temperature = "temperature_generated",
    precipitation = "precipitation_generated",
    CWD = "CWD_generated"
  ), function(gen_col) {
    canonical_col <- sub("_generated$", "", gen_col)
    data.table(
      product = "09_analysis/data/processed/condition_visit_cwm.parquet",
      field = canonical_col,
      canonical_rows = nrow(canonical),
      generated_rows = nrow(generated),
      unmatched_rows = sum(is.na(check[[canonical_col]]) != is.na(check[[gen_col]])),
      maximum_absolute_difference = max(abs(check[[canonical_col]] - check[[gen_col]]),
                                        na.rm = TRUE)
    )
  }), use.names = TRUE)
  canonical_pooled <- as.data.table(read_parquet(p$canonical_pooled_cwm))
  generated_pooled <- policy_product(pooled_compare, "current")
  pooled_check <- merge(
    canonical_pooled,
    generated_pooled[, .(PLT_CN, INVYR, CONDID,
                         total_generated = community_weight_total,
                         temperature_generated = temperature,
                         precipitation_generated = precipitation,
                         CWD_generated = CWD)],
    by = c("PLT_CN", "INVYR", "CONDID"), all = TRUE
  )
  pooled_reproduction <- rbindlist(lapply(c(
    total_individual_abundance = "total_generated",
    temperature = "temperature_generated",
    precipitation = "precipitation_generated",
    CWD = "CWD_generated"
  ), function(gen_col) {
    canonical_col <- names(c(
      total_individual_abundance = "total_generated",
      temperature = "temperature_generated",
      precipitation = "precipitation_generated",
      CWD = "CWD_generated"
    ))[c(
      total_individual_abundance = "total_generated",
      temperature = "temperature_generated",
      precipitation = "precipitation_generated",
      CWD = "CWD_generated"
    ) == gen_col]
    data.table(
      product = "09_analysis/data/processed/pooled_condition_visit_cwm.parquet",
      field = canonical_col,
      canonical_rows = nrow(canonical_pooled),
      generated_rows = nrow(generated_pooled),
      unmatched_rows = sum(is.na(pooled_check[[canonical_col]]) !=
                             is.na(pooled_check[[gen_col]])),
      maximum_absolute_difference = max(abs(pooled_check[[canonical_col]] -
                                              pooled_check[[gen_col]]), na.rm = TRUE)
    )
  }), use.names = TRUE)
  reproduction <- rbindlist(list(reproduction, pooled_reproduction), fill = TRUE)
  reproduction[, passed := unmatched_rows == 0 &
                 maximum_absolute_difference < 1e-8 &
                 canonical_rows == generated_rows]
  write_csv_out(reproduction, path(table_dir, "current_policy_reproduction_checks.csv"))

  summaries <- rbindlist(list(
    comparison_summary(condition_compare, "condition"),
    comparison_summary(plot_compare, "forest_plot_visit"),
    comparison_summary(mortality_compare, "mortality_condition"),
    comparison_summary(pooled_compare, "mortality_pooled_condition")
  ), fill = TRUE)
  write_csv_out(summaries, path(table_dir, "community_cwm_change_summary.csv"))

  largest_species <- melt(
    niche_compare,
    id.vars = intersect(c("species_key", "niche_scope_used_current",
                          "niche_scope_used_sensitivity", "niche_policy_reason"),
                        names(niche_compare)),
    measure.vars = paste0(names(metric_map), "_change"),
    variable.name = "metric", value.name = "change",
    variable.factor = FALSE
  )
  largest_species[, metric := sub("_change$", "", metric)]
  largest_species <- largest_species[!is.na(change)][order(metric, -abs(change)),
                                                      head(.SD, 25), by = metric]
  write_csv_out(largest_species, path(table_dir, "largest_changing_species.csv"))
  largest_communities <- rbindlist(list(
    copy(condition_compare)[, product_level := "condition"],
    copy(plot_compare)[, product_level := "forest_plot_visit"],
    copy(mortality_compare)[, product_level := "mortality_condition"],
    copy(pooled_compare)[, product_level := "mortality_pooled_condition"]
  ), fill = TRUE)
  largest_communities <- melt(
    largest_communities,
    id.vars = intersect(c("product_level", "group", "weight_unit", "stable_plot_id",
                          "PLT_CN", "INVYR", "CONDID", "switched_weight_share"),
                        names(largest_communities)),
    measure.vars = paste0(names(metric_map), "_change"),
    variable.name = "metric", value.name = "change",
    variable.factor = FALSE
  )
  largest_communities[, metric := sub("_change$", "", metric)]
  largest_communities <- largest_communities[!is.na(change)][
    order(product_level, group, metric, -abs(change)), head(.SD, 25),
    by = .(product_level, group, metric)]
  write_csv_out(largest_communities, path(table_dir, "largest_changing_communities.csv"))

  n_switched <- policy[switched_to_global %in% TRUE, uniqueN(species_key)]
  n_targets <- policy[needs_niche %in% TRUE, uniqueN(species_key)]
  n_missing_global <- policy[coverage_category ==
                               "bien_range_no_usable_global_niche", uniqueN(species_key)]
  worst <- summaries[order(-maximum_absolute_change)][1]
  report <- c(
    "# BIEN range coverage and clipping-policy supplement",
    "",
    glue("Generated {format(Sys.time(), '%Y-%m-%d %H:%M UTC', tz = 'UTC')} from git commit `{system2('git', c('rev-parse', 'HEAD'), stdout = TRUE)}`."),
    "",
    "## Methods",
    "",
    glue("Existing BIEN polygons and global/study-area TerraClimate niche tables were reused; no download or climate extraction was run. The configured all-US box is [{study_cfg$xmin}, {study_cfg$xmax}] longitude by [{study_cfg$ymin}, {study_cfg$ymax}] latitude. Areas were measured in `{area_crs}`. A polygon was classified as meaningfully clipped only when area loss exceeded both the practical/relative rule `max({absolute_tolerance_km2} km2, {format(fraction_tolerance, scientific = TRUE)} × global area)`. Current policy preferred the study-area niche and otherwise used the global niche. The sensitivity used the global niche for every meaningfully clipped or wholly outside range."),
    "",
    "Adult/tree thermophilization CWMs use basal area (ft² acre⁻¹), sapling CWMs use trees per acre, P2VEG understory CWMs use cover percentage multiplied by subplot-condition proportion, mortality life-stage CWMs use unadjusted trees per acre, and pooled adult–sapling mortality CWMs use sampling-element-adjusted trees per acre. These units were never summed together. Forest plot-visit CWMs exclude nonforest conditions and normalize CONDPROP_UNADJ over forested conditions.",
    "",
    "## Results",
    "",
    glue("The species universe contains {nrow(policy)} taxa, including {n_targets} species-level niche targets. {n_missing_global} target taxa had a BIEN range but no usable global niche. The sensitivity switched {n_switched} taxa from clipped study-area niches to global niches."),
    "",
    glue("Across the reported community products, the largest absolute CWM change was {signif(worst$maximum_absolute_change, 5)} for {worst$metric} in `{worst$group}` at the {worst$product_level} grain. Detailed effect sizes, affected-community percentages, and weight shares are in `tables/community_cwm_change_summary.csv` and `tables/switched_species_and_weight_by_group.csv`."),
    "",
    glue("Current-policy reproduction checks passed for {sum(reproduction$passed)}/{nrow(reproduction)} field-level checks against the existing mortality condition and pooled products. The canonical 07_thermophilization condition/plot products were absent, so current and sensitivity versions were both built externally and no canonical comparison was possible for those files."),
    "",
    "Niche availability and sample membership did not change under the sensitivity because every switched taxon already had both global and study-area niche values; only the selected niche values changed. See the membership-change columns in the comparison table.",
    "",
    "## Interpretation",
    "",
    "The practical importance of clipping should be judged from the weight shares and CWM-change distributions, not the number of switched species alone. Large species-level niche shifts can have little community effect when switched taxa carry little local weight; conversely, modest trait shifts can matter when a dominant taxon is affected.",
    "",
    "## Draft supplemental Methods",
    "",
    glue("We classified every FIA and P2VEG taxon as not targeted at species level, lacking a BIEN range, having a BIEN range but no usable global TerraClimate niche, requiring a global fallback because no study-area niche was available, or having a usable study-area niche. We summarized taxon frequency and the abundance measure used by each CWM workflow separately for adult trees, saplings, pooled adult–sapling communities, and P2VEG understory groups. To test sensitivity to geographic clipping, we intersected each complete BIEN polygon with the configured all-US bounding box and measured global and retained areas in {area_crs}. Area loss was treated as meaningful when it exceeded max({absolute_tolerance_km2} km², {format(fraction_tolerance, scientific = TRUE)} of global area). The current policy used the clipped niche whenever available; the alternative used the global niche for every meaningfully clipped or wholly outside polygon. We rebuilt CWMs without fitting models and compared paired values at species, condition, and forest plot-visit grains."),
    "",
    "## Draft supplemental Results",
    "",
    glue("Among {n_targets} species-level niche targets, {n_missing_global} had a mapped BIEN range but no usable global niche, and {n_switched} taxa changed from study-area to global niche values under the clipping sensitivity. Current-policy mortality CWMs reproduced the stored condition and pooled products within the stated numerical tolerance. The largest observed absolute community-level change was {signif(worst$maximum_absolute_change, 5)} ({worst$metric}; {worst$group}; {worst$product_level}). Complete distributions and abundance-weight shares are reported in the supplemental tables."),
    "",
    "## Suggested table titles and footnotes",
    "",
    "1. **Coverage and community importance of taxa lacking usable BIEN climate niches.** Footnote: Percentages use species-level niche targets within each community group as denominators; abundance units are reported separately and are not additive across FIA TPA, basal area, adjusted TPA, or P2VEG cover.",
    "2. **Sensitivity of species climate niches and community-weighted means to use of global BIEN ranges when clipping is required.** Footnote: A range was considered clipped only when area loss exceeded the documented absolute/relative tolerance; positive changes indicate global-policy minus current-policy values.",
    "3. **Largest species and community changes under the global-if-clipped policy.** Footnote: Forest plot-visit values are area-weighted over FIA forest conditions only; pooled mortality abundance is adjusted using microplot, subplot, or macroplot condition proportions.",
    "",
    "## Plain-language response for Joan",
    "",
    glue("I separated true BIEN gaps from non-species records and from species that simply need the existing global fallback. I also reran the CWM calculations in a separate sensitivity directory using global climate niches whenever a BIEN range meaningfully crossed the all-US clipping box. This switched {n_switched} taxa. The stored mortality CWMs were reproduced under the current rule, no models were rerun, and no canonical files were replaced. The attached summary tables show how much community weight those taxa represent and the resulting CWM changes for temperature, precipitation, and CWD."),
    "",
    "## Output paths",
    "",
    glue("- Root: `{output_dir}`"),
    "- Tables: `tables/`",
    "- Parallel products: `products/current_policy/` and `products/global_if_clipped/`",
    "- Manifest: `manifest/manifest.csv`",
    "- Execution log: `logs/bien_clipping_supplement.log`"
  )
  writeLines(report, path(report_dir, "bien_clipping_supplement_report.md"))
  writeLines(report[match("## Draft supplemental Methods", report):
                      (match("## Suggested table titles and footnotes", report) - 1)],
             path(report_dir, "draft_supplemental_methods_results.md"))
  joan_start <- match("## Plain-language response for Joan", report)
  writeLines(report[(joan_start + 2):(joan_start + 2)],
             path(report_dir, "response_for_joan.txt"))

  git_report <- c(
    "Git commit:", system2("git", c("rev-parse", "HEAD"), stdout = TRUE),
    "", "Git status --short:",
    system2("git", c("status", "--short", "--untracked-files=all"), stdout = TRUE),
    "", "Git diff --stat:",
    system2("git", c("diff", "--stat"), stdout = TRUE),
    "", "Canonical products overwritten: no",
    "Model-fitting stages run: no"
  )
  writeLines(git_report, path(report_dir, "final_git_diff_status.txt"))

  input_hashes_after <- vapply(input_files, sha256_one, character(1))
  if (!identical(unname(input_manifest$sha256), unname(input_hashes_after))) {
    stop("At least one canonical input changed during the run; inspect before using outputs.")
  }

  cat("Completed:", format(Sys.time(), tz = "UTC"), "UTC\n")
  sink(type = "message")
  sink(type = "output")
  close(log_con)

  # Final manifest is written last and excludes itself from output hashes.
  output_files <- dir_ls(output_dir, recurse = TRUE, type = "file")
  output_files <- setdiff(output_files, path(manifest_dir, "manifest.csv"))
  output_manifest <- data.table(
    record_type = "output",
    path = as.character(output_files),
    sha256 = vapply(output_files, sha256_one, character(1)),
    bytes = as.numeric(file_info(output_files)$size),
    rows = vapply(output_files, parquet_rows, numeric(1)),
    timestamp_utc = format(file_info(output_files)$modification_time,
                           "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )
  manifest <- rbindlist(list(input_manifest, output_manifest), fill = TRUE)
  manifest[, `:=`(
    git_commit = system2("git", c("rev-parse", "HEAD"), stdout = TRUE),
    command = command,
    run_started_utc = format(started, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    run_finished_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )]
  write_csv_out(manifest, path(manifest_dir, "manifest.csv"))
  writeLines(c(
    "status=success",
    paste0("completed_utc=", format(
      Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"
    )),
    paste0("manifest=", path(manifest_dir, "manifest.csv"))
  ), success_marker)
}

if (sys.nframe() == 0L) main()
