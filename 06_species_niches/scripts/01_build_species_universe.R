# ==============================================================================
# 01_build_species_universe.R
# Build the community species universe for the species niche workflow.
#
# This script unions species observed in FIA tree, sapling, seedling, and P2
# understory vegetation summaries. It writes the canonical species list consumed
# by the BIEN range-map workflow.
#
# Usage:
#   Rscript 06_species_niches/scripts/01_build_species_universe.R
#   Rscript 06_species_niches/scripts/01_build_species_universe.R --limit=100
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(fs)
  library(tibble)
})

source(here("scripts/utils/load_config.R"))

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", flag, "="), "", hit[[1]])
}

has_flag <- function(flag) flag %in% args

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
is_smoke_run <- !is.na(limit_arg)

config <- load_config()
niche_config <- config$processed$species_niches
fia_summary_config <- config$processed$fia$summaries

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

processed_dir <- here(niche_config$output_dir)
smoke_data_dir <- here("06_species_niches/data/smoke")
qa_dir <- if (is_smoke_run) here("06_species_niches/qa/smoke") else here("06_species_niches/qa/outputs")
metadata_dir <- here("06_species_niches/metadata/processed")

# Smoke runs are intentionally separated from production outputs so local test
# history stays useful without cluttering the main processed/QA folders.
species_universe_path <- file.path(
  if (is_smoke_run) smoke_data_dir else processed_dir,
  niche_config$files$species_universe
)
qa_suffix <- ""

if (is_smoke_run) {
  species_universe_path <- file.path(
    smoke_data_dir,
    sprintf("species_universe_limit_%d.parquet", limit_arg)
  )
  qa_suffix <- sprintf("_limit_%d", limit_arg)
}

dir_create(processed_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)
dir_create(metadata_dir)

summary_dir <- here(fia_summary_config$output_dir)
tree_species_path <- file.path(summary_dir, "plot_tree_species.parquet")
seedling_species_path <- file.path(summary_dir, "plot_seedling_species.parquet")
understory_species_path <- file.path(summary_dir, "plot_understory_species.parquet")
understory_veg_dir <- here(config$processed$fia$understory_veg$output_dir %||% "05_fia/data/processed/understory_veg")

clean_text <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x
}

first_nonmissing <- function(x) {
  x <- clean_text(x)
  out <- x[!is.na(x)][1]
  if (length(out) == 0) NA_character_ else out
}

collapse_unique <- function(x) {
  x <- clean_text(x)
  x <- sort(unique(x[!is.na(x)]))
  if (length(x) == 0) NA_character_ else paste(x, collapse = ";")
}

split_binomial <- function(scientific_name) {
  parts <- strsplit(clean_text(scientific_name), "\\s+")
  genus <- vapply(parts, function(x) if (length(x) >= 1) x[1] else NA_character_, character(1))
  epithet <- vapply(parts, function(x) if (length(x) >= 2) x[2] else NA_character_, character(1))
  list(genus = genus, specific_epithet = epithet)
}

is_pseudo_or_aggregate_taxon <- function(scientific_name, source_species_code = NULL,
                                         flag_p2_aggregate_code = FALSE) {
  name <- tolower(clean_text(scientific_name))
  out <- is.na(name) |
    grepl("\\b(sp|spp|ssp|var)\\.?\\b", name) |
    grepl("\\bunknown\\b|\\bunidentified\\b|\\bother\\b|\\bhybrid\\b|\\bgroup\\b|\\bcomplex\\b", name) |
    grepl("\\bx\\b|\\b×\\b", name)

  if (!is.null(source_species_code)) {
    code <- toupper(clean_text(source_species_code))
    flag <- rep_len(flag_p2_aggregate_code, length(out))
    out <- out | (flag & grepl("SPP$|SP$", code))
  }

  out[is.na(out)] <- TRUE
  out
}

standardize_tree_species <- function(path) {
  if (!file.exists(path)) return(tibble())

  dt <- as.data.table(read_parquet(path))

  if (!"community_layer" %in% names(dt)) {
    dt[, community_layer := fifelse(
      grepl("sapling", size_classes_present %||% "", ignore.case = TRUE),
      "sapling",
      "tree"
    )]
  }

  dt[, source_code_system := "fia_spcd"]
  dt[, source_species_code := as.character(SPCD)]
  dt[, species_key := paste0("fia_spcd:", source_species_code)]
  dt[, scientific_name := SCIENTIFIC_NAME]
  dt[, common_name := COMMON_NAME]
  dt[, source_table := "TREE"]
  dt[, growth_habit := "tree"]
  dt[, abundance_for_universe := as.numeric(abundance_for_cwm %||% ba_per_acre %||% n_trees_tpa)]
  dt[, community_layer := fifelse(community_layer %in% c("sapling", "tree"), community_layer, "tree")]

  base <- dt[, .(
    species_key,
    source_code_system,
    source_species_code,
    scientific_name,
    common_name,
    source_table,
    community_layer,
    growth_habit,
    stable_plot_id,
    INVYR,
    PLT_CN,
    CONDID,
    STATECD,
    state,
    abundance_for_universe
  )]

  # The tree summary can contain sapling-sized trees in size_classes_present
  # even when the main row's downstream layer is "tree". Add a companion
  # sapling row so the species universe records sapling participation.
  if ("size_classes_present" %in% names(dt)) {
    saplings <- copy(dt[grepl("sapling", size_classes_present, ignore.case = TRUE)])
    if (nrow(saplings) > 0) {
      saplings[, community_layer := "sapling"]
      saplings <- saplings[, names(base), with = FALSE]
      base <- rbindlist(list(base, saplings), fill = TRUE)
    }
  }

  unique(base)
}

standardize_seedling_species <- function(path) {
  if (!file.exists(path)) return(tibble())

  dt <- as.data.table(read_parquet(path))
  dt[, source_code_system := "fia_spcd"]
  dt[, source_species_code := as.character(SPCD)]
  dt[, species_key := paste0("fia_spcd:", source_species_code)]
  dt[, scientific_name := SCIENTIFIC_NAME]
  dt[, common_name := COMMON_NAME]
  dt[, source_table := "SEEDLING"]
  dt[, community_layer := "seedling"]
  dt[, growth_habit := "tree"]
  dt[, abundance_for_universe := as.numeric(seedlings_tpa %||% treecount_total %||% treecount_calc_total)]

  dt[, .(
    species_key,
    source_code_system,
    source_species_code,
    scientific_name,
    common_name,
    source_table,
    community_layer,
    growth_habit,
    stable_plot_id,
    INVYR,
    PLT_CN,
    CONDID,
    STATECD,
    state,
    abundance_for_universe
  )]
}

standardize_understory_species <- function(summary_path, raw_dir) {
  if (file.exists(summary_path)) {
    dt <- as.data.table(read_parquet(summary_path))
  } else if (dir_exists(raw_dir)) {
    dt <- as.data.table(open_dataset(raw_dir) |> collect())
  } else {
    return(tibble())
  }

  symbol_col <- intersect(c("accepted_symbol", "plant_symbol", "VEG_SPCD"), names(dt))[1]
  if (is.na(symbol_col)) stop("Understory data lacks accepted_symbol, plant_symbol, or VEG_SPCD.")

  dt[, source_code_system := "nrcs_plants_symbol"]
  dt[, source_species_code := as.character(.SD[[1]]), .SDcols = symbol_col]
  dt[, species_key := paste0("nrcs_plants_symbol:", source_species_code)]
  dt[, scientific_name := clean_text(scientific_name)]
  dt[, common_name := clean_text(common_name)]
  dt[, source_table := "P2VEG_SUBPLOT_SPP"]

  if (!"growth_habit" %in% names(dt)) dt[, growth_habit := plant_growth_habit %||% NA_character_]
  if (!"community_layer" %in% names(dt)) dt[, community_layer := NA_character_]

  dt[, growth_habit := tolower(clean_text(growth_habit))]
  dt[, community_layer := tolower(clean_text(community_layer))]
  dt[, community_layer := fcase(
    grepl("shrub", growth_habit, ignore.case = TRUE), "shrub",
    grepl("forb|herb", growth_habit, ignore.case = TRUE), "forb",
    grepl("graminoid|grass|sedge|rush", growth_habit, ignore.case = TRUE), "graminoid",
    grepl("tree", growth_habit, ignore.case = TRUE), "p2veg_tree_layer",
    !is.na(community_layer), community_layer,
    default = "unknown"
  )]

  abundance_col <- intersect(c("cover_pct_subpcond", "cover_pct", "abundance_for_cwm"), names(dt))[1]
  dt[, abundance_for_universe := if (!is.na(abundance_col)) as.numeric(.SD[[1]]) else NA_real_, .SDcols = abundance_col]

  dt[, .(
    species_key,
    source_code_system,
    source_species_code,
    scientific_name,
    common_name,
    source_table,
    community_layer,
    growth_habit,
    stable_plot_id,
    INVYR,
    PLT_CN,
    CONDID,
    STATECD,
    state,
    abundance_for_universe
  )]
}

cat("Species Universe Build\n")
cat("======================\n\n")

tree_rows <- standardize_tree_species(tree_species_path)
seedling_rows <- standardize_seedling_species(seedling_species_path)
understory_rows <- standardize_understory_species(understory_species_path, understory_veg_dir)

all_rows <- bind_rows(tree_rows, seedling_rows, understory_rows)
if (nrow(all_rows) == 0) stop("No species rows found from FIA summaries.")

if (!is.na(limit_arg)) {
  keep_keys <- unique(all_rows$species_key)[seq_len(min(limit_arg, length(unique(all_rows$species_key))))]
  all_rows <- all_rows |> filter(species_key %in% keep_keys)
}

all_rows <- as.data.table(all_rows)
all_rows[, scientific_name := clean_text(scientific_name)]
all_rows[, common_name := clean_text(common_name)]
all_rows[, source_species_code := clean_text(source_species_code)]
all_rows[, state_token := clean_text(fifelse(!is.na(state), state, as.character(STATECD)))]

universe <- all_rows[, .(
  source_code_system = first_nonmissing(source_code_system),
  source_species_code = first_nonmissing(source_species_code),
  scientific_name = first_nonmissing(scientific_name),
  common_name = first_nonmissing(common_name),
  source_tables = collapse_unique(source_table),
  community_layers = collapse_unique(community_layer),
  growth_habits = collapse_unique(growth_habit),
  states_present = collapse_unique(state_token),
  n_states = uniqueN(state_token, na.rm = TRUE),
  n_plot_visits = uniqueN(paste(stable_plot_id, INVYR, sep = "_"), na.rm = TRUE),
  n_conditions = uniqueN(paste(PLT_CN, CONDID, sep = "_"), na.rm = TRUE),
  n_source_rows = .N,
  abundance_total = sum(abundance_for_universe, na.rm = TRUE),
  in_seedlings = any(community_layer == "seedling", na.rm = TRUE),
  in_saplings = any(community_layer == "sapling", na.rm = TRUE),
  in_trees = any(community_layer == "tree", na.rm = TRUE),
  in_shrubs = any(community_layer == "shrub", na.rm = TRUE),
  in_forbs = any(community_layer == "forb", na.rm = TRUE),
  in_graminoids = any(community_layer == "graminoid", na.rm = TRUE),
  in_p2veg_tree_layers = any(community_layer == "p2veg_tree_layer", na.rm = TRUE)
), by = species_key]

name_parts <- split_binomial(universe$scientific_name)
universe[, genus := name_parts$genus]
universe[, specific_epithet := name_parts$specific_epithet]
universe[, is_pseudo_taxon := is_pseudo_or_aggregate_taxon(
  scientific_name,
  source_species_code,
  flag_p2_aggregate_code = source_code_system == "nrcs_plants_symbol"
)]
universe[, has_scientific_name := !is.na(scientific_name)]
universe[, needs_niche := has_scientific_name & !is_pseudo_taxon]

setcolorder(universe, c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "common_name", "genus", "specific_epithet",
  "source_tables", "community_layers", "growth_habits", "states_present",
  "n_states", "n_plot_visits", "n_conditions", "n_source_rows",
  "abundance_total", "in_seedlings", "in_saplings", "in_trees",
  "in_shrubs", "in_forbs", "in_graminoids", "in_p2veg_tree_layers",
  "is_pseudo_taxon", "has_scientific_name", "needs_niche"
))
setorder(universe, source_code_system, scientific_name, species_key)

write_parquet(as_tibble(universe), species_universe_path, compression = "snappy")

layer_counts <- universe[, .(
  n_species = .N,
  n_needs_niche = sum(needs_niche),
  n_pseudo_taxa = sum(is_pseudo_taxon)
), by = .(source_code_system)]
fwrite(layer_counts, file.path(qa_dir, paste0("species_universe_layer_counts", qa_suffix, ".csv")))

metrics <- data.table(
  metric = c("n_species", "n_needs_niche", "n_pseudo_taxa"),
  value = c(nrow(universe), sum(universe$needs_niche), sum(universe$is_pseudo_taxon))
)
fwrite(metrics, file.path(qa_dir, paste0("species_universe_metrics", qa_suffix, ".csv")))

pseudo_taxa <- universe[is_pseudo_taxon == TRUE]
fwrite(pseudo_taxa, file.path(qa_dir, paste0("species_universe_pseudo_taxa", qa_suffix, ".csv")))

metadata_script <- here("scripts/utils/parquet_metadata.R")
if (file.exists(metadata_script) && is.na(limit_arg)) {
  source(metadata_script)
  write_parquet_metadata(species_universe_path, sample_size = Inf)
}

cat("Done.\n")
cat(glue("Species universe: {species_universe_path}"), "\n")
cat(glue("Species:          {format(nrow(universe), big.mark = ',')}"), "\n")
cat(glue("Needs niche:      {format(sum(universe$needs_niche), big.mark = ',')}"), "\n")
