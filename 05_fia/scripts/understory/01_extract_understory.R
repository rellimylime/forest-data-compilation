# ==============================================================================
# 06_extract_understory.R
# Download and extract FIA Phase 2 vegetation (P2VEG) understory products.
#
# This rebuilds the two understory state-partitioned products from the raw FIADB
# P2VEG tables, mirroring the extraction pattern of 03_extract_trees.R and
# 04_extract_seedlings_mortality.R. It exists because those products were present
# on disk with no producer in this repository (see docs/DATA_PRODUCTS.md).
#
# STRUCTURE (P2VEG_SUBP_STRUCTURE):
#   Percent cover of each growth-habit x height-layer on a subplot condition.
#   One output row per PLT_CN x INVYR x CONDID x SUBP x GROWTH_HABIT_CD x LAYER.
#
# SPECIES (P2VEG_SUBPLOT_SPP):
#   Percent cover of each recorded plant species (NRCS PLANTS symbol) on a
#   subplot condition. One output row per
#   PLT_CN x INVYR x CONDID x SUBP x VEG_FLDSPCD x UNIQUE_SP_NBR x LAYER.
#   Species attributes come from REF_PLANT_DICTIONARY (join on VEG_SPCD = SYMBOL).
#
# Both products scale field cover to the subplot-condition proportion from
# SUBP_COND: cover_pct_subpcond = cover_pct * subpcond_prop. Plot identity and
# public coordinates are taken from the condition extract (cond partitions), so
# 03_extract_trees.R must have been run first.
#
# The script downloads any missing raw inputs (the three P2VEG-related state
# tables plus REF_PLANT_DICTIONARY), then extracts. Downloading uses the same
# rFIA path as 01_download_fia.R.
#
# Usage:
#   Rscript 05_fia/scripts/understory/01_extract_understory.R
#   Rscript 05_fia/scripts/understory/01_extract_understory.R CO WY MT
#   Rscript 05_fia/scripts/understory/01_extract_understory.R --force
#   Rscript 05_fia/scripts/understory/01_extract_understory.R --output-dir=/tmp/staging
#   Rscript 05_fia/scripts/understory/01_extract_understory.R --no-download
#
# Output:
#   05_fia/data/processed/understory_structure/state={ST}/understory_structure_{ST}.parquet
#   05_fia/data/processed/understory_veg/state={ST}/understory_veg_{ST}.parquet
# ==============================================================================

source("scripts/utils/load_config.R")
source("scripts/utils/parquet_atomic.R")
config <- load_config()

suppressPackageStartupMessages({
  library(here)
  library(fs)
  library(glue)
  library(data.table)
  library(arrow)
  library(bit64)
  library(tibble)
})

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config <- config$raw$fia
raw_dir    <- here(fia_config$local_dir)
cond_dir   <- here(config$processed$fia$cond$output_dir)
out_struct <- here(config$processed$fia$understory_structure$output_dir)
out_veg    <- here(config$processed$fia$understory_veg$output_dir)
ref_dir    <- file.path(raw_dir, "REF")

args        <- commandArgs(trailingOnly = TRUE)
force_build <- "--force" %in% args
no_download <- "--no-download" %in% args

arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) == 0) return(default)
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}
# --output-dir stages both products under one directory; used for verification.
stage_dir <- arg_value("output-dir", NULL)
if (!is.null(stage_dir)) {
  out_struct <- file.path(here(stage_dir), "understory_structure")
  out_veg    <- file.path(here(stage_dir), "understory_veg")
}

state_args <- toupper(args[!startsWith(args, "--")])
states <- if (length(state_args) > 0) state_args else fia_config$states

invyr_min <- fia_config$invyr_min
invyr_max <- fia_config$invyr_max

# P2VEG state tables this extraction needs.
p2veg_tables <- c("SUBP_COND", "P2VEG_SUBP_STRUCTURE", "P2VEG_SUBPLOT_SPP")

cat("FIA Understory (P2VEG) Extraction\n")
cat("==================================\n\n")
cat(glue("Output (structure): {out_struct}\n"))
cat(glue("Output (species):   {out_veg}\n"))
cat(glue("States: {length(states)}   INVYR: {invyr_min}-{invyr_max}\n\n"))

# ------------------------------------------------------------------------------
# Recoding crosswalks (reverse-engineered from the pre-existing products and the
# FIADB P2VEG code definitions; kept explicit so the mapping is auditable).
# ------------------------------------------------------------------------------

# GROWTH_HABIT_CD -> growth_habit. Codes appear in both P2VEG tables.
growth_habit_map <- c(
  FB = "forb",       GR = "graminoid",  SH = "shrub_vine", SS = "subshrub",
  TT = "tally_tree", NT = "nontally_tree",
  LT = "large_tree", SD = "seedling_sapling_tree", ST = "tree_unknown_size",
  DS = "unknown",    FN = "unknown",    MB = "unknown"
)

# growth_habit -> community_layer.
community_layer_map <- c(
  forb = "forb", graminoid = "graminoid", shrub_vine = "shrub",
  subshrub = "subshrub", tally_tree = "p2veg_tally_tree",
  nontally_tree = "p2veg_nontally_tree", large_tree = "p2veg_large_tree",
  seedling_sapling_tree = "p2veg_seedling_sapling_tree",
  tree_unknown_size = "p2veg_tree_unknown_size", unknown = "unknown"
)

# LAYER -> layer_label (P2VEG height layers).
layer_label_map <- c(
  "1" = "0_2_ft", "2" = "2_6_ft", "3" = "6_16_ft",
  "4" = "16_32_ft", "5" = "32_plus_ft"
)

recode <- function(x, map) unname(map[as.character(x)])

# ------------------------------------------------------------------------------
# Ensure raw inputs exist (download the missing ones)
# ------------------------------------------------------------------------------

ensure_reference_dictionary <- function() {
  dest <- file.path(ref_dir, "REF_PLANT_DICTIONARY.csv")
  if (file_exists(dest)) return(invisible(TRUE))
  if (no_download) stop("REF_PLANT_DICTIONARY.csv missing and --no-download set.")
  dir_create(ref_dir)
  cat("Downloading REF_PLANT_DICTIONARY...\n")
  options(timeout = 3600)
  download.file(
    "https://apps.fs.usda.gov/fia/datamart/CSV/REF_PLANT_DICTIONARY.csv",
    destfile = dest, mode = "wb", quiet = TRUE
  )
  invisible(TRUE)
}

ensure_state_tables <- function(st) {
  state_dir <- file.path(raw_dir, st)
  expected  <- file.path(state_dir, glue("{st}_{p2veg_tables}.csv"))
  if (all(file_exists(expected))) return(TRUE)
  if (no_download) {
    cat(glue("  {st}: raw P2VEG tables missing and --no-download set - skipping\n"))
    return(FALSE)
  }
  if (!requireNamespace("rFIA", quietly = TRUE)) {
    stop("rFIA is required to download missing P2VEG tables. install.packages('rFIA')")
  }
  cat(glue("  {st}: downloading missing P2VEG tables...\n"))
  dir_create(state_dir)
  options(timeout = 3600)
  rFIA::getFIA(states = st, dir = state_dir, load = FALSE, tables = p2veg_tables)
  all(file_exists(expected))
}

ensure_reference_dictionary()

# ------------------------------------------------------------------------------
# Load REF_PLANT_DICTIONARY once — the species attribute source (join on SYMBOL).
# ------------------------------------------------------------------------------

plant_dict_raw <- fread(
  file.path(ref_dir, "REF_PLANT_DICTIONARY.csv"),
  select = c("SYMBOL", "NEW_SYMBOL", "SCIENTIFIC_NAME", "NEW_SCIENTIFIC_NAME",
             "COMMON_NAME", "CATEGORY", "FAMILY", "GROWTH_HABIT", "DURATION",
             "US_NATIVITY", "GENUS", "SPECIES"),
  colClasses = "character", showProgress = FALSE
)
# accepted_symbol / scientific_name follow the dictionary's own update columns:
# when a symbol is superseded, NEW_SYMBOL and NEW_SCIENTIFIC_NAME carry the
# accepted replacement. Every other descriptive field stays with the symbol's
# own row.
plant_dict_raw[, accepted_symbol := fifelse(
  !is.na(NEW_SYMBOL) & NEW_SYMBOL != "", NEW_SYMBOL, SYMBOL
)]
plant_dict_raw[, scientific_name_resolved := fifelse(
  !is.na(NEW_SCIENTIFIC_NAME) & NEW_SCIENTIFIC_NAME != "",
  NEW_SCIENTIFIC_NAME, SCIENTIFIC_NAME
)]
plant_dict <- plant_dict_raw[, .(
  VEG_SPCD = SYMBOL,
  plant_symbol = SYMBOL,
  accepted_symbol,
  scientific_name = scientific_name_resolved,
  common_name = COMMON_NAME,
  plant_category = CATEGORY,
  plant_family = FAMILY,
  plant_growth_habit = GROWTH_HABIT,
  plant_duration = DURATION,
  plant_us_nativity = US_NATIVITY,
  plant_genus = GENUS,
  plant_species = SPECIES
)]
# One row per symbol; dictionary occasionally repeats a symbol across historical
# rows, so keep the first deterministically.
setkey(plant_dict, VEG_SPCD)
plant_dict <- unique(plant_dict, by = "VEG_SPCD")

# ------------------------------------------------------------------------------
# Per-state condition metadata: plot identity and public coordinates.
# ------------------------------------------------------------------------------

read_cond_meta <- function(st) {
  cond_file <- file.path(cond_dir, glue("state={st}/cond_{st}.parquet"))
  if (!file_exists(cond_file)) return(NULL)
  meta <- as.data.table(read_parquet(cond_file))
  keep <- c("PLT_CN", "INVYR", "CONDID", "stable_plot_id", "STATECD",
            "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN", "LAT", "LON", "ELEV")
  meta <- meta[, intersect(keep, names(meta)), with = FALSE]
  # Condition grain is one row per PLT_CN x INVYR x CONDID.
  unique(meta, by = c("PLT_CN", "INVYR", "CONDID"))
}

read_subpcond <- function(st) {
  f <- file.path(raw_dir, st, glue("{st}_SUBP_COND.csv"))
  if (!file_exists(f)) return(NULL)
  sc <- fread(f, select = c("PLT_CN", "INVYR", "SUBP", "CONDID", "SUBPCOND_PROP"),
              integer64 = "integer64", showProgress = FALSE)
  setnames(sc, "SUBPCOND_PROP", "subpcond_prop")
  unique(sc, by = c("PLT_CN", "INVYR", "SUBP", "CONDID"))
}

# ------------------------------------------------------------------------------
# Extractors
# ------------------------------------------------------------------------------

STRUCT_ORDER <- c(
  "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
  "PREV_PLT_CN", "LAT", "LON", "ELEV", "CONDID", "SUBP", "subpcond_prop",
  "GROWTH_HABIT_CD", "growth_habit", "community_layer", "LAYER", "layer_label",
  "cover_pct", "cover_pct_subpcond", "n_p2veg_structure_records"
)

VEG_ORDER <- c(
  "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
  "PREV_PLT_CN", "LAT", "LON", "ELEV", "CONDID", "SUBP", "subpcond_prop",
  "VEG_FLDSPCD", "UNIQUE_SP_NBR", "VEG_SPCD", "GROWTH_HABIT_CD", "growth_habit",
  "community_layer", "LAYER", "layer_label", "plant_symbol", "accepted_symbol",
  "scientific_name", "common_name", "plant_category", "plant_family",
  "plant_growth_habit", "plant_duration", "plant_us_nativity", "plant_genus",
  "plant_species", "cover_pct", "cover_pct_subpcond", "n_p2veg_records"
)

# Force the exact stored dtypes so a rebuilt file is byte-comparable to the old.
cast_types <- function(dt) {
  int32 <- intersect(c("INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "ELEV",
                        "CONDID", "SUBP", "LAYER",
                        "n_p2veg_structure_records", "n_p2veg_records"), names(dt))
  for (c in int32) dt[, (c) := as.integer(get(c))]
  int64 <- intersect(c("PLT_CN", "PREV_PLT_CN"), names(dt))
  for (c in int64) dt[, (c) := as.integer64(get(c))]
  dbl <- intersect(c("LAT", "LON", "subpcond_prop", "cover_pct",
                     "cover_pct_subpcond"), names(dt))
  for (c in dbl) dt[, (c) := as.numeric(get(c))]
  dt
}

extract_structure <- function(st, meta, subp, out_file) {
  f <- file.path(raw_dir, st, glue("{st}_P2VEG_SUBP_STRUCTURE.csv"))
  if (!file_exists(f)) { cat("  structure source not found\n"); return(0L) }
  dt <- fread(f, select = c("PLT_CN", "INVYR", "SUBP", "CONDID",
                            "GROWTH_HABIT_CD", "LAYER", "COVER_PCT"),
              integer64 = "integer64", showProgress = FALSE)
  # P2VEG_SUBP_STRUCTURE is zero-padded: every growth-habit x layer combination
  # is recorded on every subplot condition, with COVER_PCT = 0 where the habit is
  # absent. Keep only positive cover so a row means a habit was actually present.
  dt <- dt[INVYR >= invyr_min & INVYR <= invyr_max & COVER_PCT > 0]
  if (nrow(dt) == 0) return(0L)

  dt[, cover_pct := as.numeric(COVER_PCT)]
  dt <- subp[dt, on = c("PLT_CN", "INVYR", "SUBP", "CONDID")]
  dt[, cover_pct_subpcond := cover_pct * subpcond_prop]

  # One row per structural grain; count collapsed source rows.
  agg <- dt[, .(
    subpcond_prop = subpcond_prop[1],
    cover_pct = sum(cover_pct, na.rm = TRUE),
    cover_pct_subpcond = sum(cover_pct_subpcond, na.rm = TRUE),
    n_p2veg_structure_records = .N
  ), by = .(PLT_CN, INVYR, CONDID, SUBP, GROWTH_HABIT_CD, LAYER)]

  agg[, growth_habit := recode(GROWTH_HABIT_CD, growth_habit_map)]
  agg[, community_layer := recode(growth_habit, community_layer_map)]
  agg[, layer_label := recode(LAYER, layer_label_map)]

  agg <- meta[agg, on = c("PLT_CN", "INVYR", "CONDID")]
  agg <- cast_types(agg)
  setcolorder(agg, STRUCT_ORDER)

  dir_create(dirname(out_file))
  write_parquet_atomic(as_tibble(agg[, ..STRUCT_ORDER]), out_file, compression = "snappy")
  nrow(agg)
}

extract_veg <- function(st, meta, subp, out_file) {
  f <- file.path(raw_dir, st, glue("{st}_P2VEG_SUBPLOT_SPP.csv"))
  if (!file_exists(f)) { cat("  species source not found\n"); return(0L) }
  dt <- fread(f, select = c("PLT_CN", "INVYR", "SUBP", "CONDID", "VEG_FLDSPCD",
                            "UNIQUE_SP_NBR", "VEG_SPCD", "GROWTH_HABIT_CD",
                            "LAYER", "COVER_PCT"),
              colClasses = list(character = c("VEG_FLDSPCD", "UNIQUE_SP_NBR",
                                              "VEG_SPCD", "GROWTH_HABIT_CD")),
              integer64 = "integer64", showProgress = FALSE)
  # Species records are only written when a species is present, so unlike the
  # structure table there are no zero-cover padding rows; the filter is a no-op
  # here but kept for parity and safety.
  dt <- dt[INVYR >= invyr_min & INVYR <= invyr_max & COVER_PCT > 0]
  if (nrow(dt) == 0) return(0L)

  dt[, cover_pct := as.numeric(COVER_PCT)]
  dt <- subp[dt, on = c("PLT_CN", "INVYR", "SUBP", "CONDID")]
  dt[, cover_pct_subpcond := cover_pct * subpcond_prop]

  agg <- dt[, .(
    VEG_SPCD = VEG_SPCD[1],
    GROWTH_HABIT_CD = GROWTH_HABIT_CD[1],
    subpcond_prop = subpcond_prop[1],
    cover_pct = sum(cover_pct, na.rm = TRUE),
    cover_pct_subpcond = sum(cover_pct_subpcond, na.rm = TRUE),
    n_p2veg_records = .N
  ), by = .(PLT_CN, INVYR, CONDID, SUBP, VEG_FLDSPCD, UNIQUE_SP_NBR, LAYER)]

  agg[, growth_habit := recode(GROWTH_HABIT_CD, growth_habit_map)]
  agg[, community_layer := recode(growth_habit, community_layer_map)]
  agg[, layer_label := recode(LAYER, layer_label_map)]

  # Species attributes from the PLANTS dictionary (join on the veg species code).
  agg <- plant_dict[agg, on = "VEG_SPCD"]

  agg <- meta[agg, on = c("PLT_CN", "INVYR", "CONDID")]
  agg <- cast_types(agg)
  setcolorder(agg, VEG_ORDER)

  dir_create(dirname(out_file))
  write_parquet_atomic(as_tibble(agg[, ..VEG_ORDER]), out_file, compression = "snappy")
  nrow(agg)
}

# ------------------------------------------------------------------------------
# Per-state loop
# ------------------------------------------------------------------------------

t_total <- Sys.time()
n_done <- 0; n_skipped <- 0; n_failed <- 0

for (i in seq_along(states)) {
  st <- states[i]
  struct_out <- file.path(out_struct, glue("state={st}/understory_structure_{st}.parquet"))
  veg_out    <- file.path(out_veg,    glue("state={st}/understory_veg_{st}.parquet"))

  if (!force_build && file_exists(struct_out) && file_exists(veg_out)) {
    cat(glue("[{i}/{length(states)}] {st}: output exists - skipping\n"))
    n_skipped <- n_skipped + 1
    next
  }

  if (!ensure_state_tables(st)) { n_failed <- n_failed + 1; next }

  meta <- read_cond_meta(st)
  if (is.null(meta)) {
    cat(glue("[{i}/{length(states)}] {st}: cond partition not found - run 03_extract_trees.R first - skipping\n"))
    n_failed <- n_failed + 1
    next
  }
  subp <- read_subpcond(st)
  if (is.null(subp)) {
    cat(glue("[{i}/{length(states)}] {st}: SUBP_COND not found - skipping\n"))
    n_failed <- n_failed + 1
    next
  }

  cat(glue("[{i}/{length(states)}] {st}:\n"))
  t_st <- Sys.time()
  tryCatch({
    ns <- extract_structure(st, meta, subp, struct_out)
    nv <- extract_veg(st, meta, subp, veg_out)
    elapsed <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  structure: {format(ns, big.mark=',')} rows | species: {format(nv, big.mark=',')} rows | {sprintf('%.1fs', elapsed)}\n"))
    n_done <- n_done + 1
  }, error = function(e) {
    warning(glue("  Error processing {st}: {e$message}"))
    n_failed <<- n_failed + 1
  })
  gc(verbose = FALSE)
}

elapsed_total <- as.numeric(difftime(Sys.time(), t_total, units = "mins"))
cat(glue("\n{strrep('=', 50)}\n"))
cat("Understory extraction complete.\n\n")
cat(glue("  Processed: {n_done} state(s)\n"))
cat(glue("  Skipped:   {n_skipped} state(s)\n"))
cat(glue("  Failed:    {n_failed} state(s)\n"))
cat(glue("  Time:      {sprintf('%.1f', elapsed_total)} min\n"))
