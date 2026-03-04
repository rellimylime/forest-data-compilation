# ==============================================================================
# 02_inspect_fia.R
# Inspect FIADB schema and generate lookup tables
#
# - Verifies that all required columns are present in a sample of state CSVs
# - Reads national REF_SPECIES and REF_FOREST_TYPE and writes them as parquet
#   to 05_fia/lookups/ for fast joining in subsequent scripts
# - Prints a quick summary of each state's table sizes
#
# Usage:
#   Rscript 05_fia/scripts/02_inspect_fia.R
#
# Output:
#   05_fia/lookups/ref_species.parquet
#   05_fia/lookups/ref_forest_type.parquet
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(arrow)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config  <- config$raw$fia
raw_dir     <- here(fia_config$local_dir)
lookup_dir  <- here("05_fia/lookups")
states      <- fia_config$states

dir_create(lookup_dir)

cat("FIA Schema Inspection\n")
cat("=====================\n\n")

# ------------------------------------------------------------------------------
# Required columns per table (minimum set used by downstream scripts)
# ------------------------------------------------------------------------------

required_cols <- list(
  TREE = c("CN", "PLT_CN", "CONDID", "SUBP", "SPCD", "STATUSCD",
           "DIA", "TPA_UNADJ", "CCLCD", "AGENTCD", "INVYR"),
  PLOT = c("CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
           "INVYR", "LAT", "LON", "ELEV"),
  COND = c("PLT_CN", "CONDID", "FORTYPCD", "COND_STATUS_CD",
           "CONDPROP_UNADJ", "INVYR"),
  SEEDLING = c("PLT_CN", "CONDID", "SUBP", "SPCD", "TREECOUNT", "INVYR"),
  TREE_GRM_COMPONENT = c("TRE_CN", "PLT_CN", "MICR_COMPONENT_AL_FOREST",
                          "MICR_TPAMORT_UNADJ_AL_FOREST")
)

# ------------------------------------------------------------------------------
# Step 1: Generate lookup parquets from national REF tables
# ------------------------------------------------------------------------------

cat("Step 1: Generating lookup parquets from REF tables\n")

ref_dir <- file.path(raw_dir, "REF")

# REF_SPECIES
ref_sp_path <- file.path(ref_dir, "REF_SPECIES.csv")
if (!file_exists(ref_sp_path)) {
  stop(glue("REF_SPECIES.csv not found at {ref_sp_path}. Run 01_download_fia.R first."))
}

ref_sp <- fread(ref_sp_path,
                select = c("SPCD", "COMMON_NAME", "GENUS", "SPECIES",
                           "SCIENTIFIC_NAME", "SFTWD_HRDWD", "WOODLAND",
                           "MAJOR_SPGRPCD", "JENKINS_SPGRPCD"))
# Standardize text fields
ref_sp[, SFTWD_HRDWD := toupper(trimws(SFTWD_HRDWD))]
ref_sp[, WOODLAND    := toupper(trimws(WOODLAND))]

out_path <- file.path(lookup_dir, "ref_species.parquet")
write_parquet(as_tibble(ref_sp), out_path)
cat(glue("  ref_species.parquet: {nrow(ref_sp)} species ({file_size(out_path)})\n"))
cat(glue("  SFTWD_HRDWD values: {paste(sort(unique(ref_sp$SFTWD_HRDWD)), collapse=', ')}\n"))
cat(glue("  WOODLAND values:    {paste(sort(unique(ref_sp$WOODLAND)), collapse=', ')}\n"))

# REF_FOREST_TYPE
ref_ft_path <- file.path(ref_dir, "REF_FOREST_TYPE.csv")
if (!file_exists(ref_ft_path)) {
  warning("REF_FOREST_TYPE.csv not found - skipping forest type lookup")
} else {
  ref_ft <- fread(ref_ft_path)
  out_path <- file.path(lookup_dir, "ref_forest_type.parquet")
  write_parquet(as_tibble(ref_ft), out_path)
  cat(glue("  ref_forest_type.parquet: {nrow(ref_ft)} types ({file_size(out_path)})\n"))
}

cat("\n")

# ------------------------------------------------------------------------------
# Step 2: Check schema of a sample of states
# ------------------------------------------------------------------------------

cat("Step 2: Checking schema consistency across sample states\n\n")

# Use first 5 states that have data downloaded
sample_states <- Filter(function(st) {
  dir_exists(file.path(raw_dir, st))
}, states)[seq_len(min(5, length(states)))]

if (length(sample_states) == 0) {
  cat("  No state data found. Run 01_download_fia.R first.\n\n")
} else {
  cat(glue("  Sample states: {paste(sample_states, collapse=', ')}\n\n"))

  for (tbl in names(required_cols)) {
    req <- required_cols[[tbl]]
    missing_by_state <- list()

    for (st in sample_states) {
      f <- file.path(raw_dir, st, paste0(st, "_", tbl, ".csv"))
      if (!file_exists(f)) {
        missing_by_state[[st]] <- paste0("[file missing]")
        next
      }
      # Read just the header row (0 data rows)
      header <- names(fread(f, nrows = 0))
      missing <- setdiff(req, header)
      if (length(missing) > 0) missing_by_state[[st]] <- missing
    }

    if (length(missing_by_state) == 0) {
      cat(glue("  {tbl}: all required columns present\n"))
    } else {
      for (st in names(missing_by_state)) {
        warning(glue("  {tbl} ({st}): missing {paste(missing_by_state[[st]], collapse=', ')}"))
      }
    }
  }
  cat("\n")
}

# ------------------------------------------------------------------------------
# Step 3: Print table sizes for all downloaded states
# ------------------------------------------------------------------------------

cat("Step 3: Table sizes for downloaded states\n\n")

downloaded_states <- Filter(function(st) {
  dir_exists(file.path(raw_dir, st))
}, states)

if (length(downloaded_states) == 0) {
  cat("  No state data downloaded yet.\n\n")
} else {
  cat(sprintf("  %-5s  %10s  %10s  %10s  %10s  %10s\n",
              "State", "TREE", "PLOT", "COND", "SEEDLING", "GRM_COMP"))
  cat(sprintf("  %s\n", paste(rep("-", 65), collapse="")))

  for (st in downloaded_states) {
    sizes <- vapply(c("TREE", "PLOT", "COND", "SEEDLING", "TREE_GRM_COMPONENT"), function(tbl) {
      f <- file.path(raw_dir, st, paste0(st, "_", tbl, ".csv"))
      if (file_exists(f)) as.character(file_size(f)) else "---"
    }, character(1))
    cat(sprintf("  %-5s  %10s  %10s  %10s  %10s  %10s\n",
                st, sizes[1], sizes[2], sizes[3], sizes[4], sizes[5]))
  }
  cat(glue("\n  States with data: {length(downloaded_states)}/{length(states)}\n\n"))
}

cat("Next step: Rscript 05_fia/scripts/03_extract_trees.R\n")
