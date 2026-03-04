# ==============================================================================
# 01_download_fia.R
# Download USDA Forest Service Forest Inventory and Analysis (FIADB) data
#
# Downloads all required table CSVs for all 50 states via the rFIA package,
# plus national reference tables (REF_SPECIES, REF_FOREST_TYPE) directly.
#
# rFIA handles FIA DataMart URL construction. We use load=FALSE to download
# only; all processing is done downstream with data.table/arrow.
#
# Usage:
#   Rscript 05_fia/scripts/01_download_fia.R
#   Rscript 05_fia/scripts/01_download_fia.R CO WY MT   # specific states only
#
# Output: 05_fia/data/raw/{STATE}/{STATE}_{TABLE}.csv
#         05_fia/data/raw/REF/REF_SPECIES.csv
#         05_fia/data/raw/REF/REF_FOREST_TYPE.csv
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(rFIA)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config <- config$raw$fia
raw_dir    <- here(fia_config$local_dir)
tables     <- fia_config$tables_required

# Allow optional command-line state subset (for reruns or testing)
args   <- commandArgs(trailingOnly = TRUE)
states <- if (length(args) > 0) toupper(args) else fia_config$states

dir_create(raw_dir)

cat("FIA Data Download\n")
cat("=================\n\n")
cat(glue("Raw data directory: {raw_dir}\n\n"))
cat(glue("States to download: {length(states)}\n"))
cat(glue("Tables per state:   {paste(tables, collapse=', ')}\n\n"))

# ------------------------------------------------------------------------------
# Step 1: National reference tables (downloaded once, not per state)
# ------------------------------------------------------------------------------

cat("Step 1: National reference tables\n")

ref_dir <- file.path(raw_dir, "REF")
dir_create(ref_dir)

ref_tables <- c("REF_SPECIES", "REF_FOREST_TYPE")
ref_base   <- "https://apps.fs.usda.gov/fia/datamart/CSV"

for (tbl in ref_tables) {
  dest <- file.path(ref_dir, paste0(tbl, ".csv"))
  if (file_exists(dest)) {
    cat(glue("  Skipping {tbl}: already exists ({file_size(dest)})\n"))
    next
  }
  cat(glue("  Downloading {tbl}...\n"))
  tryCatch({
    download.file(
      url      = glue("{ref_base}/{tbl}.csv"),
      destfile = dest,
      mode     = "wb",
      quiet    = TRUE
    )
    cat(glue("  Saved: {file_size(dest)}\n"))
  }, error = function(e) {
    warning(glue("  Failed to download {tbl}: {e$message}"))
    if (file_exists(dest)) file_delete(dest)
  })
}

cat("\n")

# ------------------------------------------------------------------------------
# Step 2: Per-state table downloads via rFIA
# ------------------------------------------------------------------------------

cat(glue("Step 2: Downloading {length(states)} state(s) via rFIA\n\n"))

# Tables without REF_ prefix (rFIA handles state tables only)
state_tables <- tables[!startsWith(tables, "REF_")]

# Pre-scan: show status of all states before starting
cat("Status before download:\n")
for (st in states) {
  state_dir <- file.path(raw_dir, st)
  expected  <- file.path(state_dir, paste0(st, "_", state_tables, ".csv"))
  n_present <- sum(file.exists(expected))
  n_expect  <- length(expected)
  status    <- if (n_present == n_expect) "complete" else if (n_present > 0) glue("partial ({n_present}/{n_expect})") else "missing"
  cat(glue("  {st}: {status}\n"))
}
cat("\n")

# Large state TREE files can be several hundred MB — extend download timeout
options(timeout = 3600)  # 1 hour

t_start       <- Sys.time()
n_skipped     <- 0
n_done        <- 0
n_failed      <- 0
failed_states <- character(0)

for (i in seq_along(states)) {
  st        <- states[i]
  state_dir <- file.path(raw_dir, st)
  expected  <- file.path(state_dir, paste0(st, "_", state_tables, ".csv"))
  present   <- expected[file.exists(expected)]
  n_present <- length(present)
  n_expect  <- length(expected)

  # All files present — skip
  if (n_present == n_expect) {
    sizes <- vapply(present, function(f) as.character(file_size(f)), character(1))
    cat(glue("[{i}/{length(states)}] {st}: complete ({n_present}/{n_expect} files) - skipping\n"))
    n_skipped <- n_skipped + 1
    next
  }

  # Partial download — delete stale files before retrying
  if (n_present > 0) {
    cat(glue("[{i}/{length(states)}] {st}: partial download ({n_present}/{n_expect} files) - ",
             "removing and retrying\n"))
    file_delete(present)
  } else {
    cat(glue("[{i}/{length(states)}] {st}: downloading {n_expect} tables...\n"))
  }

  t_st <- Sys.time()
  dir_create(state_dir)

  tryCatch({
    getFIA(
      states = st,
      dir    = state_dir,
      load   = FALSE,
      tables = state_tables
    )
    present_now <- expected[file_exists(expected)]
    sizes       <- vapply(present_now, function(f) as.character(file_size(f)), character(1))
    elapsed     <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  Done ({sprintf('%.0fs', elapsed)}): ",
             "{paste(basename(present_now), sizes, sep='=', collapse=', ')}\n"))
    n_done <- n_done + 1
  }, error = function(e) {
    cat(glue("\n  *** FAILED: {st} — {e$message} ***\n\n"))
    n_failed      <<- n_failed + 1
    failed_states <<- c(failed_states, st)
  })
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

elapsed_total <- as.numeric(difftime(Sys.time(), t_start, units = "mins"))

cat(glue("\n{strrep('=', 50)}\n"))
cat("Download complete.\n\n")
cat(glue("  Downloaded:  {n_done} state(s)\n"))
cat(glue("  Skipped:     {n_skipped} state(s) (already present)\n"))
cat(glue("  Failed:      {n_failed} state(s)\n"))
cat(glue("  Total time:  {sprintf('%.1f', elapsed_total)} min\n\n"))

if (n_failed > 0) {
  cat(strrep("!", 50), "\n")
  cat("DOWNLOAD FAILURES — re-run to retry:\n")
  for (st in failed_states) cat(glue("  Rscript 05_fia/scripts/01_download_fia.R {st}\n"))
  cat(strrep("!", 50), "\n\n")
}

cat("Next step: Rscript 05_fia/scripts/02_inspect_fia.R\n")
