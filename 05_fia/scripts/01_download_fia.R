# ==============================================================================
# 01_download_fia.R
# Download USDA Forest Service Forest Inventory and Analysis (FIADB) data
#
# Downloads all configured state-table ZIP archives directly from FIA DataMart.
# A state is downloaded into staging and validated before its existing raw
# directory is replaced, so one failed table cannot leave a partial snapshot.
#
# Usage:
#   Rscript 05_fia/scripts/01_download_fia.R
#   Rscript 05_fia/scripts/01_download_fia.R CO WY MT
#   Rscript 05_fia/scripts/01_download_fia.R --refresh FL KY TX
#
# By default, a state with every configured file already present is skipped.
# --refresh reacquires every configured table for the requested states.
#
# Output: 05_fia/data/raw/{STATE}/{STATE}_{TABLE}.csv
#         05_fia/data/raw/REF/{REF_TABLE}.csv
#         05_fia/data/raw/download_manifest.csv
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config  <- config$raw$fia
raw_dir     <- here(fia_config$local_dir)
state_tables <- fia_config$tables_required
datamart_base <- "https://apps.fs.usda.gov/fia/datamart/CSV"

args <- commandArgs(trailingOnly = TRUE)
refresh <- "--refresh" %in% args
unknown_flags <- setdiff(args[startsWith(args, "--")], "--refresh")
if (length(unknown_flags) > 0L) {
  stop(glue("Unknown option(s): {paste(unknown_flags, collapse=', ')}"))
}

state_args <- toupper(args[!startsWith(args, "--")])
states <- if (length(state_args) > 0L) state_args else fia_config$states
unknown_states <- setdiff(states, fia_config$states)
if (length(unknown_states) > 0L) {
  stop(glue("State(s) are not configured: {paste(unknown_states, collapse=', ')}"))
}

dir_create(raw_dir)
staging_root <- file.path(raw_dir, ".download_staging")
manifest_path <- file.path(raw_dir, "download_manifest.csv")

cat("FIA Data Download\n")
cat("=================\n\n")
cat(glue("Raw data directory: {raw_dir}\n"))
cat(glue("States: {paste(states, collapse=', ')}\n"))
cat(glue("State tables: {paste(state_tables, collapse=', ')}\n"))
cat(glue("Mode: {if (refresh) 'refresh complete state snapshots' else 'download missing state snapshots'}\n\n"))

# Large state TREE archives can take several minutes.
options(timeout = 3600)

validate_csv <- function(path) {
  if (!file_exists(path)) return("file is missing")
  size <- file_info(path)$size
  if (is.na(size) || size <= 0) return("file is empty")
  header <- readLines(path, n = 1L, warn = FALSE)
  if (length(header) != 1L || !nzchar(header) || !grepl(",", header, fixed = TRUE)) {
    return("file does not have a readable CSV header")
  }
  NA_character_
}

download_state_table <- function(state, table, stage_dir) {
  archive_name <- glue("{state}_{table}.zip")
  csv_name <- glue("{state}_{table}.csv")
  archive_path <- file.path(stage_dir, archive_name)
  csv_path <- file.path(stage_dir, csv_name)
  url <- glue("{datamart_base}/{archive_name}")

  cat(glue("    {archive_name}\n"))
  download.file(url, archive_path, mode = "wb", quiet = TRUE)
  members <- unzip(archive_path, list = TRUE)$Name
  member <- members[basename(members) == csv_name]
  if (length(member) != 1L) {
    stop(glue("{archive_name} did not contain exactly one {csv_name}"))
  }

  extracted <- unzip(archive_path, files = member, exdir = stage_dir, overwrite = TRUE)
  extracted_path <- extracted[[1L]]
  if (normalizePath(extracted_path, winslash = "/", mustWork = FALSE) !=
      normalizePath(csv_path, winslash = "/", mustWork = FALSE)) {
    file_move(extracted_path, csv_path)
  }
  file_delete(archive_path)

  problem <- validate_csv(csv_path)
  if (!is.na(problem)) stop(glue("Invalid {csv_name}: {problem}"))
  csv_path
}

validate_state_relationships <- function(state, stage_dir) {
  required <- c("PLOT", "COND", "TREE", "SEEDLING")
  if (!all(required %in% state_tables)) return(invisible(NULL))
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("data.table is required for staged FIA key validation")
  }

  staged_path <- function(table) {
    file.path(stage_dir, glue("{state}_{table}.csv"))
  }
  plot_ids <- unique(data.table::fread(
    staged_path("PLOT"),
    select = "CN",
    integer64 = "character",
    showProgress = FALSE
  )$CN)
  condition_keys <- unique(data.table::fread(
    staged_path("COND"),
    select = c("PLT_CN", "CONDID"),
    integer64 = "character",
    showProgress = FALSE
  ))

  check_biological_table <- function(table) {
    keys <- unique(data.table::fread(
      staged_path(table),
      select = c("PLT_CN", "CONDID"),
      integer64 = "character",
      showProgress = FALSE
    ))
    missing_plot <- keys[!PLT_CN %in% plot_ids]
    missing_condition <- data.table::fsetdiff(keys, condition_keys)
    if (nrow(missing_plot) > 0L || nrow(missing_condition) > 0L) {
      stop(glue(
        "{state} staged {table} keys do not match staged PLOT/COND: ",
        "missing plot keys={nrow(missing_plot)}, ",
        "missing condition keys={nrow(missing_condition)}"
      ))
    }
    cat(glue(
      "    {table} key check: {format(nrow(keys), big.mark=',')} ",
      "distinct plot-condition keys matched\n"
    ))
  }

  check_biological_table("TREE")
  check_biological_table("SEEDLING")
  invisible(NULL)
}

promote_state_snapshot <- function(state, stage_dir) {
  state_dir <- file.path(raw_dir, state)
  backup_dir <- file.path(raw_dir, glue(".download_previous_{state}_{snapshot_id}"))
  staged <- file.path(stage_dir, glue("{state}_{state_tables}.csv"))
  expected <- file.path(state_dir, basename(staged))

  # Directory renames are unreliable in synced Windows workspaces. Copy every
  # current file to a private backup first, then replace the state files one by
  # one. If any copy or validation fails, restore the full previous set.
  dir_create(state_dir)
  previous <- expected[file_exists(expected)]
  new_only <- setdiff(expected, previous)
  if (length(previous) > 0L) {
    dir_create(backup_dir)
    file_copy(previous, file.path(backup_dir, basename(previous)), overwrite = TRUE)
  }

  tryCatch({
    file_copy(staged, expected, overwrite = TRUE)
    problems <- vapply(expected, validate_csv, character(1))
    if (any(!is.na(problems))) {
      stop(glue(
        "Promoted snapshot failed validation: {paste(basename(expected[!is.na(problems)]), problems[!is.na(problems)], collapse='; ')}"
      ))
    }
    if (!identical(unname(tools::md5sum(staged)), unname(tools::md5sum(expected)))) {
      stop("Promoted files do not match the validated staged files")
    }
  }, error = function(e) {
    # An incomplete state may not have had every configured table before this
    # run. Remove any newly introduced targets before restoring its old files,
    # so a failed promotion cannot make that state look complete next time.
    introduced <- new_only[file_exists(new_only)]
    if (length(introduced) > 0L) file_delete(introduced)
    if (length(previous) > 0L) {
      backup_files <- file.path(backup_dir, basename(previous))
      file_copy(backup_files, previous, overwrite = TRUE)
    }
    stop(e)
  })

  # Old source files are recoverable from DataMart. Remove backup contents only
  # after the promoted files match staging; an empty directory is harmless if
  # Windows declines to remove the final read-only directory entry.
  if (dir_exists(backup_dir)) {
    backup_files <- dir_ls(backup_dir, type = "file", fail = FALSE)
    if (length(backup_files) > 0L) file_delete(backup_files)
    try(suppressWarnings(dir_delete(backup_dir)), silent = TRUE)
  }
  try(suppressWarnings(dir_delete(stage_dir)), silent = TRUE)
  expected
}

manifest_rows <- list()
snapshot_id <- format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC")

# ------------------------------------------------------------------------------
# Step 1: National reference tables
# ------------------------------------------------------------------------------

cat("Step 1: National reference tables\n")
ref_dir <- file.path(raw_dir, "REF")
dir_create(ref_dir)
ref_tables <- c("REF_SPECIES", "REF_FOREST_TYPE", "REF_PLANT_DICTIONARY")

for (table in ref_tables) {
  dest <- file.path(ref_dir, paste0(table, ".csv"))
  if (file_exists(dest)) {
    cat(glue("  {table}: already present - skipping\n"))
    next
  }
  cat(glue("  Downloading {table}\n"))
  tmp <- paste0(dest, ".tmp")
  tryCatch({
    download.file(glue("{datamart_base}/{table}.csv"), tmp, mode = "wb", quiet = TRUE)
    problem <- validate_csv(tmp)
    if (!is.na(problem)) stop(problem)
    file_move(tmp, dest)
  }, error = function(e) {
    if (file_exists(tmp)) file_delete(tmp)
    warning(glue("Failed to download {table}: {e$message}"))
  })
}

# ------------------------------------------------------------------------------
# Step 2: Complete per-state snapshots
# ------------------------------------------------------------------------------

cat("\nStep 2: State tables\n")
dir_create(staging_root)
t_start <- Sys.time()
failed_states <- character()
skipped_states <- character()
completed_states <- character()

for (i in seq_along(states)) {
  state <- states[[i]]
  state_dir <- file.path(raw_dir, state)
  expected <- file.path(state_dir, glue("{state}_{state_tables}.csv"))
  complete <- all(file_exists(expected))

  if (complete && !refresh) {
    cat(glue("[{i}/{length(states)}] {state}: complete - skipping\n"))
    skipped_states <- c(skipped_states, state)
    next
  }

  stage_dir <- file.path(staging_root, glue("{state}_{snapshot_id}"))
  if (dir_exists(stage_dir)) {
    stop(glue("Safety stop: staging directory already exists: {stage_dir}"))
  }
  dir_create(stage_dir)
  cat(glue("[{i}/{length(states)}] {state}: downloading complete snapshot\n"))

  tryCatch({
    for (table in state_tables) download_state_table(state, table, stage_dir)
    validate_state_relationships(state, stage_dir)
    promoted_files <- promote_state_snapshot(state, stage_dir)
    acquired_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    manifest_rows[[state]] <- data.frame(
      snapshot_id = snapshot_id,
      state = state,
      table = state_tables,
      acquired_at_utc = acquired_at,
      source_url = glue("{datamart_base}/{state}_{state_tables}.zip"),
      file_bytes = as.numeric(file_info(promoted_files)$size),
      md5 = unname(tools::md5sum(promoted_files)),
      stringsAsFactors = FALSE
    )
    completed_states <- c(completed_states, state)
    cat(glue("  Promoted {length(promoted_files)} validated files\n"))
  }, error = function(e) {
    failed_states <<- c(failed_states, state)
    cat(glue("  FAILED: {e$message}\n"))
  })
}

# Keep one current manifest row per state/table. This small sidecar records the
# actual raw snapshot; source metadata is not repeated in biological outputs.
if (length(manifest_rows) > 0L) {
  new_manifest <- do.call(rbind, manifest_rows)
  if (file_exists(manifest_path)) {
    old_manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
    replace_key <- paste(new_manifest$state, new_manifest$table)
    old_manifest <- old_manifest[!paste(old_manifest$state, old_manifest$table) %in% replace_key, ]
    new_manifest <- rbind(old_manifest, new_manifest)
  }
  new_manifest <- new_manifest[order(new_manifest$state, new_manifest$table), ]
  manifest_tmp <- paste0(manifest_path, ".tmp")
  write.csv(new_manifest, manifest_tmp, row.names = FALSE, na = "")
  if (file_exists(manifest_path)) file_delete(manifest_path)
  file_move(manifest_tmp, manifest_path)
}

elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "mins"))
cat("\nDownload summary\n")
cat(glue("  Refreshed/downloaded: {length(completed_states)} [{paste(completed_states, collapse=', ')}]\n"))
cat(glue("  Skipped: {length(skipped_states)} [{paste(skipped_states, collapse=', ')}]\n"))
cat(glue("  Failed: {length(failed_states)} [{paste(failed_states, collapse=', ')}]\n"))
cat(glue("  Elapsed: {sprintf('%.1f', elapsed)} minutes\n"))
if (file_exists(manifest_path)) cat(glue("  Manifest: {manifest_path}\n"))

if (length(failed_states) > 0L) quit(save = "no", status = 1L)

cat("\nNext step: rebuild the processed products that depend on refreshed states.\n")
