# ------------------------------------------------------------------------------
# Shared summary helpers
# ------------------------------------------------------------------------------

# Helper: Shannon diversity index
# dt must have group_cols + value_col (one row per species within group)
# Returns data.table with group_cols + shannon_h
# ------------------------------------------------------------------------------

compute_shannon_h <- function(dt, group_cols, value_col) {
  # Work on a copy so helper columns do not modify the caller's data.table.
  dt <- copy(dt)

  # Compute total abundance within each plot visit before calculating proportions.
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]

  # Convert each species or stratum value to a relative abundance.
  dt[total > 0, p_i := get(value_col) / total]

  # Keep Shannon contributions only where the proportion is valid and positive.
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]

  # Sum species contributions back to one diversity value per plot visit.
  dt[, .(shannon_h = sum(h_i, na.rm = TRUE)), by = group_cols]
}

# Sum optional fields while preserving NA when the whole field is unavailable.
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

# ------------------------------------------------------------------------------
# Freshness / rebuild contract shared by all summary builders.
#
# A product is (re)built when: forced; the output is missing; any declared input
# (file or partition directory) is newer than the output; or the existing output
# is missing a required contract column. Otherwise it is skipped. This replaces
# the unconditional skip-if-exists pattern so an upstream fix never leaves a
# stale downstream product silently in place.
#
# Force is read from getOption("fia_force_rebuild") (set once by the 05
# orchestrator from --force / --force=<product>) unless passed explicitly.
# ------------------------------------------------------------------------------
# TRUE when a rebuild was forced for this product (via --force / --force=<name>).
fia_force_requested <- function(label) {
  forced <- getOption("fia_force_rebuild", FALSE)
  isTRUE(forced) ||
    (is.character(forced) && (label %in% forced || "all" %in% forced))
}

fia_should_rebuild <- function(out_path,
                               input_paths = character(0),
                               required_cols = NULL,
                               force = NULL,
                               label = basename(out_path)) {
  if (is.null(force)) {
    forced <- getOption("fia_force_rebuild", FALSE)
    force <- isTRUE(forced) ||
      (is.character(forced) && (label %in% forced || "all" %in% forced))
  }
  if (isTRUE(force)) return(list(rebuild = TRUE, reason = "force"))
  if (!file.exists(out_path)) return(list(rebuild = TRUE, reason = "output missing"))

  # A declared input disappearing must never make a stale output look current.
  missing_inputs <- input_paths[!file.exists(input_paths)]
  if (length(missing_inputs) > 0) {
    return(list(
      rebuild = TRUE,
      reason = paste0(
        "declared input missing: ",
        paste(missing_inputs, collapse = ", ")
      )
    ))
  }

  # Expand any partition directories to their parquet files for mtime checks.
  existing <- input_paths
  files <- unlist(lapply(existing, function(p) {
    if (dir.exists(p)) {
      list.files(p, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE)
    } else {
      p
    }
  }))
  out_mtime <- file.info(out_path)$mtime
  if (length(files) > 0) {
    newest_in <- max(file.info(files)$mtime, na.rm = TRUE)
    if (is.finite(newest_in) && newest_in > out_mtime) {
      return(list(rebuild = TRUE, reason = "input newer than output"))
    }
  }

  # Schema contract: existing output must carry the required columns.
  if (!is.null(required_cols)) {
    sch_names <- tryCatch(names(arrow::open_dataset(out_path)),
                          error = function(e) NULL)
    if (is.null(sch_names) ||
        length(setdiff(required_cols, sch_names)) > 0) {
      return(list(rebuild = TRUE, reason = "schema/contract mismatch"))
    }
  }

  list(rebuild = FALSE, reason = "up to date")
}

# ------------------------------------------------------------------------------

