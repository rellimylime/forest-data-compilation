# ------------------------------------------------------------------------------
# Freshness / rebuild contract shared by the numbered build stages.
#
# A product is (re)built when: forced; the output is missing; any declared input
# (file or partition directory) is newer than the output; or the existing output
# is missing a required contract column. Otherwise it is skipped. This replaces
# the unconditional skip-if-exists pattern so an upstream fix never leaves a
# stale downstream product silently in place.
#
# Force is read from an option, by default getOption("build_force_rebuild"), so
# a stage can set it once from --force / --force=<product> and every product in
# that stage sees it. Modules that already own an option name pass it through
# `option`; 05_fia keeps "fia_force_rebuild" so its orchestrator is unchanged.
#
# Modification time is not a content hash. A fresh clone, a file copy, and a
# cloud-sync restore all rewrite it, and any of those can make a stale output
# look current. The required-column check catches the common case of a product
# built before a schema change, and --force is the escape hatch for the rest.
# ------------------------------------------------------------------------------

# TRUE when a rebuild was forced for this product (via --force / --force=<name>).
build_force_requested <- function(label, option = "build_force_rebuild") {
  forced <- getOption(option, FALSE)
  isTRUE(forced) ||
    (is.character(forced) && (label %in% forced || "all" %in% forced))
}

# Turn --force / --force=<product>[,<product>] into the value the option holds.
# Returns TRUE (force everything), a character vector of product labels, or
# FALSE when the flag is absent.
build_force_from_args <- function(args = commandArgs(trailingOnly = TRUE)) {
  if (any(args == "--force")) return(TRUE)
  hits <- grep("^--force=", args, value = TRUE)
  if (length(hits) == 0) return(FALSE)
  vals <- trimws(unlist(strsplit(sub("^--force=", "", hits), ",")))
  vals <- vals[nzchar(vals)]
  if (length(vals) == 0) TRUE else vals
}

build_should_rebuild <- function(out_path,
                                 input_paths = character(0),
                                 required_cols = NULL,
                                 force = NULL,
                                 label = basename(out_path),
                                 option = "build_force_rebuild") {
  if (is.null(force)) force <- build_force_requested(label, option = option)
  if (isTRUE(force)) return(list(rebuild = TRUE, reason = "force"))
  if (!file.exists(out_path)) {
    return(list(rebuild = TRUE, reason = "output missing"))
  }

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
  files <- unlist(lapply(input_paths, function(p) {
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

# Every stage reports the same way so skip decisions are visible in the log.
build_log_decision <- function(label, decision) {
  cat(sprintf(
    "%-46s %-7s (%s)\n",
    label,
    if (isTRUE(decision$rebuild)) "rebuild" else "skip",
    decision$reason
  ))
  invisible(decision)
}

# ------------------------------------------------------------------------------
