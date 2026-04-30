#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(testthat)
  library(here)
})

available_modules <- c("01_ids", "02_terraclimate", "03_prism", "04_worldclim", "05_fia")

args <- commandArgs(trailingOnly = TRUE)
strict <- "--strict" %in% args
args <- args[args != "--strict"]

modules <- if (length(args) == 0) available_modules else args
invalid <- setdiff(modules, available_modules)
if (length(invalid) > 0) {
  stop(
    "Unknown module(s): ", paste(invalid, collapse = ", "),
    "\nValid values: ", paste(available_modules, collapse = ", ")
  )
}

if (strict) {
  Sys.setenv(STRICT_OUTPUT_CHECKS = "true")
}

cat("\n")
cat("===============================================================================\n")
cat("Forest Data Compilation - Repository Test Suite\n")
cat("===============================================================================\n")
cat("Modules : ", paste(modules, collapse = ", "), "\n", sep = "")
cat("Mode    : ", if (strict) "STRICT (missing outputs fail)" else "NON-STRICT (missing outputs skip)", "\n", sep = "")
cat("\n")

all_passed <- TRUE

for (module in modules) {
  test_dir_path <- here(module, "tests", "testthat")
  cat("\n")
  cat("--- ", module, " ---------------------------------------------------------------\n", sep = "")

  if (!dir.exists(test_dir_path)) {
    message("Missing test directory: ", test_dir_path)
    all_passed <- FALSE
    next
  }

  ok <- TRUE
  tryCatch(
    {
      testthat::test_dir(
        test_dir_path,
        reporter = "summary",
        stop_on_failure = TRUE,
        stop_on_warning = FALSE
      )
    },
    error = function(e) {
      ok <<- FALSE
      message("Failure in ", module, ": ", conditionMessage(e))
    }
  )

  if (!ok) {
    all_passed <- FALSE
  }
}

cat("\n")
cat("===============================================================================\n")
if (all_passed) {
  cat("All selected module suites passed.\n")
  cat("===============================================================================\n")
  quit(save = "no", status = 0)
} else {
  cat("One or more module suites failed.\n")
  cat("===============================================================================\n")
  quit(save = "no", status = 1)
}
