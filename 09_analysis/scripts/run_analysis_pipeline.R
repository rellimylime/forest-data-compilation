#!/usr/bin/env Rscript

# Run the active condition-level analysis in its documented order.

args <- commandArgs(trailingOnly = TRUE)

arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (!length(hit)) return(default)
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

dry_run <- "--dry-run" %in% args
skip_extraction <- "--skip-extraction" %in% args
skip_models <- "--skip-models" %in% args
from_stage <- arg_value("from", "00")
through_stage <- arg_value("through", "10")
run_id <- arg_value(
  "run-id",
  "20260822_cumulative_mortality_site_cwd_all_groups_v01"
)
climate_backend <- arg_value("climate-backend", "gee")

repo_root <- normalizePath(here::here(), winslash = "/", mustWork = TRUE)
setwd(repo_root)

qa_dirs <- c(
  "00_remeasurement_components",
  "01_condition_histories_and_cwm",
  "02_interval_mortality",
  "04_cumulative_mortality",
  "05_site_cwd_extraction",
  "06_cumulative_site_cwd",
  "07_pooled_community_cwm"
)
invisible(lapply(
  file.path("09_analysis", "qa", "outputs", qa_dirs),
  dir.create,
  recursive = TRUE,
  showWarnings = FALSE
))

run_r <- function(script, script_args = character(), env = character()) {
  command <- file.path(R.home("bin"), "Rscript")
  command_args <- c(script, script_args)
  cat("Rscript ", paste(command_args, collapse = " "), "\n", sep = "")
  if (dry_run) return(invisible(TRUE))
  status <- system2(command, command_args, env = env)
  if (!identical(status, 0L)) stop("Stage failed: ", script)
}

run_sql <- function(script) {
  cat("DuckDB ", script, "\n", sep = "")
  if (dry_run) return(invisible(TRUE))

  duckdb_cli <- Sys.which("duckdb")
  if (nzchar(duckdb_cli)) {
    read_command <- paste(
      ".read",
      normalizePath(script, winslash = "/", mustWork = TRUE)
    )
    status <- system2(
      duckdb_cli,
      c(":memory:", "-c", shQuote(read_command))
    )
    if (!identical(status, 0L)) stop("SQL stage failed: ", script)
    return(invisible(TRUE))
  }

  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    stop(
      "SQL stages require either the DuckDB CLI or the DBI and duckdb R ",
      "packages from renv."
    )
  }
  connection <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  sql <- paste(readLines(script, warn = FALSE), collapse = "\n")
  DBI::dbExecute(connection, sql)
}

stages <- list(
  list(id = "00", run = function() {
    run_r("09_analysis/scripts/00_build_remeasurement_components.R")
  }),
  list(id = "01", run = function() {
    run_r("09_analysis/scripts/01_build_condition_histories_and_cwm.R")
  }),
  list(id = "02", run = function() {
    run_r("09_analysis/scripts/02_build_interval_mortality.R")
  }),
  list(id = "03", run = function() {
    run_sql("09_analysis/scripts/03_select_complete_condition_histories.sql")
  }),
  list(id = "04", run = function() {
    run_r("09_analysis/scripts/04_build_cumulative_mortality.R")
  }),
  list(id = "05", run = function() {
    run_sql("09_analysis/scripts/05_prepare_site_cwd_inputs.sql")
  }),
  list(id = "05_extract", run = function() {
    if (skip_extraction) {
      cat("Skipping TerraClimate extraction; using the existing cache.\n")
      return(invisible(TRUE))
    }
    run_r(
      "site_climate/scripts/extract_terraclimate_points.R",
      c(
        "--input=09_analysis/data/intermediate/model_site_locations.csv",
        "--output-dir=09_analysis/data/cache/terraclimate_site_cwd",
        "--qa-dir=09_analysis/qa/outputs/05_site_cwd_extraction",
        "--variables=def",
        "--start-year=1958",
        "--end-year=2024",
        paste0("--backend=", climate_backend)
      )
    )
  }),
  list(id = "06", run = function() {
    run_sql("09_analysis/scripts/06_add_cumulative_site_cwd.sql")
  }),
  list(id = "07", run = function() {
    run_sql("09_analysis/scripts/07_build_pooled_community_cwm.sql")
  }),
  list(id = "08", run = function() {
    if (!skip_models) {
      run_r(
        "09_analysis/scripts/08_fit_preliminary_models_and_report.R",
        env = paste0("ANALYSIS_RUN_ID=", run_id)
      )
    }
  }),
  list(id = "09", run = function() {
    if (!skip_models) {
      run_r(
        "09_analysis/scripts/09_run_preliminary_robustness_checks.R",
        env = paste0("ANALYSIS_RUN_ID=", run_id)
      )
    }
  }),
  list(id = "10", run = function() {
    run_r(
      "09_analysis/qa/scripts/validate_qa_products.R",
      "--require-outputs"
    )
  })
)

stage_ids <- vapply(stages, `[[`, character(1), "id")
from_index <- match(from_stage, stage_ids)
through_index <- match(through_stage, stage_ids)
if (is.na(from_index) || is.na(through_index)) {
  stop(
    "Unknown stage. Valid stages are: ", paste(stage_ids, collapse = ", ")
  )
}
if (from_index > through_index) stop("--from must not follow --through.")

cat("Analysis run: ", run_id, "\n", sep = "")
cat(
  "Stages: ", stage_ids[[from_index]], " through ",
  stage_ids[[through_index]], "\n",
  sep = ""
)
for (i in seq.int(from_index, through_index)) {
  cat("\n[", stage_ids[[i]], "]\n", sep = "")
  stages[[i]]$run()
}

cat("\nAnalysis pipeline completed.\n")
