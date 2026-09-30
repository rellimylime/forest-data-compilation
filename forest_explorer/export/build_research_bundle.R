#!/usr/bin/env Rscript

# Build the forest research bundle: one DuckDB database with the tables named in
# forest_explorer/registry/research_bundle.yaml, a README table for each research
# question, and convenience views. The canonical products are only read.
#
# Run from the repository root:
#   Rscript forest_explorer/export/build_research_bundle.R
#   Rscript forest_explorer/export/build_research_bundle.R --output=scratch_output/bundle.duckdb --overwrite
#
# The default output is under scratch_output/, which is gitignored.

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(yaml)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
root <- normalizePath(".", mustWork = TRUE)
if (!file.exists(file.path(root, "forest_explorer", "registry", "products.yaml"))) {
  stop("Run this script from the repository root.")
}

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(name, default) {
  hit <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(hit)) sub(paste0("^--", name, "="), "", hit[[1]]) else default
}
out_path <- arg_value("output", file.path("scratch_output", "research_bundle", "forest_research_bundle.duckdb"))
overwrite <- "--overwrite" %in% args
out_path <- file.path(root, out_path)
readme_path <- file.path(dirname(out_path), "README.md")

if (file.exists(out_path) && !overwrite) {
  stop("Output exists: ", out_path, "\nPass --overwrite to replace it.")
}

registry <- yaml::read_yaml(file.path(root, "forest_explorer", "registry", "products.yaml"))
bundle <- yaml::read_yaml(file.path(root, "forest_explorer", "registry", "research_bundle.yaml"))
products <- stats::setNames(registry$products, vapply(registry$products, `[[`, "", "id"))

table_ids <- vapply(bundle$tables, `[[`, "", "product_id")
unknown <- setdiff(table_ids, names(products))
if (length(unknown)) stop("Bundle names unknown products: ", paste(unknown, collapse = ", "))
for (question in bundle$questions) {
  missing <- setdiff(unlist(question$tables), table_ids)
  if (length(missing)) stop(question$id, " uses tables not in the bundle: ", paste(missing, collapse = ", "))
}

# Mirror the query planner's scan expressions so both read products identically.
source_sql <- function(product, con) {
  path <- file.path(root, product$path)
  switch(
    product$format,
    parquet_file = sprintf("read_parquet(%s)", dbQuoteString(con, path)),
    parquet_dataset = sprintf("read_parquet(%s, union_by_name = true)",
                              dbQuoteString(con, file.path(path, "**", "*.parquet"))),
    csv = sprintf("read_csv_auto(%s, header = true)", dbQuoteString(con, path)),
    csv_glob = sprintf("read_csv_auto(%s, header = true, union_by_name = true)",
                       dbQuoteString(con, if (grepl("[*?[]", path)) path else file.path(path, "**", "*.csv"))),
    stop(product$id, " has unsupported format ", product$format)
  )
}

git <- function(...) {
  out <- tryCatch(system2("git", c("-C", root, ...), stdout = TRUE, stderr = FALSE),
                  error = function(e) character())
  if (length(out)) out[[1]] else NA_character_
}
commit <- git("rev-parse", "--short", "HEAD")
dirty <- length(tryCatch(system2("git", c("-C", root, "status", "--porcelain", "--untracked-files=no"),
                                 stdout = TRUE, stderr = FALSE), error = function(e) character())) > 0
built_at <- format(Sys.time(), tz = "UTC", usetz = TRUE)

# Build into a temporary file so an interrupted run never leaves a partial bundle.
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
tmp_path <- paste0(out_path, ".partial")
unlink(c(tmp_path, paste0(tmp_path, ".wal")))
con <- dbConnect(duckdb::duckdb(dbdir = tmp_path, shared_home = FALSE))
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

cat("Build forest research bundle\n============================\n\n")
table_rows <- list()
for (entry in bundle$tables) {
  product <- products[[entry$product_id]]
  cat(sprintf("  %-40s ", product$id))
  dbExecute(con, sprintf("CREATE TABLE %s AS SELECT * FROM %s",
                         dbQuoteIdentifier(con, product$id), source_sql(product, con)))
  n <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM %s", dbQuoteIdentifier(con, product$id)))$n
  if (n == 0) stop(product$id, " is empty")
  cat(format(n, big.mark = ","), "rows\n")
  table_rows[[product$id]] <- data.frame(
    table_name = product$id,
    title = product$title,
    role = entry$role,
    one_row_is = product$one_row_is,
    keys = paste(unlist(product$keys), collapse = ", "),
    review_status = product$review_status,
    caveats = paste(unlist(product$caveats), collapse = " | "),
    rows = as.numeric(n),
    source_path = product$path,
    producer = product$producer %||% "",
    stringsAsFactors = FALSE
  )
}
tables_df <- do.call(rbind, table_rows)

for (view in bundle$views) {
  dbExecute(con, sprintf("CREATE VIEW %s AS %s", dbQuoteIdentifier(con, view$name), view$sql))
  dbGetQuery(con, sprintf("SELECT * FROM %s LIMIT 1", dbQuoteIdentifier(con, view$name)))
}

questions_df <- do.call(rbind, lapply(bundle$questions, function(q) data.frame(
  question_id = q$id, request = q$request, tables = paste(unlist(q$tables), collapse = ", "),
  how = trimws(q$how), stringsAsFactors = FALSE
)))
views_df <- do.call(rbind, lapply(bundle$views, function(v) data.frame(
  view_name = v$name, description = v$description, stringsAsFactors = FALSE
)))
info_df <- data.frame(
  bundle_version = bundle$bundle_registry_version,
  catalog_registry_version = registry$registry_version,
  built_at = built_at,
  git_commit = commit,
  git_uncommitted_changes = dirty,
  stringsAsFactors = FALSE
)
dbWriteTable(con, "bundle_tables", tables_df)
dbWriteTable(con, "bundle_questions", questions_df)
dbWriteTable(con, "bundle_views", views_df)
dbWriteTable(con, "bundle_info", info_df)

dbDisconnect(con, shutdown = TRUE)
if (file.exists(out_path)) unlink(out_path)
invisible(file.rename(tmp_path, out_path))

# Companion README, generated from the same definitions.
fmt <- function(n) format(n, big.mark = ",", scientific = FALSE)
readme <- c(
  paste0("# ", bundle$title),
  "",
  paste0("**Built:** ", built_at, " from commit `", commit, "`",
         if (dirty) " (with uncommitted changes)" else ""),
  paste0("**File:** `", basename(out_path), "` (", sprintf("%.0f MB", file.size(out_path) / 1e6), ")"),
  "",
  trimws(bundle$summary),
  "",
  "## Open it",
  "",
  "```r",
  "library(DBI)",
  paste0("con <- dbConnect(duckdb::duckdb(), \"", basename(out_path), "\", read_only = TRUE)"),
  "dbListTables(con)",
  "dbGetQuery(con, \"SELECT * FROM bundle_questions\")",
  "panel <- dbGetQuery(con, \"SELECT * FROM visit_cwm_with_mortality WHERE layer = 'trees'\")",
  "dbDisconnect(con, shutdown = TRUE)",
  "```",
  "",
  "```python",
  "import duckdb",
  paste0("con = duckdb.connect(\"", basename(out_path), "\", read_only=True)"),
  "panel = con.sql(\"SELECT * FROM visit_cwm_with_mortality\").df()",
  "```",
  "",
  "## Research questions",
  ""
)
for (i in seq_len(nrow(questions_df))) {
  readme <- c(readme,
    paste0("### ", questions_df$request[[i]]), "",
    paste0("Tables: ", paste0("`", strsplit(questions_df$tables[[i]], ", ")[[1]], "`", collapse = ", ")), "",
    questions_df$how[[i]], "")
}
readme <- c(readme, "## Tables", "", "| Table | One row is | Keys | Rows | Review |", "|---|---|---|---:|---|")
for (i in seq_len(nrow(tables_df))) {
  readme <- c(readme, sprintf("| `%s` | %s | `%s` | %s | %s |", tables_df$table_name[[i]],
                              tables_df$one_row_is[[i]], tables_df$keys[[i]],
                              fmt(tables_df$rows[[i]]), tables_df$review_status[[i]]))
}
readme <- c(readme, "", "## Views", "")
for (i in seq_len(nrow(views_df))) {
  readme <- c(readme, sprintf("- `%s`: %s", views_df$view_name[[i]], views_df$description[[i]]))
}
readme <- c(readme, "",
  "## Before using the numbers", "",
  "- Every table links on `PLT_CN`, `INVYR`, and `CONDID` (plus `SUBP` and species codes where present). `stable_plot_id` and `PREV_PLT_CN` follow a place through time.",
  "- Values from a coarser grain repeat when joined to a finer one (plot values on condition rows, condition values on life-stage rows). Never sum repeated values.",
  "- Tables marked `not_reviewed` or `domain_review_required` are reproducible but still need scientific review. Each table's caveats are in `bundle_tables.caveats`.",
  "- This file is an export. The authoritative products are listed in `bundle_tables.source_path`.")
writeLines(readme, readme_path, useBytes = TRUE)

cat("\nWrote ", out_path, " (", sprintf("%.0f MB", file.size(out_path) / 1e6), ")\n", sep = "")
cat("Wrote ", readme_path, "\n", sep = "")
