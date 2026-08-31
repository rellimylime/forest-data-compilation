#!/usr/bin/env Rscript

# Check repository organization and output-to-producer references without
# reading or rebuilding any scientific data products.

suppressPackageStartupMessages({
  library(here)
  library(data.table)
})

repo_root <- here()
failures <- character()

record_failure <- function(message) {
  failures <<- c(failures, message)
}

# 1. Require the common product-module structure.
modules <- sprintf("%02d", 1:9)
module_dirs <- c(
  "01_ids", "02_terraclimate", "03_prism", "04_worldclim", "05_fia",
  "06_species_niches", "07_thermophilization", "08_disturbance_linkage",
  "09_analysis"
)
stopifnot(length(modules) == length(module_dirs))

for (module in module_dirs) {
  required <- file.path(repo_root, module, c("README.md", "scripts"))
  missing <- required[!file.exists(required)]
  if (length(missing) > 0L) {
    record_failure(sprintf("%s is missing: %s", module, paste(basename(missing), collapse = ", ")))
  }
}

# 2. Parse tracked workflow R scripts so moves do not hide syntax errors.
script_roots <- c(module_dirs, "scripts")
r_scripts <- unlist(lapply(
  file.path(repo_root, script_roots),
  list.files,
  pattern = "\\.[Rr]$",
  recursive = TRUE,
  full.names = TRUE
))
for (script in r_scripts) {
  tryCatch(
    parse(file = script),
    error = function(error) {
      record_failure(sprintf("R parse failure in %s: %s", script, conditionMessage(error)))
    }
  )
}

# Require short purpose/flow comments in maintained workflow code, including SQL.
code_scripts <- unlist(lapply(
  file.path(repo_root, c(module_dirs, "scripts")),
  list.files,
  pattern = "\\.(R|r|py|sql)$",
  recursive = TRUE,
  full.names = TRUE
))
code_scripts <- code_scripts[
  grepl("[/\\\\](scripts|qa[/\\\\]scripts)[/\\\\]", code_scripts) &
    !grepl("[/\\\\]templates[/\\\\]", code_scripts)
]
for (script in code_scripts) {
  lines <- readLines(script, warn = FALSE, encoding = "UTF-8")
  is_sql <- grepl("\\.sql$", script, ignore.case = TRUE)
  is_python <- grepl("\\.py$", script, ignore.case = TRUE)
  marker <- if (is_sql) "^\\s*--" else "^\\s*#"
  comment_lines <- grep(marker, lines)
  has_python_docstring <- is_python && any(grepl("^\\s*(\"\"\"|''')", head(lines, 20L)))
  has_header <- any(comment_lines <= 20L) || has_python_docstring
  if (!has_header || length(comment_lines) < 2L) {
    record_failure(sprintf("Workflow script needs purpose/flow comments: %s", script))
  }
}

# 3. Keep validation code under qa/scripts and out of production script trees.
deprecated_qc_dirs <- file.path(repo_root, module_dirs, "scripts", "qc")
nonempty_qc_dirs <- deprecated_qc_dirs[
  dir.exists(deprecated_qc_dirs) &
    vapply(deprecated_qc_dirs, function(path) length(list.files(path, all.files = FALSE)) > 0L, logical(1))
]
if (length(nonempty_qc_dirs) > 0L) {
  record_failure(paste("Deprecated nonempty scripts/qc directories:", paste(nonempty_qc_dirs, collapse = ", ")))
}

for (module in module_dirs) {
  qa_dir <- file.path(repo_root, module, "qa")
  if (!dir.exists(qa_dir)) next
  loose_code <- list.files(qa_dir, pattern = "\\.(R|r|py|sql)$", full.names = TRUE)
  if (length(loose_code) > 0L) {
    record_failure(sprintf("%s has QA code outside qa/scripts: %s", module, paste(basename(loose_code), collapse = ", ")))
  }
}

# 4. Require every QA manifest to point to a tracked producer that exists.
manifests <- list.files(
  repo_root,
  pattern = "qa_products\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)
for (manifest in manifests) {
  entries <- fread(manifest)
  if (!"producer_script" %in% names(entries)) {
    record_failure(sprintf("QA manifest lacks producer_script: %s", manifest))
    next
  }
  producer_paths <- file.path(repo_root, entries$producer_script)
  missing <- unique(entries$producer_script[!file.exists(producer_paths)])
  if (length(missing) > 0L) {
    record_failure(sprintf("Missing QA producer(s) in %s: %s", manifest, paste(missing, collapse = ", ")))
  }
}

# 5. Check the central product registry without requiring a YAML package.
registry_path <- file.path(repo_root, "forest_explorer", "registry", "products.yaml")
registry_lines <- readLines(registry_path, warn = FALSE, encoding = "UTF-8")
producer_lines <- grep("^\\s*producer:\\s*", registry_lines, value = TRUE)
registry_producers <- trimws(sub("^\\s*producer:\\s*", "", producer_lines))
registry_producers <- gsub("^[\"']|[\"']$", "", registry_producers)
registry_producers <- registry_producers[grepl("/", registry_producers, fixed = TRUE)]
missing_registry_producers <- unique(
  registry_producers[!file.exists(file.path(repo_root, registry_producers))]
)
if (length(missing_registry_producers) > 0L) {
  record_failure(paste("Missing registry producer(s):", paste(missing_registry_producers, collapse = ", ")))
}

# Report all structural problems together so one run is enough to repair them.
if (length(failures) > 0L) {
  cat("Repository structure audit failed:\n")
  cat(paste0("- ", failures, collapse = "\n"), "\n")
  quit(save = "no", status = 1L)
}

cat(sprintf(
  "Repository structure audit passed (%d modules, %d QA manifests, %d registry producers).\n",
  length(module_dirs), length(manifests), length(registry_producers)
))
