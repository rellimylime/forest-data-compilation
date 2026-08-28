#!/usr/bin/env Rscript

# Validate that every analysis QA result has one tracked producer.

args <- commandArgs(trailingOnly = TRUE)
require_outputs <- "--require-outputs" %in% args
repo_root <- normalizePath(here::here(), winslash = "/", mustWork = TRUE)
manifest_path <- file.path(repo_root, "09_analysis", "qa", "qa_products.csv")
output_root <- file.path(repo_root, "09_analysis", "qa", "outputs")

products <- read.csv(manifest_path, stringsAsFactors = FALSE)
required_columns <- c(
  "stage", "output_path", "producer_script", "required", "description"
)
missing_columns <- setdiff(required_columns, names(products))
if (length(missing_columns)) {
  stop("QA manifest is missing columns: ", paste(missing_columns, collapse = ", "))
}
if (anyDuplicated(products$output_path)) {
  stop("QA manifest contains duplicate output paths.")
}

tracked <- system2(
  "git",
  c("-C", shQuote(repo_root), "ls-files"),
  stdout = TRUE,
  stderr = TRUE
)
if (!is.null(attr(tracked, "status")) && attr(tracked, "status") != 0L) {
  stop("Could not read the Git file index.")
}

problems <- character()
for (i in seq_len(nrow(products))) {
  product <- products[i, ]
  producer_path <- file.path(repo_root, product$producer_script)
  output_path <- file.path(repo_root, product$output_path)

  if (!file.exists(producer_path)) {
    problems <- c(problems, paste("Missing producer:", product$producer_script))
    next
  }
  if (!product$producer_script %in% tracked) {
    problems <- c(problems, paste("Producer is not tracked:", product$producer_script))
  }

  producer_text <- paste(readLines(producer_path, warn = FALSE), collapse = "\n")
  if (!grepl(basename(product$output_path), producer_text, fixed = TRUE)) {
    problems <- c(
      problems,
      paste("Producer does not name output:", product$output_path)
    )
  }

  if (require_outputs && isTRUE(product$required) && !file.exists(output_path)) {
    problems <- c(problems, paste("Missing required output:", product$output_path))
  }
}

local_outputs <- list.files(
  output_root,
  recursive = TRUE,
  full.names = TRUE,
  all.files = TRUE,
  no.. = TRUE
)
local_outputs <- local_outputs[file.info(local_outputs)$isdir %in% FALSE]
local_outputs <- local_outputs[basename(local_outputs) != ".gitkeep"]
normalized_root <- normalizePath(repo_root, winslash = "/", mustWork = TRUE)
local_outputs <- normalizePath(local_outputs, winslash = "/", mustWork = FALSE)
local_outputs <- substring(local_outputs, nchar(normalized_root) + 2L)
unregistered <- setdiff(local_outputs, products$output_path)
if (length(unregistered)) {
  problems <- c(problems, paste("Unregistered QA output:", unregistered))
}

if (length(problems)) {
  stop(paste(problems, collapse = "\n"), call. = FALSE)
}

cat(
  "QA provenance passed for ", nrow(products), " registered products",
  if (require_outputs) " with required outputs present" else "",
  ".\n",
  sep = ""
)
