#!/usr/bin/env Rscript

# Profile derived analysis products without modifying repository data. The
# output is deliberately compact so profiles can be compared across machines.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(digest)
})

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(name, default = NULL) {
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (!length(hit)) return(default)
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

repo <- normalizePath(arg_value("repo", getwd()), winslash = "/", mustWork = TRUE)
output_dir <- arg_value(
  "output-dir",
  file.path(tempdir(), "forest_consistency_differences")
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

canonical_value <- function(x) {
  if (inherits(x, "Date")) {
    out <- ifelse(is.na(x), "<NA>", format(x, "%Y-%m-%d"))
  } else if (inherits(x, "POSIXt")) {
    out <- ifelse(
      is.na(x), "<NA>",
      format(x, "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")
    )
  } else if (inherits(x, "integer64")) {
    out <- ifelse(is.na(x), "<NA>", as.character(x))
  } else if (is.double(x)) {
    out <- rep.int("<NA>", length(x))
    out[is.nan(x)] <- "<NaN>"
    out[is.infinite(x) & x > 0] <- "<Inf>"
    out[is.infinite(x) & x < 0] <- "<-Inf>"
    finite <- is.finite(x)
    out[finite] <- sprintf("%.17g", x[finite])
  } else if (is.integer(x)) {
    out <- ifelse(is.na(x), "<NA>", as.character(x))
  } else if (is.logical(x)) {
    out <- ifelse(is.na(x), "<NA>", ifelse(x, "TRUE", "FALSE"))
  } else {
    out <- ifelse(is.na(x), "<NA>", enc2utf8(as.character(x)))
  }
  paste0(nchar(out, type = "bytes"), ":", out)
}

canonical_sha256 <- function(data, key, label) {
  d <- copy(data)
  setorderv(d, key, na.last = TRUE)
  safe_label <- gsub("[^A-Za-z0-9_.-]", "_", label)
  tmp <- file.path(output_dir, paste0(".", safe_label, ".canonical.tmp"))
  con <- file(tmp, open = "wb")
  on.exit({
    try(close(con), silent = TRUE)
    unlink(tmp)
  }, add = TRUE)

  starts <- seq.int(1L, nrow(d), by = 10000L)
  for (start in starts) {
    end <- min(start + 9999L, nrow(d))
    fields <- lapply(d[start:end], canonical_value)
    lines <- do.call(paste, c(fields, sep = "\t"))
    writeBin(charToRaw(paste0(lines, collapse = "\n")), con)
    writeBin(charToRaw("\n"), con)
  }
  close(con)
  con <- NULL
  digest(tmp, algo = "sha256", file = TRUE)
}

scalar_value <- function(x) {
  if (!length(x) || is.na(x)) return(NA_character_)
  if (is.double(x)) return(sprintf("%.17g", x))
  as.character(x)
}

specs <- list(
  history_cumulative_mortality = list(
    path = "09_analysis/data/processed/history_cumulative_mortality.parquet",
    key = "history_id"
  ),
  history_site_cwd = list(
    path = "09_analysis/data/processed/history_site_cwd.parquet",
    key = "history_id"
  ),
  lifestage_model_data = list(
    path = "09_analysis/data/processed/lifestage_model_data.parquet",
    key = c("history_id", "layer")
  ),
  pooled_model_data = list(
    path = "09_analysis/data/processed/pooled_model_data.parquet",
    key = "history_id"
  )
)

column_profiles <- list()
state_profiles <- list()
category_counts <- list()

for (product in names(specs)) {
  spec <- specs[[product]]
  path <- file.path(repo, spec$path)
  if (!file.exists(path)) stop("Missing product: ", path)
  d <- as.data.table(read_parquet(path))
  if (!all(spec$key %chin% names(d))) {
    stop("Missing declared key in ", product)
  }

  for (column in names(d)) {
    hash_columns <- unique(c(spec$key, column))
    x <- d[[column]]
    numeric_field <- is.numeric(x) && !inherits(x, "integer64")
    finite_x <- if (numeric_field) x[is.finite(x)] else numeric()
    column_profiles[[paste(product, column, sep = "|")]] <- data.table(
      product = product,
      position = match(column, names(d)),
      column = column,
      class = paste(class(x), collapse = "+"),
      rows = length(x),
      missing = sum(is.na(x)),
      nan = if (is.double(x)) sum(is.nan(x)) else 0L,
      distinct = uniqueN(x, na.rm = FALSE),
      minimum = if (length(finite_x)) scalar_value(min(finite_x)) else NA_character_,
      maximum = if (length(finite_x)) scalar_value(max(finite_x)) else NA_character_,
      sum = if (length(finite_x)) scalar_value(sum(finite_x)) else NA_character_,
      key_plus_column_sha256 = canonical_sha256(
        d[, ..hash_columns], spec$key,
        paste(product, "column", column, sep = "_")
      )
    )
  }

  if ("state" %chin% names(d)) {
    for (state_value in sort(unique(d$state), na.last = TRUE)) {
      subset <- if (is.na(state_value)) d[is.na(state)] else d[state == state_value]
      state_profiles[[paste(product, state_value, sep = "|")]] <- data.table(
        product = product,
        state = ifelse(is.na(state_value), "<NA>", as.character(state_value)),
        rows = nrow(subset),
        canonical_sha256 = canonical_sha256(
          subset, spec$key,
          paste(product, "state", state_value, sep = "_")
        )
      )
    }
  }

  categorical <- intersect(
    c(
      "cumulative_site_CWD_complete", "cumulative_site_CWD_status",
      "layer", "state"
    ),
    names(d)
  )
  for (column in categorical) {
    counts <- d[, .N, by = column]
    setnames(counts, column, "value")
    counts[, `:=`(product = product, column = column)]
    setcolorder(counts, c("product", "column", "value", "N"))
    category_counts[[paste(product, column, sep = "|")]] <- counts
  }

  message("Profiled ", product)
}

fwrite(
  rbindlist(column_profiles, fill = TRUE),
  file.path(output_dir, "product_column_profiles.csv")
)
fwrite(
  rbindlist(state_profiles, fill = TRUE),
  file.path(output_dir, "product_state_profiles.csv")
)
fwrite(
  rbindlist(category_counts, fill = TRUE),
  file.path(output_dir, "product_category_counts.csv")
)

git_output <- function(arguments) {
  paste(system2("git", arguments, stdout = TRUE, stderr = TRUE), collapse = "\n")
}
metadata <- data.table(
  field = c("git_branch", "git_commit", "git_status_short", "repository"),
  value = c(
    git_output(c("-C", repo, "branch", "--show-current")),
    git_output(c("-C", repo, "rev-parse", "HEAD")),
    git_output(c("-C", repo, "status", "--short")),
    repo
  )
)
fwrite(metadata, file.path(output_dir, "collection_metadata.csv"))

cat(
  "Difference profiles written to: ",
  normalizePath(output_dir, winslash = "/", mustWork = TRUE),
  "\n",
  sep = ""
)
