#!/usr/bin/env Rscript

# Build the committed Forest Data Explorer snapshot.
#
# This is a maintainer command. The dashboard reads the resulting JSON and never
# needs to crawl the data directories. GitHub users can search the Markdown copy.
# Run from the repository root:
#   Rscript forest_explorer/catalog/build_snapshot.R

suppressPackageStartupMessages({
  library(arrow)
  library(jsonlite)
  library(yaml)
})

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
root <- normalizePath(".", mustWork = TRUE)
registry_path <- file.path(root, "forest_explorer", "registry", "products.yaml")
json_out <- file.path(root, "forest_explorer", "catalog", "snapshot", "catalog.json")
md_out <- file.path(root, "docs", "DATA_CATALOG.md")

registry <- yaml::read_yaml(registry_path)

schema_columns <- function(schema) {
  lapply(schema$fields, function(field) {
    list(field$name, field$type$ToString())
  })
}

probe_product <- function(product) {
  path <- file.path(root, product$path)
  format <- product$format
  observed <- list()
  availability <- "missing"
  reason <- "path does not exist in the snapshot environment"

  tryCatch({
    if (format %in% c("parquet_file", "parquet_dataset")) {
      if (!file.exists(path)) stop("path does not exist")
      parquet_files <- if (dir.exists(path)) {
        list.files(path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE)
      } else {
        path
      }
      if (!length(parquet_files)) stop("no Parquet files")
      ds <- arrow::open_dataset(path, format = "parquet")
      observed <- list(
        n_columns = length(ds$schema$fields),
        bytes = sum(file.info(parquet_files)$size, na.rm = TRUE),
        n_files = length(parquet_files),
        columns = schema_columns(ds$schema)
      )
      availability <- "available"
      reason <- NULL
    } else if (format == "csv") {
      if (!file.exists(path)) stop("path does not exist")
      frame <- utils::read.csv(path, nrows = 100, check.names = FALSE)
      observed <- list(
        bytes = unname(file.info(path)$size),
        n_columns = ncol(frame),
        columns = Map(function(name, value) list(name, class(value)[1]), names(frame), frame)
      )
      availability <- "available"
      reason <- NULL
    } else if (format == "csv_glob") {
      csv_files <- if (dir.exists(path)) {
        list.files(path, pattern = "[.]csv$", recursive = TRUE, full.names = TRUE)
      } else {
        Sys.glob(path)
      }
      if (!length(csv_files)) stop("no CSV files")
      frames <- lapply(csv_files, utils::read.csv, nrows = 100, check.names = FALSE)
      all_names <- unique(unlist(lapply(frames, names)))
      columns <- lapply(all_names, function(name) {
        value <- NULL
        for (frame in frames) {
          if (name %in% names(frame)) {
            value <- frame[[name]]
            break
          }
        }
        list(name, class(value)[1])
      })
      observed <- list(
        bytes = sum(file.info(csv_files)$size, na.rm = TRUE),
        n_files = length(csv_files),
        n_columns = length(columns),
        columns = columns
      )
      availability <- "available"
      reason <- NULL
    } else if (format == "gpkg_layer") {
      if (!file.exists(path)) stop("path does not exist")
      con <- DBI::dbConnect(RSQLite::SQLite(), path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)
      layer <- product$gpkg_layer
      info <- DBI::dbGetQuery(con, paste0("PRAGMA table_info(", DBI::dbQuoteString(con, layer), ")"))
      if (!nrow(info)) stop("GeoPackage layer does not exist")
      count <- DBI::dbGetQuery(
        con, paste0("SELECT COUNT(*) AS n FROM ", DBI::dbQuoteIdentifier(con, layer))
      )$n[[1]]
      observed <- list(
        n_rows = as.numeric(count),
        n_columns = nrow(info),
        bytes = unname(file.info(path)$size),
        columns = Map(function(name, type) list(name, type), info$name, info$type)
      )
      availability <- "available"
      reason <- NULL
    }
  }, error = function(error) {
    reason <<- conditionMessage(error)
  })

  product$availability <- availability
  product$reason <- reason
  product$observed <- observed
  product$key_check <- list(
    status = "not_checked",
    note = "The repository snapshot records schemas and locations; run build_inventory.py to verify keys against a data root."
  )
  product
}

message("Reading ", length(registry$products), " registered products...")
products <- lapply(registry$products, probe_product)
snapshot_time <- format(Sys.time(), tz = "UTC", usetz = TRUE)

snapshot <- list(
  generated_at = snapshot_time,
  environment_label = "committed repository snapshot",
  registry_version = registry$registry_version,
  snapshot_kind = "portable_catalog",
  families = registry$families,
  grains = registry$grains,
  products = products
)

dir.create(dirname(json_out), recursive = TRUE, showWarnings = FALSE)
jsonlite::write_json(snapshot, json_out, pretty = TRUE, auto_unbox = TRUE, na = "null")

escape_md <- function(x) {
  x <- paste(x %||% "", collapse = ", ")
  x <- gsub("\\|", "\\\\|", x)
  gsub("[\r\n]+", " ", x)
}

lines <- c(
  "# Searchable Data Catalog",
  "",
  paste0("**Snapshot generated:** ", snapshot_time),
  paste0("**Registry version:** ", registry$registry_version),
  "**Purpose:** find a product or variable without access to the data directories.",
  "",
  "Use your browser's Find command to search this page by variable, product, subject, path, or producer. The Streamlit dashboard reads the same committed snapshot. Availability is what was present when this snapshot was generated; it is not a live server check.",
  "",
  "## Products",
  "",
  "| Family | Product | One row / scale | Key | Path | Producer |",
  "|---|---|---|---|---|---|"
)

for (product in products) {
  family <- registry$families[[product$family]]$title %||% product$family
  lines <- c(lines, paste0(
    "| ", escape_md(family),
    " | ", escape_md(product$title),
    " | ", escape_md(product$one_row_is),
    " | `", escape_md(product$keys), "`",
    " | `", escape_md(product$path), "`",
    " | `", escape_md(product$producer), "` |"
  ))
}

lines <- c(
  lines, "", "## Variables", "",
  "A variable appears once for every product that contains it. This is intentional: the same name can occur at different row scales.",
  "",
  "| Variable | Type | Product | One row / scale | Key | Path |",
  "|---|---|---|---|---|---|"
)

for (product in products) {
  columns <- product$observed$columns %||% list()
  if (!length(columns)) {
    declared <- unique(c(product$keys %||% character(), product$facets %||% character()))
    columns <- lapply(declared, function(name) list(name, "declared key/filter"))
  }
  for (column in columns) {
    lines <- c(lines, paste0(
      "| `", escape_md(column[[1]]), "`",
      " | ", escape_md(column[[2]]),
      " | ", escape_md(product$title),
      " | ", escape_md(product$one_row_is),
      " | `", escape_md(product$keys), "`",
      " | `", escape_md(product$path), "` |"
    ))
  }
}

lines <- c(
  lines, "",
  "## Refreshing this snapshot",
  "",
  "From the repository root, in the restored R environment:",
  "",
  "```bash",
  "Rscript forest_explorer/catalog/build_snapshot.R",
  "```",
  "",
  "For a data-root-specific availability and key-uniqueness audit, use `python3 forest_explorer/catalog/build_inventory.py`. That local inventory is intentionally not committed."
)
writeLines(lines, md_out, useBytes = TRUE)

message("Wrote ", json_out)
message("Wrote ", md_out)
