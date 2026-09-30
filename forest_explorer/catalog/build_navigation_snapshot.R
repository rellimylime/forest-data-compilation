# Build portable join and query-preset snapshots plus a GitHub-readable guide.
#
# This file is sourced by build_snapshot.R after the product snapshot is built.
# The parent script supplies root, registry, products, snapshot_time, escape_md,
# and `%||%`, keeping every navigation artifact tied to one catalog refresh.

join_path <- file.path(root, "forest_explorer", "registry", "joins.yaml")
preset_path <- file.path(root, "forest_explorer", "registry", "query_presets.yaml")
snapshot_dir <- file.path(root, "forest_explorer", "catalog", "snapshot")
join_out <- file.path(snapshot_dir, "joins.json")
preset_out <- file.path(snapshot_dir, "query_presets.json")
guide_out <- file.path(root, "docs", "QUERY_GUIDE.md")

join_registry <- yaml::read_yaml(join_path)
preset_registry <- yaml::read_yaml(preset_path)
product_ids <- vapply(registry$products, `[[`, character(1), "id")
product_by_id <- stats::setNames(registry$products, product_ids)
observed_by_id <- stats::setNames(products, product_ids)
join_ids <- vapply(join_registry$joins, `[[`, character(1), "id")
join_by_id <- stats::setNames(join_registry$joins, join_ids)

as_vector <- function(value) as.character(unlist(value %||% character()))
known_columns <- function(product_id) {
  product <- observed_by_id[[product_id]]
  observed <- product$observed$columns %||% list()
  observed_names <- if (length(observed)) {
    vapply(observed, function(column) as.character(column[[1]]), character(1))
  } else {
    character()
  }
  unique(c(
    observed_names,
    as_vector(product_by_id[[product_id]]$keys),
    as_vector(product_by_id[[product_id]]$facets)
  ))
}

if (anyDuplicated(join_ids)) stop("Duplicate join ids in joins.yaml")
for (join in join_registry$joins) {
  refs <- c(join$left_product_id, join$right_product_id)
  unknown <- setdiff(refs, product_ids)
  if (length(unknown)) {
    stop(join$id, " references unknown product ids: ", paste(unknown, collapse = ", "))
  }
  left_on <- as_vector(join$left_on)
  right_on <- as_vector(join$right_on)
  if (!length(left_on) || length(left_on) != length(right_on)) {
    stop(join$id, " must declare equally sized left_on and right_on keys")
  }
  missing_left <- setdiff(left_on, known_columns(join$left_product_id))
  missing_right <- setdiff(right_on, known_columns(join$right_product_id))
  if (length(missing_left) || length(missing_right)) {
    stop(join$id, " references columns absent from the catalog snapshot")
  }
}

preset_ids <- vapply(preset_registry$presets, `[[`, character(1), "id")
if (anyDuplicated(preset_ids)) stop("Duplicate preset ids in query_presets.yaml")
for (preset in preset_registry$presets) {
  if (!preset$anchor_product_id %in% product_ids) {
    stop(preset$id, " references an unknown anchor product")
  }
  selected_joins <- as_vector(preset$join_ids)
  unknown_joins <- setdiff(selected_joins, join_ids)
  if (length(unknown_joins)) {
    stop(preset$id, " references unknown joins: ", paste(unknown_joins, collapse = ", "))
  }
  for (join_id in selected_joins) {
    if (join_by_id[[join_id]]$left_product_id != preset$anchor_product_id) {
      stop(preset$id, " uses a join whose left product is not its anchor")
    }
  }
  reachable <- c(
    preset$anchor_product_id,
    vapply(selected_joins, function(id) join_by_id[[id]]$right_product_id, character(1))
  )
  for (product_id in names(preset$columns %||% list())) {
    if (!product_id %in% reachable) {
      stop(preset$id, " selects columns from an unreachable product: ", product_id)
    }
    unknown_columns <- setdiff(as_vector(preset$columns[[product_id]]), known_columns(product_id))
    if (length(unknown_columns)) {
      stop(preset$id, " selects unknown columns from ", product_id, ": ",
           paste(unknown_columns, collapse = ", "))
    }
  }
}

join_snapshot <- join_registry
join_snapshot$generated_at <- snapshot_time
join_snapshot$catalog_registry_version <- registry$registry_version
preset_snapshot <- preset_registry
preset_snapshot$generated_at <- snapshot_time
preset_snapshot$catalog_registry_version <- registry$registry_version

jsonlite::write_json(join_snapshot, join_out, pretty = TRUE, auto_unbox = TRUE, na = "null")
jsonlite::write_json(preset_snapshot, preset_out, pretty = TRUE, auto_unbox = TRUE, na = "null")

relationship_labels <- c(
  one_to_one = "1:1 — preserves rows",
  many_to_one = "many:1 — preserves left rows",
  one_to_many = "1:many — expands rows"
)
status_labels <- c(
  certified = "Certified",
  constrained = "Constrained",
  review_required = "Scientific review required"
)

guide <- c(
  "# Dataset Query Guide",
  "",
  paste0("**Snapshot generated:** ", snapshot_time),
  paste0("**Join registry:** ", join_registry$join_registry_version),
  paste0("**Preset registry:** ", preset_registry$preset_registry_version),
  "",
  paste0(
    "**Navigation:** [Repository home](../README.md) | ",
    "[Documentation hub](README.md) | ",
    "[Searchable data catalog](DATA_CATALOG.md) | ",
    "[Dashboard guide](dashboard/README.md)"
  ),
  "",
  "This guide shows how existing products connect. It does not replace the product catalog or materialize a second giant database. The dashboard reads the same committed snapshots and generates paste-ready DuckDB SQL without opening the data files.",
  "",
  "## Quick start",
  "",
  "```bash",
  "streamlit run docs/dashboard/app.py",
  "```",
  "",
  "Open **Data → Build a dataset** in the dashboard menu, choose a recipe or anchor product, select fields, review row-expansion warnings, and copy or download the generated SQL. See the [dashboard guide](dashboard/README.md) for running an export.",
  "",
  "## Curated joins",
  "",
  "| Anchor product | Add product | Join keys | Cardinality | Review | Important warning |",
  "|---|---|---|---|---|---|"
)
for (join in join_registry$joins) {
  left <- product_by_id[[join$left_product_id]]
  right <- product_by_id[[join$right_product_id]]
  key_text <- paste0(
    paste(as_vector(join$left_on), collapse = ", "),
    " = ",
    paste(as_vector(join$right_on), collapse = ", ")
  )
  guide <- c(guide, paste0(
    "| ", escape_md(left$title),
    " | ", escape_md(right$title),
    " | `", escape_md(key_text), "`",
    " | ", relationship_labels[[join$relationship]],
    " | ", status_labels[[join$status]],
    " | ", escape_md(join$warning), " |"
  ))
}

guide <- c(
  guide,
  "",
  "## Research recipes",
  "",
  "The baseline-table recipe is deliberately a query, not a new canonical product. This keeps one authoritative copy of every value while still giving collaborators a single export when they need one.",
  ""
)
for (preset in preset_registry$presets) {
  guide <- c(
    guide,
    paste0("### ", escape_md(preset$title)),
    "",
    escape_md(preset$description),
    "",
    paste0("- **Anchor:** ", escape_md(product_by_id[[preset$anchor_product_id]]$title)),
    paste0("- **Joins:** ", length(as_vector(preset$join_ids))),
    paste0("- **Search terms:** ", escape_md(preset$request_tags))
  )
  cautions <- as_vector(preset$cautions)
  if (length(cautions)) {
    guide <- c(guide, "- **Cautions:**", paste0("  - ", vapply(cautions, escape_md, character(1))))
  }
  missing <- as_vector(preset$missing_capabilities)
  if (length(missing)) {
    guide <- c(guide, "- **Still missing:**", paste0("  - ", vapply(missing, escape_md, character(1))))
  }
  guide <- c(guide, "")
}

guide <- c(
  guide,
  "## Safety rules",
  "",
  "- A `many:1` join preserves anchor rows but repeats the right-side value across the finer anchor grain.",
  "- A `1:many` join expands anchor rows. The dashboard permits only one expanding join per query, preventing accidental cross-products between unrelated detail tables.",
  "- Products marked for scientific review remain visible because the goal is navigation. The dashboard shows their warnings beside the plan; the generated SQL itself does not repeat them.",
  "- Generated paths are repository-relative. Run queries from the repository root or edit the paths explicitly. Exports default to the gitignored `scratch_output/` directory, which must exist before DuckDB writes to it.",
  "- Generated SQL uses `LEFT JOIN`, so an unmatched anchor row remains visible for QA.",
  "",
  "## Refreshing the snapshots",
  "",
  "Edit the curated YAML registries, then run:",
  "",
  "```bash",
  "Rscript forest_explorer/catalog/build_snapshot.R",
  "```",
  "",
  "The command refreshes the product catalog, join map, recipes, and this page together."
)
writeLines(guide, guide_out, useBytes = TRUE)

message("Wrote ", join_out)
message("Wrote ", preset_out)
message("Wrote ", guide_out)
