# ==============================================================================
# ids_dedupe.R
# Remove redundant copies introduced by merging overlapping regional IDS archives
# ==============================================================================
#
# The IDS regional geodatabases overlap. One observation can be delivered in
# several regional archives, and occasionally twice within one archive. Merging
# them with bind_rows() keeps every copy, which breaks identifier uniqueness and
# inflates any acreage total.
#
# Measured on the 2026-07 build before this was applied: damage_areas held 3,500
# redundant rows across 3,498 identifier groups. That is only 0.08% of rows, but
# because the duplicated polygons are large (mean 12,869 acres against an overall
# mean of 131) it was 7.66% of total reported acreage.

library(dplyr)
library(sf)
library(glue)

#' Collapse rows that the same observation contributed more than once.
#'
#' The regional geodatabases overlap: one observation can be delivered in several
#' regional archives, and occasionally twice within one archive. bind_rows() keeps
#' every copy, so identifiers stop being unique and any acreage total is inflated.
#'
#' Copies that agree on every retained field are collapsed to one row, with
#' SOURCE_FILE recording all archives that supplied it. Copies that share an
#' identifier but disagree on some field are a different problem — those are kept
#' and written to a QA file, because picking one silently would hide a conflict.
dedupe_merged_layer <- function(df, id_cols, layer_name, qa_dir) {

  present_ids <- intersect(id_cols, names(df))
  if (length(present_ids) == 0) {
    cat(glue("  {layer_name}: no source identifier, skipping de-duplication\n"))
    return(df)
  }

  geom_col  <- attr(df, "sf_column")
  attr_cols <- setdiff(names(df), c("SOURCE_FILE", geom_col))
  n_before  <- nrow(df)

  # Two rows are copies when every retained field agrees. SOURCE_FILE is excluded
  # because differing archives is exactly the redundancy being removed.
  attrs <- st_drop_geometry(df)[, attr_cols, drop = FALSE]
  key   <- do.call(paste, c(attrs, sep = "\r"))
  dup   <- duplicated(key)

  if (!any(dup)) {
    cat(glue("  {layer_name}: no redundant copies found\n"))
    deduped <- df
  } else {
    # Before dropping anything, confirm the copies really are the same feature.
    # Only the affected rows are fingerprinted, so this stays cheap.
    affected  <- key %in% unique(key[dup])
    geom_fp   <- vapply(
      st_as_binary(st_geometry(df[affected, ])),
      function(g) paste0(as.integer(g), collapse = ","),
      character(1)
    )
    geom_agrees <- tapply(geom_fp, key[affected], function(g) length(unique(g)) == 1)
    bad_keys    <- names(geom_agrees)[!geom_agrees]

    # A group whose copies share every attribute but sit in different places is
    # not a redundant copy. Keep every row of such a group and report it.
    drop_row <- dup & !(key %in% bad_keys)

    # Retained rows record every archive that supplied them.
    archives <- tapply(df$SOURCE_FILE, key,
                       function(s) paste(sort(unique(s)), collapse = ";"))
    n_copies <- tapply(key, key, length)

    deduped <- df[!drop_row, ]
    deduped$SOURCE_FILE     <- unname(archives[key[!drop_row]])
    deduped$N_SOURCE_COPIES <- unname(n_copies[key[!drop_row]])

    n_removed <- n_before - nrow(deduped)
    cat(glue("  De-duplicated {layer_name}: {n_before} -> {nrow(deduped)} features ",
             "({n_removed} redundant copies removed)\n"))

    if (length(bad_keys) > 0) {
      cat(glue("  WARNING: {length(bad_keys)} group(s) share every attribute but ",
               "differ in geometry. All copies kept.\n"))
    }
  }

  # Anything still sharing an identifier disagrees on some field. Report, do not
  # guess which copy is right.
  conflicts <- deduped |>
    st_drop_geometry() |>
    group_by(across(all_of(present_ids))) |>
    filter(n() > 1) |>
    ungroup()

  if (nrow(conflicts) > 0) {
    dir.create(qa_dir, showWarnings = FALSE, recursive = TRUE)
    conflict_path <- file.path(qa_dir, glue("{layer_name}_identifier_conflicts.csv"))
    write.csv(conflicts, conflict_path, row.names = FALSE)
    cat(glue("  WARNING: {nrow(conflicts)} rows share an identifier but differ on ",
             "some field. Kept as-is and written to {conflict_path}\n"))
  }

  deduped
}
