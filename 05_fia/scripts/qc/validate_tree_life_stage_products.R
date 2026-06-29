# ==============================================================================
# validate_tree_life_stage_products.R
# Validate the recovered TREE-derived tree and sapling composition products.
#
# Run:
#   Rscript 05_fia/scripts/qc/validate_tree_life_stage_products.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(data.table)
  library(arrow)
})

summary_dir <- here("05_fia/data/processed/summaries")
tree_path <- file.path(summary_dir, "plot_tree_species.parquet")
sapling_path <- file.path(summary_dir, "plot_sapling_species.parquet")

missing_files <- c(tree_path, sapling_path)[
  !file.exists(c(tree_path, sapling_path))
]
if (length(missing_files) > 0) {
  stop("Missing product(s): ", paste(missing_files, collapse = ", "))
}

trees <- as.data.table(read_parquet(tree_path))
saplings <- as.data.table(read_parquet(sapling_path))

required_cols <- c(
  "PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD", "species_key",
  "community_layer", "size_classes_present", "n_trees_tpa"
)
if (length(setdiff(required_cols, names(trees))) > 0) {
  stop("Tree product is missing required columns.")
}
if (length(setdiff(required_cols, names(saplings))) > 0) {
  stop("Sapling product is missing required columns.")
}

tree_bad_size <- trees[
  grepl("sapling", size_classes_present, fixed = TRUE)
]
sapling_bad_size <- saplings[
  size_classes_present != "sapling"
]
tree_bad_layer <- trees[community_layer != "tree"]
sapling_bad_layer <- saplings[community_layer != "sapling"]

grain <- c("PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD")
tree_duplicates <- trees[, .N, by = grain][N > 1]
sapling_duplicates <- saplings[, .N, by = grain][N > 1]
tree_metadata_match_rate <- mean(!is.na(trees$stable_plot_id))
sapling_metadata_match_rate <- mean(!is.na(saplings$stable_plot_id))

checks <- data.table(
  check = c(
    "tree_rows_present",
    "sapling_rows_present",
    "tree_product_excludes_sapling_size",
    "sapling_product_contains_only_sapling_size",
    "tree_community_layer",
    "sapling_community_layer",
    "tree_grain_unique",
    "sapling_grain_unique",
    "tree_condition_metadata_at_least_99_8pct",
    "sapling_condition_metadata_at_least_99_8pct"
  ),
  passed = c(
    nrow(trees) > 0,
    nrow(saplings) > 0,
    nrow(tree_bad_size) == 0,
    nrow(sapling_bad_size) == 0,
    nrow(tree_bad_layer) == 0,
    nrow(sapling_bad_layer) == 0,
    nrow(tree_duplicates) == 0,
    nrow(sapling_duplicates) == 0,
    tree_metadata_match_rate >= 0.998,
    sapling_metadata_match_rate >= 0.998
  )
)

cat("TREE Life-Stage Product Validation\n")
cat("==================================\n\n")
print(checks)
cat("\nRows:\n")
cat("  Trees:    ", format(nrow(trees), big.mark = ","), "\n", sep = "")
cat("  Saplings: ", format(nrow(saplings), big.mark = ","), "\n", sep = "")
cat("\nCondition metadata coverage:\n")
cat(
  "  Trees:    ",
  sprintf("%.3f%%", 100 * tree_metadata_match_rate),
  " (",
  format(sum(is.na(trees$stable_plot_id)), big.mark = ","),
  " unmatched rows)\n",
  sep = ""
)
cat(
  "  Saplings: ",
  sprintf("%.3f%%", 100 * sapling_metadata_match_rate),
  " (",
  format(sum(is.na(saplings$stable_plot_id)), big.mark = ","),
  " unmatched rows)\n",
  sep = ""
)

if (any(!checks$passed)) {
  stop("One or more tree/sapling validation checks failed.")
}
