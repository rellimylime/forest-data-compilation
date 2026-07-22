# ------------------------------------------------------------------------------
# build_tree_species
# Build separate live-tree and sapling species-composition products.
#
# FIA stores both saplings and larger trees in TREE:
#   sapling: 1.0-4.9 inches diameter
#   tree:    5.0 inches diameter and larger
#
# The per-state extraction already assigns size_class from TREE.DIA. This
# builder preserves the common source species key while separating the two
# life-stage communities into different output files.
# ------------------------------------------------------------------------------

build_tree_species <- function(out_dir, proc_fia, states, out_cond_metadata) {
  cat("Step 4c: plot_tree_species + plot_sapling_species\n")

  out_tree_species <- file.path(out_dir, "plot_tree_species.parquet")
  out_sapling_species <- file.path(out_dir, "plot_sapling_species.parquet")

  tree_source_files <- list.files(
    here(proc_fia$trees$output_dir),
    pattern = "[.]parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  products_exist <- file_exists(out_tree_species) &&
    file_exists(out_sapling_species)
  newest_input <- max(
    c(
      file.info(tree_source_files)$mtime,
      file.info(out_cond_metadata)$mtime
    ),
    na.rm = TRUE
  )
  oldest_output <- if (products_exist) {
    min(file.info(c(out_tree_species, out_sapling_species))$mtime)
  } else {
    as.POSIXct(NA)
  }
  source_newer <- products_exist && newest_input > oldest_output
  forced <- fia_force_requested("plot_tree_species")

  if (products_exist && !source_newer && !forced) {
    cat(glue(
      "  Already exist ({file_size(out_tree_species)}, ",
      "{file_size(out_sapling_species)}) - skipping\n\n"
    ))
    return(c(
      tree_species = out_tree_species,
      sapling_species = out_sapling_species
    ))
  }

  if (source_newer) {
    cat("  Tree extracts or condition metadata are newer - rebuilding\n")
  }

  if (!file_exists(out_cond_metadata)) {
    cat("  plot_condition_metadata.parquet not found. Run Step 4b first.\n\n")
    return(c(
      tree_species = out_tree_species,
      sapling_species = out_sapling_species
    ))
  }

  trees_ds <- tryCatch(
    open_dataset(here(proc_fia$trees$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(trees_ds)) {
    cat("  No tree parquets found. Run 03_extract_trees.R first.\n\n")
    return(c(
      tree_species = out_tree_species,
      sapling_species = out_sapling_species
    ))
  }

  required_tree_cols <- c(
    "CONDID", "SUBP", "SPCD", "STATUSCD", "size_class",
    "ba_per_acre", "n_trees_tpa", "n_trees_raw"
  )
  missing_tree_cols <- setdiff(required_tree_cols, names(trees_ds))
  if (length(missing_tree_cols) > 0) {
    cat(glue(
      "  Tree parquets missing: {paste(missing_tree_cols, collapse = ', ')}\n"
    ))
    cat("  Re-run: Rscript 05_fia/scripts/03_extract_trees.R --force-trees\n\n")
    return(c(
      tree_species = out_tree_species,
      sapling_species = out_sapling_species
    ))
  }

  cond_meta <- as.data.table(read_parquet(out_cond_metadata))
  meta_cols <- intersect(
    c(
      "PLT_CN", "INVYR", "CONDID", "stable_plot_id",
      "STATECD", "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN",
      "LAT", "LON", "ELEV", "FORTYPCD", "forest_type_label",
      "forest_type_group", "COND_STATUS_CD", "CONDPROP_UNADJ",
      "pct_forested", "is_forested_condition",
      "has_fire_condition", "has_crown_fire_condition",
      "has_insect_condition", "has_disease_condition",
      "has_wind_condition", "has_drought_condition",
      "has_human_dist_condition", "has_cutting_treatment"
    ),
    names(cond_meta)
  )
  cond_meta <- cond_meta[, ..meta_cols]

  tree_cols <- intersect(
    c(
      "PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD",
      "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
      "SFTWD_HRDWD", "WOODLAND", "MAJOR_SPGRPCD", "JENKINS_SPGRPCD",
      "STATUSCD", "size_class", "canopy_layer",
      "ba_per_acre", "n_trees_tpa", "n_trees_raw", "state"
    ),
    names(trees_ds)
  )

  summarize_layer <- function(dt, layer_name, keep_size_classes) {
    layer_dt <- dt[
      STATUSCD == 1L &
        size_class %in% keep_size_classes &
        (
          (!is.na(ba_per_acre) & ba_per_acre > 0) |
            (!is.na(n_trees_tpa) & n_trees_tpa > 0)
        )
    ]
    if (nrow(layer_dt) == 0) return(NULL)

    species_cols <- intersect(
      c(
        "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
        "SFTWD_HRDWD", "WOODLAND", "MAJOR_SPGRPCD", "JENKINS_SPGRPCD"
      ),
      names(layer_dt)
    )
    group_cols <- c(
      "PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD",
      species_cols, "state"
    )

    result <- layer_dt[, .(
      ba_per_acre = sum(ba_per_acre, na.rm = TRUE),
      n_trees_tpa = sum(n_trees_tpa, na.rm = TRUE),
      n_trees_raw = sum(n_trees_raw, na.rm = TRUE),
      n_tree_strata = .N,
      size_classes_present = paste(
        sort(unique(na.omit(size_class))),
        collapse = ";"
      ),
      canopy_layers_present = paste(
        sort(unique(na.omit(canopy_layer))),
        collapse = ";"
      )
    ), by = group_cols]

    result[, `:=`(
      source_table = "TREE",
      source_species_code = as.character(SPCD),
      species_key = paste0("fia_spcd:", SPCD),
      community_layer = layer_name,
      # Stem density is comparable across sapling species. Basal area remains
      # available for adult-tree dominance analyses.
      abundance_for_cwm = if (layer_name == "sapling") {
        n_trees_tpa
      } else {
        fifelse(ba_per_acre > 0, ba_per_acre, n_trees_tpa)
      }
    )]

    result
  }

  tree_results <- vector("list", length(states))
  sapling_results <- vector("list", length(states))
  started <- Sys.time()

  for (i in seq_along(states)) {
    state_code <- states[[i]]
    state_trees <- tryCatch(
      trees_ds |>
        filter(state == state_code) |>
        select(all_of(tree_cols)) |>
        collect() |>
        as.data.table(),
      error = function(e) {
        cat(glue("  Warning: could not read {state_code}: {conditionMessage(e)}\n"))
        NULL
      }
    )
    if (is.null(state_trees) || nrow(state_trees) == 0) next

    tree_results[[i]] <- summarize_layer(
      state_trees,
      "tree",
      c("intermediate", "mature")
    )
    sapling_results[[i]] <- summarize_layer(
      state_trees,
      "sapling",
      "sapling"
    )

    rm(state_trees)
    gc(verbose = FALSE)

    if (i %% 10 == 0 || i == length(states)) {
      elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))
      cat(glue("  [{i}/{length(states)}] {sprintf('%.0fs', elapsed)}\n"))
    }
  }

  tree_species <- rbindlist(Filter(Negate(is.null), tree_results), fill = TRUE)
  sapling_species <- rbindlist(
    Filter(Negate(is.null), sapling_results),
    fill = TRUE
  )
  if (nrow(tree_species) == 0 || nrow(sapling_species) == 0) {
    stop("Tree or sapling summary is empty; refusing to overwrite products.")
  }

  tree_species <- merge(
    tree_species,
    cond_meta,
    by = c("PLT_CN", "INVYR", "CONDID"),
    all.x = TRUE
  )
  sapling_species <- merge(
    sapling_species,
    cond_meta,
    by = c("PLT_CN", "INVYR", "CONDID"),
    all.x = TRUE
  )

  write_parquet_atomic(
    as_tibble(tree_species),
    out_tree_species,
    compression = "snappy"
  )
  write_parquet_atomic(
    as_tibble(sapling_species),
    out_sapling_species,
    compression = "snappy"
  )

  cat(glue(
    "  plot_tree_species: {format(nrow(tree_species), big.mark = ',')} rows -> ",
    "{file_size(out_tree_species)}\n"
  ))
  cat(glue(
    "  plot_sapling_species: ",
    "{format(nrow(sapling_species), big.mark = ',')} rows -> ",
    "{file_size(out_sapling_species)}\n\n"
  ))

  c(
    tree_species = out_tree_species,
    sapling_species = out_sapling_species
  )
}
