# ------------------------------------------------------------------------------
# build_disturbance_history
# ------------------------------------------------------------------------------

build_disturbance_history <- function(out_dir, cond_ds, cond_dir = NULL) {
  # Step 5: plot_disturbance_history
  # Pivot DSTRBCD1/2/3 + DSTRBYR1/2/3 to long format and label disturbance types
  # ------------------------------------------------------------------------------

  cat("Step 5: plot_disturbance_history\n")
  out_disturb <- file.path(out_dir, "plot_disturbance_history.parquet")

  rb <- fia_should_rebuild(
    out_disturb,
    input_paths = if (!is.null(cond_dir)) cond_dir else character(0),
    required_cols = c("PLT_CN", "INVYR", "CONDID", "DSTRBCD", "DSTRBYR",
                      "disturbance_label"),
    label = "plot_disturbance_history"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_disturb)}) - skipping\n\n"))
  } else if (is.null(cond_ds)) {
    cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    if (file_exists(out_disturb)) cat(glue("  Rebuilding ({rb$reason})\n"))
    # Inline disturbance code lookup (FIADB v9.4 Appendix, COND.DSTRBCD)
    ref_disturbance <- data.table(
      DSTRBCD = c(10L, 11L, 12L,
                  20L, 21L, 22L,
                  30L, 31L, 32L,
                  40L, 41L, 42L, 43L, 44L, 45L, 46L,
                  50L, 51L, 52L, 53L, 54L,
                  60L, 70L, 80L,
                  90L, 91L, 92L, 93L, 94L, 95L),
      disturbance_label = c(
        "Insect damage", "Insect damage to understory", "Insect damage to trees",
        "Disease damage", "Disease damage to understory", "Disease damage to trees",
        "Fire damage (general)", "Ground fire", "Crown fire",
        "Animal damage", "Beaver", "Porcupine", "Deer/ungulate",
        "Bear", "Rabbit", "Domestic animal/livestock",
        "Weather damage", "Ice", "Wind/hurricane/tornado", "Flooding", "Drought",
        "Vegetation (competition/vines)", "Unknown/other", "Human-induced",
        "Geologic", "Landslide", "Avalanche", "Volcanic blast zone",
        "Other geologic event", "Earth movement/avalanche"
      ),
      disturbance_category = c(
        "insects", "insects", "insects",
        "disease", "disease", "disease",
        "fire", "fire", "fire",
        "animal", "animal", "animal", "animal", "animal", "animal", "animal",
        "weather", "weather", "weather", "weather", "weather",
        "vegetation", "other", "other",
        "geologic", "geologic", "geologic", "geologic", "geologic", "geologic"
      )
    )
    setkey(ref_disturbance, DSTRBCD)
  
    all_cond_d <- cond_ds |>
      select(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
             DSTRBCD1, DSTRBCD2, DSTRBCD3, DSTRBYR1, DSTRBYR2, DSTRBYR3) |>
      collect() |> as.data.table()
  
    # Pivot code and year columns together in matched pairs
    disturb_long <- rbindlist(list(
      all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     disturbance_slot = 1L,
                     DSTRBCD = DSTRBCD1, DSTRBYR = DSTRBYR1)],
      all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     disturbance_slot = 2L,
                     DSTRBCD = DSTRBCD2, DSTRBYR = DSTRBYR2)],
      all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     disturbance_slot = 3L,
                     DSTRBCD = DSTRBCD3, DSTRBYR = DSTRBYR3)]
    ))
  
    # Keep only actual disturbances (non-zero, non-NA)
    disturb_long <- disturb_long[!is.na(DSTRBCD) & DSTRBCD != 0L]
  
    # Join labels
    setkey(disturb_long, DSTRBCD)
    disturb_long <- ref_disturbance[disturb_long, on = "DSTRBCD"]
  
    write_parquet_atomic(as_tibble(disturb_long), out_disturb, compression = "snappy")
    cat(glue("  plot_disturbance_history: {format(nrow(disturb_long), big.mark=',')} rows -> ",
             "{file_size(out_disturb)}\n\n"))
    rm(all_cond_d, disturb_long); gc(verbose = FALSE)
  }
  
  # ------------------------------------------------------------------------------

  out_disturb
}

