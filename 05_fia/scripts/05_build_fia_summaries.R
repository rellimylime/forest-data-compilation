# ==============================================================================
# 05_build_fia_summaries.R
# Build plot-level FIA summary metrics from extracted parquet files
#
# Reads the per-state partitioned parquets from scripts 03 and 04, and
# aggregates to plot x INVYR level with:
#
#   plot_tree_metrics.parquet
#     - Total and per-stratum basal area (live/dead, soft/hardwood, size class,
#       canopy layer)
#     - Species richness and Shannon diversity index (BA-weighted, live only)
#     - Schema includes empty column for species temperature optima join (TBD)
#
#   plot_seedling_metrics.parquet
#     - Seedling counts per plot x INVYR, with Shannon H (count-weighted)
#
#   plot_mortality_metrics.parquet
#     - Between-measurement mortality per plot x INVYR x species x agent
#
#   plot_cond_fortypcd.parquet
#     - Forest type per plot x INVYR x condition (for transition analysis)
#
# Usage:
#   Rscript 05_fia/scripts/05_build_fia_summaries.R
#
# Output: 05_fia/data/processed/summaries/
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(arrow)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config  <- config$raw$fia
proc_fia    <- config$processed$fia
out_dir     <- here(proc_fia$summaries$output_dir)
states      <- fia_config$states

# Open cond dataset once (used for LAT/LON join in Step 1 and for Step 5)
cond_ds <- tryCatch(
  open_dataset(here(proc_fia$cond$output_dir), partitioning = "state"),
  error = function(e) NULL
)

dir_create(out_dir)

cat("FIA Plot-Level Summaries\n")
cat("========================\n\n")
cat(glue("Output: {out_dir}\n\n"))

# ------------------------------------------------------------------------------
# Helper: Shannon diversity index
# dt must have group_cols + value_col (one row per species within group)
# Returns data.table with group_cols + shannon_h
# ------------------------------------------------------------------------------

compute_shannon_h <- function(dt, group_cols, value_col) {
  dt <- copy(dt)
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]
  dt[total > 0, p_i := get(value_col) / total]
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]
  dt[, .(shannon_h = sum(h_i, na.rm = TRUE)), by = group_cols]
}

# ------------------------------------------------------------------------------
# Step 1: plot_tree_metrics
# Aggregate trees parquet to plot x INVYR level
# ------------------------------------------------------------------------------

cat("Step 1: plot_tree_metrics\n")
out_tree_metrics <- file.path(out_dir, "plot_tree_metrics.parquet")

if (file_exists(out_tree_metrics)) {
  cat(glue("  Already exists ({file_size(out_tree_metrics)}) - skipping\n\n"))
} else {
  trees_ds <- tryCatch(
    open_dataset(here(proc_fia$trees$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(trees_ds)) {
    cat("  No tree parquets found. Run 03_extract_trees.R first.\n\n")
  } else {

    t_start <- Sys.time()
    results <- vector("list", length(states))

    for (i in seq_along(states)) {
      st <- states[i]
      dt <- tryCatch({
        trees_ds |> filter(state == st) |> collect() |> as.data.table()
      }, error = function(e) NULL)

      if (is.null(dt) || nrow(dt) == 0) next

      # -- BA totals by stratum (all combinations computed in one pass) --------
      live <- dt[STATUSCD == 1]
      dead <- dt[STATUSCD == 2]

      # Total BA
      ba_live <- live[, .(ba_live_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_live  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]
      ba_dead <- dead[, .(ba_dead_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_dead  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # By functional group (live only)
      ba_soft <- live[SFTWD_HRDWD == "S",
                       .(ba_live_softwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]
      ba_hard <- live[SFTWD_HRDWD == "H",
                       .(ba_live_hardwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # By size class (live only)
      ba_sz <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, size_class)],
        PLT_CN + INVYR ~ size_class,
        value.var = "ba_per_acre", fill = 0
      )
      # Rename columns defensively (not all size classes may be present)
      for (sc in c("sapling", "intermediate", "mature")) {
        if (!sc %in% names(ba_sz)) ba_sz[, (paste0("ba_live_", sc)) := NA_real_]
        else setnames(ba_sz, sc, paste0("ba_live_", sc))
      }

      # By canopy layer (live only)
      ba_ly <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, canopy_layer)],
        PLT_CN + INVYR ~ canopy_layer,
        value.var = "ba_per_acre", fill = 0
      )
      for (ly in c("overstory", "understory")) {
        if (!ly %in% names(ba_ly)) ba_ly[, (paste0("ba_live_", ly)) := NA_real_]
        else setnames(ba_ly, ly, paste0("ba_live_", ly))
      }

      # -- Diversity: species richness and Shannon H (BA-weighted, live) -------
      species_ba <- live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
                          by = .(PLT_CN, INVYR, SPCD)]
      species_ba <- species_ba[ba_per_acre > 0]

      richness <- species_ba[, .(n_species_live = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]
      shannon  <- compute_shannon_h(species_ba, c("PLT_CN", "INVYR"), "ba_per_acre")
      setnames(shannon, "shannon_h", "shannon_h_ba")

      # -- Join all metrics into one row per plot x INVYR ----------------------
      plot_keys <- .(PLT_CN, INVYR)
      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(ba_live, ba_dead, ba_soft, ba_hard,
                            ba_sz, ba_ly, richness, shannon))

      # Add placeholder column for species temperature optima (thermophilization)
      result[, species_temp_optima_mean := NA_real_]   # join TBD when boss provides data

      # Add LAT/LON from cond (one coordinate per PLT_CN)
      if (!is.null(cond_ds)) {
        coord_dt <- tryCatch({
          cond_ds |>
            filter(state == st) |>
            select(PLT_CN, LAT, LON) |>
            collect() |>
            as.data.table() |>
            unique(by = "PLT_CN")
        }, error = function(e) NULL)
        if (!is.null(coord_dt)) result <- coord_dt[result, on = "PLT_CN"]
      }

      result[, state := st]
      results[[i]] <- result

      rm(dt, live, dead, ba_live, ba_dead, ba_soft, ba_hard,
         ba_sz, ba_ly, species_ba, richness, shannon, result)
      gc(verbose = FALSE)

      if (i %% 10 == 0 || i == length(states)) {
        elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
        cat(glue("  [{i}/{length(states)}] {sprintf('%.0fs', elapsed)}\n"))
      }
    }

    all_metrics <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)
    write_parquet(as_tibble(all_metrics), out_tree_metrics, compression = "snappy")
    cat(glue("  plot_tree_metrics: {format(nrow(all_metrics), big.mark=',')} rows -> ",
             "{file_size(out_tree_metrics)}\n\n"))
    rm(all_metrics, results)
    gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 2: plot_seedling_metrics
# ------------------------------------------------------------------------------

cat("Step 2: plot_seedling_metrics\n")
out_seed_metrics <- file.path(out_dir, "plot_seedling_metrics.parquet")

if (file_exists(out_seed_metrics)) {
  cat(glue("  Already exists ({file_size(out_seed_metrics)}) - skipping\n\n"))
} else {
  seed_ds <- tryCatch(
    open_dataset(here(proc_fia$seedlings$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(seed_ds)) {
    cat("  No seedling parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    results <- vector("list", length(states))
    for (i in seq_along(states)) {
      st <- states[i]
      dt <- tryCatch(
        seed_ds |> filter(state == st) |> collect() |> as.data.table(),
        error = function(e) NULL
      )
      if (is.null(dt) || nrow(dt) == 0) next

      richness <- dt[, .(n_species_seedling = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]
      shannon  <- compute_shannon_h(dt, c("PLT_CN", "INVYR"), "treecount_total")
      setnames(shannon, "shannon_h", "shannon_h_count")

      totals <- dt[, .(treecount_total = sum(treecount_total, na.rm = TRUE),
                        count_softwood  = sum(treecount_total[SFTWD_HRDWD == "S"], na.rm = TRUE),
                        count_hardwood  = sum(treecount_total[SFTWD_HRDWD == "H"], na.rm = TRUE)),
                    by = .(PLT_CN, INVYR)]

      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(totals, richness, shannon))
      result[, state := st]
      results[[i]] <- result
      rm(dt, totals, richness, shannon, result); gc(verbose = FALSE)
    }
    all_seed <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)
    write_parquet(as_tibble(all_seed), out_seed_metrics, compression = "snappy")
    cat(glue("  plot_seedling_metrics: {format(nrow(all_seed), big.mark=',')} rows -> ",
             "{file_size(out_seed_metrics)}\n\n"))
    rm(all_seed, results); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 3: plot_mortality_metrics
# Pass-through of per-state mortality parquets into a single national file
# ------------------------------------------------------------------------------

cat("Step 3: plot_mortality_metrics\n")
out_mort_metrics <- file.path(out_dir, "plot_mortality_metrics.parquet")

if (file_exists(out_mort_metrics)) {
  cat(glue("  Already exists ({file_size(out_mort_metrics)}) - skipping\n\n"))
} else {
  mort_ds <- tryCatch(
    open_dataset(here(proc_fia$mortality$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(mort_ds)) {
    cat("  No mortality parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    all_mort <- mort_ds |> collect() |> as.data.table()
    write_parquet(as_tibble(all_mort), out_mort_metrics, compression = "snappy")
    cat(glue("  plot_mortality_metrics: {format(nrow(all_mort), big.mark=',')} rows -> ",
             "{file_size(out_mort_metrics)}\n\n"))
    rm(all_mort); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 4: plot_cond_fortypcd
# Pass-through of per-state cond parquets into a single national file
# ------------------------------------------------------------------------------

cat("Step 4: plot_cond_fortypcd\n")
out_cond_metrics <- file.path(out_dir, "plot_cond_fortypcd.parquet")

if (file_exists(out_cond_metrics)) {
  cat(glue("  Already exists ({file_size(out_cond_metrics)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {
  all_cond <- cond_ds |> collect() |> as.data.table()
  write_parquet(as_tibble(all_cond), out_cond_metrics, compression = "snappy")
  cat(glue("  plot_cond_fortypcd: {format(nrow(all_cond), big.mark=',')} rows -> ",
           "{file_size(out_cond_metrics)}\n\n"))
  rm(all_cond); gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Step 5: plot_disturbance_history
# Pivot DSTRBCD1/2/3 + DSTRBYR1/2/3 to long format and label disturbance types
# ------------------------------------------------------------------------------

cat("Step 5: plot_disturbance_history\n")
out_disturb <- file.path(out_dir, "plot_disturbance_history.parquet")

if (file_exists(out_disturb)) {
  cat(glue("  Already exists ({file_size(out_disturb)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {
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

  write_parquet(as_tibble(disturb_long), out_disturb, compression = "snappy")
  cat(glue("  plot_disturbance_history: {format(nrow(disturb_long), big.mark=',')} rows -> ",
           "{file_size(out_disturb)}\n\n"))
  rm(all_cond_d, disturb_long); gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Step 6: plot_damage_agents
# Collect per-state damage_agents parquets and join agent code labels
# ------------------------------------------------------------------------------

cat("Step 6: plot_damage_agents\n")
out_damage_ag <- file.path(out_dir, "plot_damage_agents.parquet")

if (file_exists(out_damage_ag)) {
  cat(glue("  Already exists ({file_size(out_damage_ag)}) - skipping\n\n"))
} else {
  da_ds <- tryCatch(
    open_dataset(here(proc_fia$damage_agents$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(da_ds)) {
    cat("  No damage_agents parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    # Inline damage agent lookup — category codes + ~30 high-profile species
    # Source: FIADB v9.4 Appendix H (PTIPS/FHAAST codes)
    ref_damage_agent <- data.table(
      DAMAGE_AGENT_CD = c(
        # Category-level codes
        10000L, 11000L, 12000L, 13000L, 14000L, 15000L, 16000L, 17000L, 18000L,
        19000L, 20000L, 21000L, 22000L, 22500L, 23000L, 24000L, 25000L, 26000L, 27000L,
        30000L, 30001L, 30002L, 30003L, 30004L,
        41000L, 42000L, 50000L, 60000L, 70000L, 71000L, 80000L, 90000L, 99000L,
        # Named bark beetle species (11xxx)
        11003L, 11006L, 11007L, 11009L, 11010L, 11019L, 11023L, 11029L, 11045L,
        11800L, 11900L, 11999L,
        # Named defoliators (12xxx)
        12038L, 12039L, 12040L, 12041L, 12083L, 12089L, 12096L, 12197L,
        12800L, 12900L,
        # Sucking insects (14xxx)
        14003L, 14004L, 14016L, 14800L, 14900L,
        # Boring insects (15xxx)
        15082L, 15087L, 15090L, 15800L, 15900L,
        # Root/butt diseases (21xxx)
        21001L, 21010L, 21014L, 21017L, 21019L, 21028L, 21800L, 21900L,
        # Cankers & rusts (22xxx, 26xxx)
        22023L, 22042L, 22086L, 22300L,
        26001L, 26009L, 26800L,
        # Other disease (24xxx, 25xxx)
        24021L, 24022L, 24031L, 24800L,
        25040L, 25043L, 25800L
      ),
      agent_label = c(
        "General insects", "Bark beetles", "Defoliators", "Chewing insects",
        "Sucking insects (adelgids/scales/aphids)", "Boring insects",
        "Seed/cone insects", "Gallmakers", "Insect predators",
        "General diseases", "Biotic damage", "Root/butt diseases",
        "Cankers (non-rust)", "Stem decay", "Parasitic/epiphytic plants",
        "Decline complexes/dieback/wilts", "Foliage diseases",
        "Stem rusts", "Broom rusts",
        "Fire", "Wildfire", "Human-caused fire", "Crown fire", "Ground fire",
        "Wild animals", "Domestic animals", "Abiotic damage",
        "Competition", "Human activities", "Harvest",
        "Multi-damage insect/disease complex", "Other damages", "Unknown",
        # Bark beetles
        "Southern pine beetle", "Mountain pine beetle", "Douglas-fir beetle",
        "Spruce beetle", "Eastern larch beetle", "Pinon ips",
        "Southern pine engraver", "Pine engraver", "Small European elm bark beetle",
        "Other bark beetle (known)", "Unknown bark beetle", "Western bark beetle complex",
        # Defoliators
        "Spruce budworm", "Western pine budworm", "Western spruce budworm",
        "Jack pine budworm", "Hemlock looper", "Gypsy moth",
        "Forest tent caterpillar", "Winter moth",
        "Other defoliator (known)", "Unknown defoliator",
        # Sucking insects
        "Balsam woolly adelgid", "Hemlock woolly adelgid", "Beech scale",
        "Other sucking insect (known)", "Unknown sucking insect",
        # Boring insects
        "Asian longhorned beetle", "Emerald ash borer", "Sirex woodwasp",
        "Other boring insect (known)", "Unknown boring insect",
        # Root/butt diseases
        "Armillaria root disease", "Heterobasidion root disease",
        "Black stain root disease", "Laminated root rot",
        "Phytophthora root rot / littleleaf disease", "Sudden oak death",
        "Other root/butt disease (known)", "Unknown root/butt disease",
        # Cankers & rusts
        "Chestnut blight", "Beech bark disease",
        "Thousand cankers disease", "Other canker (known)",
        "White pine blister rust", "Fusiform rust", "Other stem rust (known)",
        # Other disease
        "Oak wilt", "Dutch elm disease", "Laurel wilt", "Other decline/wilt (known)",
        "Dothistroma needle blight", "Swiss needle cast", "Other foliage disease (known)"
      ),
      agent_category = c(
        "insects", "insects", "insects", "insects", "insects", "insects",
        "insects", "insects", "insects",
        "disease", "disease", "disease", "disease", "disease", "disease",
        "disease", "disease", "disease", "disease",
        "fire", "fire", "fire", "fire", "fire",
        "animal", "animal", "abiotic", "competition", "human", "human",
        "complex", "other", "unknown",
        # Bark beetles
        rep("bark beetles", 12),
        # Defoliators
        rep("defoliators", 10),
        # Sucking insects
        rep("sucking insects", 5),
        # Boring insects
        rep("boring insects", 5),
        # Root/butt diseases
        rep("root/butt disease", 8),
        # Cankers & rusts
        rep("canker/rust", 7),
        # Other disease
        rep("foliage/wilt disease", 7)
      )
    )
    setkey(ref_damage_agent, DAMAGE_AGENT_CD)

    all_da <- da_ds |> collect() |> as.data.table()
    setkey(all_da, DAMAGE_AGENT_CD)
    # Left join: keep all records, label known codes, leave others as NA
    all_da <- ref_damage_agent[all_da, on = "DAMAGE_AGENT_CD"]

    write_parquet(as_tibble(all_da), out_damage_ag, compression = "snappy")
    cat(glue("  plot_damage_agents: {format(nrow(all_da), big.mark=',')} rows -> ",
             "{file_size(out_damage_ag)}\n\n"))
    rm(all_da); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Done
# ------------------------------------------------------------------------------

cat("FIA summaries complete.\n\n")
cat("Outputs:\n")
for (f in c(out_tree_metrics, out_seed_metrics, out_mort_metrics,
            out_cond_metrics, out_disturb, out_damage_ag)) {
  if (file_exists(f)) cat(glue("  {basename(f)}: {file_size(f)}\n"))
}
cat("\nRead with:\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_tree_metrics.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_disturbance_history.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_damage_agents.parquet')\n")
