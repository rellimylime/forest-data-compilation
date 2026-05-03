# ==============================================================================
# 06_disease_east_drilldown.R
# Investigate the disease-East thermophilization signal (delta_temp +0.42 deg C
# in script 04 results).
#
# Hypothesis: the signal is concentrated in a small number of forest types
# (e.g. ash, hemlock, beech) being killed by introduced pests (emerald ash
# borer, hemlock woolly adelgid, beech bark disease) and replaced by warmer-
# affinity recruits. If true, the signal will look much stronger inside a few
# forest_type_group cells, and the top recruiting species in those cells will
# be different from the species being killed (or reflect canopy gap responders
# like red maple, yellow-poplar, sweetgum).
#
# Inputs:
#   06_traits/data/processed/plot_matches.parquet
#     Per (disturbed, control) pair with deltas + forest_type_group +
#     disturbance_class_primary. Source for the per-disturbed-plot Delta.
#   05_fia/data/processed/summaries/plot_seedling_species.parquet
#     Per (PLT_CN, INVYR, CONDID, SUBP, SPCD) seedling counts with species
#     names. Source for "what is recruiting at disease-East plots."
#   06_traits/data/processed/species_climate_affinity.parquet
#     Per-species temp_mean, used to tag top recruits as warm/cool affinity.
#
# Outputs:
#   06_traits/data/processed/disease_east_by_forest_type.parquet
#     Per (forest_type_group x disturbance_class_primary), bootstrap mean +
#     95% CI of delta_temp / delta_precip / delta_cwd, restricted to East
#     disease cells. Identifies which forest types carry the signal.
#   06_traits/data/processed/disease_east_top_recruits.parquet
#     Top 20 recruiting species (by total seedling count) at East disease
#     plots, with their species-level temp_mean for context.
#
# Sign convention as elsewhere: delta_temp > 0 means recruits favor warmer-
# climate species relative to matched controls.
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

N_BOOT      <- 1000L
ALPHA       <- 0.05
SEED        <- 42L
TOP_N       <- 20L
MIN_N_CELL  <- 10L

cat("Disease-East Drilldown\n")
cat("======================\n\n")

# ------------------------------------------------------------------------------
# 1. Load matches and aggregate to per-disturbed-plot deltas
# ------------------------------------------------------------------------------

cat("[1/5] Loading matches and aggregating to per-disturbed-plot deltas...\n")

matches <- read_parquet(here("06_traits/data/processed/plot_matches.parquet")) |>
  as.data.table()

per_dist <- matches[, .(
  dist_PLT_CN               = first(dist_PLT_CN),
  dist_INVYR                = first(dist_INVYR),
  dist_CONDID               = first(dist_CONDID),
  forest_type_group         = first(forest_type_group),
  region_east_west          = first(region_east_west),
  disturbance_class         = first(disturbance_class),
  disturbance_class_primary = first(disturbance_class_primary),
  delta_temp                = mean(delta_cwm_temp,   na.rm = TRUE),
  delta_precip              = mean(delta_cwm_precip, na.rm = TRUE),
  delta_cwd                 = mean(delta_cwm_cwd,    na.rm = TRUE)
), by = disturbed_id]

# Restrict everything below to East disease.
de <- per_dist[disturbance_class == "disease" & region_east_west == "East"]
cat(glue("       disease-East plots: {format(nrow(de), big.mark=',')}\n"))
cat(glue("       distinct forest_type_group cells: ",
         "{uniqueN(de$forest_type_group)}\n"))

# ------------------------------------------------------------------------------
# 2. Bootstrap helpers (same as scripts 04/05)
# ------------------------------------------------------------------------------

boot_ci <- function(x, n_boot = N_BOOT, alpha = ALPHA) {
  x <- x[!is.na(x)]
  n <- length(x)
  if (n < 2L) {
    return(list(mean = if (n == 1L) x else NA_real_,
                lo = NA_real_, hi = NA_real_, n = n))
  }
  means <- numeric(n_boot)
  for (b in seq_len(n_boot)) {
    means[b] <- mean(x[sample.int(n, n, replace = TRUE)])
  }
  list(mean = mean(x),
       lo   = unname(quantile(means, alpha / 2)),
       hi   = unname(quantile(means, 1 - alpha / 2)),
       n    = n)
}

boot_three <- function(dt) {
  ct <- boot_ci(dt$delta_temp)
  cp <- boot_ci(dt$delta_precip)
  cd <- boot_ci(dt$delta_cwd)
  list(
    n_plots          = nrow(dt),
    delta_temp_mean  = ct$mean,  delta_temp_lo  = ct$lo,  delta_temp_hi  = ct$hi,
    delta_precip_mean= cp$mean,  delta_precip_lo= cp$lo,  delta_precip_hi= cp$hi,
    delta_cwd_mean   = cd$mean,  delta_cwd_lo   = cd$lo,  delta_cwd_hi   = cd$hi
  )
}

set.seed(SEED)

# ------------------------------------------------------------------------------
# 3. Stratify by forest_type_group x disturbance_class_primary
# ------------------------------------------------------------------------------
# disturbance_class "disease" collapses primary classes 20/21/22; the primary
# code distinguishes general disease vs specific subcategories. Worth keeping
# both groupings in case all the signal sits in one primary code.

cat("[2/5] Stratifying disease-East by forest_type_group x primary class...\n")

ftg_table <- de[, boot_three(.SD), by = .(forest_type_group, disturbance_class_primary)]
ftg_table[, sparse_cell := n_plots < MIN_N_CELL]
setorder(ftg_table, -delta_temp_mean)

# Top forest type groups by signal magnitude (filter sparse cells out for the
# headline ranking; they are still in the parquet output).
cat("       headline cells (n >= ", MIN_N_CELL, ", ranked by delta_temp_mean):\n", sep = "")
print(ftg_table[sparse_cell == FALSE,
                .(forest_type_group, disturbance_class_primary, n_plots,
                  delta_temp_mean = round(delta_temp_mean, 3),
                  delta_temp_lo   = round(delta_temp_lo,   3),
                  delta_temp_hi   = round(delta_temp_hi,   3),
                  delta_cwd_mean  = round(delta_cwd_mean,  2))],
      nrows = 20L)

# ------------------------------------------------------------------------------
# 4. Top recruiting species at East disease plots
# ------------------------------------------------------------------------------
# Pulls the seedling species product, restricts to the disease-East disturbed
# conditions identified above, ranks by total seedling count, and joins the
# species temp_mean trait so each top recruit can be tagged warm/cool.

cat("[3/5] Loading seedling species at disease-East disturbed conditions...\n")

# Use the dist_* columns to identify the disease-East disturbed conditions.
de_keys <- de[, .(PLT_CN = dist_PLT_CN, INVYR = dist_INVYR, CONDID = dist_CONDID)]
setkey(de_keys, PLT_CN, INVYR, CONDID)

seed_species_path <- here("05_fia/data/processed/summaries/plot_seedling_species.parquet")
seed_species <- read_parquet(seed_species_path) |> as.data.table()

# Inner-restrict to the disease-East disturbed conditions.
setkey(seed_species, PLT_CN, INVYR, CONDID)
de_seed <- seed_species[de_keys, nomatch = NULL]
rm(seed_species); gc(verbose = FALSE)

cat(glue("       seedling rows at disease-East plots: ",
         "{format(nrow(de_seed), big.mark=',')}\n"))

# Aggregate per species across all disease-East plots.
de_species <- de_seed[!is.na(treecount_total) & treecount_total > 0, .(
  total_seedlings   = sum(treecount_total, na.rm = TRUE),
  n_plots_with_spp  = uniqueN(paste(PLT_CN, INVYR, CONDID))
), by = .(SPCD, COMMON_NAME, SCIENTIFIC_NAME)]
setorder(de_species, -total_seedlings)

# ------------------------------------------------------------------------------
# 5. Join species traits for warm/cool tagging
# ------------------------------------------------------------------------------

cat("[4/5] Joining species climate-affinity traits...\n")

traits <- read_parquet(here("06_traits/data/processed/species_climate_affinity.parquet")) |>
  as.data.table()
traits_keep <- traits[, .(SPCD, n_occurrences,
                          species_temp_mean   = temp_mean,
                          species_precip_mean = precip_mean,
                          species_cwd_mean    = cwd_mean)]

de_species <- merge(de_species, traits_keep, by = "SPCD", all.x = TRUE)

# Rank to take the top N for the headline output.
de_top <- de_species[seq_len(min(TOP_N, .N))]

# Compare top recruits' temp affinity to the national tree-species median so the
# warm/cool label has a defensible reference.
nat_median_temp <- median(traits$temp_mean, na.rm = TRUE)
de_top[, temp_vs_national_median := round(species_temp_mean - nat_median_temp, 2)]

cat(glue("       national median species temp_mean (reference): ",
         "{round(nat_median_temp, 2)} deg C\n\n"))

cat("       Top ", TOP_N, " recruiting species at disease-East plots:\n", sep = "")
print(de_top[, .(SPCD, COMMON_NAME, total_seedlings, n_plots_with_spp,
                 species_temp_mean = round(species_temp_mean, 2),
                 temp_vs_national_median,
                 n_occurrences)])

# ------------------------------------------------------------------------------
# 6. Write
# ------------------------------------------------------------------------------

cat("\n[5/5] Writing outputs...\n")

out_dir <- here("06_traits/data/processed")
dir_create(out_dir)

write_parquet(as_tibble(ftg_table),
              file.path(out_dir, "disease_east_by_forest_type.parquet"),
              compression = "snappy")
write_parquet(as_tibble(de_species),
              file.path(out_dir, "disease_east_top_recruits.parquet"),
              compression = "snappy")

cat("\nDone.\n")
cat(glue("Outputs:\n"))
cat(glue("  {out_dir}/disease_east_by_forest_type.parquet\n"))
cat(glue("  {out_dir}/disease_east_top_recruits.parquet\n"))
