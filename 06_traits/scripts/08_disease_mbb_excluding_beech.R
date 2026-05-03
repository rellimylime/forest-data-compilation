# ==============================================================================
# 08_disease_mbb_excluding_beech.R
# Re-compute the maple-beech-birch (MBB) disease vs control CWM comparison
# with American beech (SPCD 531) removed from both sides.
#
# Why this script exists:
#   The previous script (07) showed the +0.74 deg C MBB disease signal is
#   driven almost entirely by beech itself increasing share (+0.33), via the
#   beech bark / beech leaf disease sprouting response. This is mathematically
#   a thermophilization signal (beech is warmer-affinity than the cool conifers
#   and sugar maple losing share) but ecologically it is a single-species
#   disease response, not climate-driven species turnover.
#
#   Removing beech from both disturbed and control plots and re-computing the
#   CWM tells us whether anything else is shifting:
#     - If the residual signal is ~0, the +0.74 deg C is purely beech sprouts.
#     - If a residual positive signal remains, there is a real climate-driven
#       species turnover underneath the beech sprout response.
#
# Inputs:
#   06_traits/data/processed/plot_matches.parquet
#     Source for MBB disease plots and their matched controls.
#   05_fia/data/processed/summaries/plot_seedling_species.parquet
#     Per-species seedling counts at those plots.
#   06_traits/data/processed/species_climate_affinity.parquet
#     Species-level temp_mean, precip_mean, cwd_mean used to recompute CWM.
#
# Output:
#   06_traits/data/processed/disease_mbb_excluding_beech_summary.parquet
#     One row with the original (with-beech) and beech-excluded CWM Deltas
#     side by side, plus diagnostics for how many plots had to be dropped.
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

MBB_FOREST_TYPE_GROUP <- 800L
BEECH_SPCD            <- 531L
MIN_OCCURRENCES       <- 30L  # match the trait filter from script 02
N_BOOT                <- 1000L
ALPHA                 <- 0.05
SEED                  <- 42L

cat("MBB Disease vs Control CWM, beech-excluded sensitivity\n")
cat("=======================================================\n\n")

# ------------------------------------------------------------------------------
# 1. Identify MBB disease plots and their matched controls
# ------------------------------------------------------------------------------

cat("[1/5] Selecting MBB disease plots and matched controls...\n")

matches <- read_parquet(here("06_traits/data/processed/plot_matches.parquet")) |>
  as.data.table()

mbb <- matches[disturbance_class == "disease" &
               region_east_west  == "East" &
               forest_type_group == MBB_FOREST_TYPE_GROUP]

dist_keys <- unique(mbb[, .(side = "disturbed",
                            PLT_CN = dist_PLT_CN,
                            INVYR  = dist_INVYR,
                            CONDID = dist_CONDID,
                            disturbed_id)])
ctrl_keys <- unique(mbb[, .(side = "control",
                            PLT_CN = ctrl_PLT_CN,
                            INVYR  = ctrl_INVYR,
                            CONDID = ctrl_CONDID,
                            disturbed_id)])

cat(glue("       MBB disease plots: {format(uniqueN(dist_keys$PLT_CN), big.mark=',')}\n"))
cat(glue("       distinct matched control plots: ",
         "{format(uniqueN(ctrl_keys$PLT_CN), big.mark=',')}\n"))
cat(glue("       (disturbed, control) pairs: ",
         "{format(nrow(matches[disturbance_class == 'disease' & ",
         "region_east_west == 'East' & forest_type_group == MBB_FOREST_TYPE_GROUP]), big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 2. Load per-condition seedling species counts
# ------------------------------------------------------------------------------

cat("[2/5] Loading seedling species records...\n")

seed <- read_parquet(here("05_fia/data/processed/summaries/plot_seedling_species.parquet")) |>
  as.data.table()
seed <- seed[!is.na(treecount_total) & treecount_total > 0,
             .(PLT_CN, INVYR, CONDID, SPCD, treecount_total)]

# Aggregate across SUBP within each condition x species, matching the grain
# used by the original CWM builder (script 02).
seed_cond <- seed[, .(seedling_count = sum(treecount_total, na.rm = TRUE)),
                  by = .(PLT_CN, INVYR, CONDID, SPCD)]
rm(seed); gc(verbose = FALSE)

# ------------------------------------------------------------------------------
# 3. Join species traits and compute per-condition CWM, with vs without beech
# ------------------------------------------------------------------------------

cat("[3/5] Computing per-condition CWMs with and without beech...\n")

traits <- read_parquet(here("06_traits/data/processed/species_climate_affinity.parquet")) |>
  as.data.table()
traits_keep <- traits[n_occurrences >= MIN_OCCURRENCES,
                      .(SPCD, temp_mean, precip_mean, cwd_mean)]

setkey(seed_cond, SPCD)
setkey(traits_keep, SPCD)
seed_traits <- traits_keep[seed_cond, on = "SPCD"]

# Drop seedling rows with no usable trait (matches script 02's behavior).
trait_subset <- seed_traits[!is.na(temp_mean)]

cwm_one <- function(dt, exclude_beech = FALSE) {
  if (exclude_beech) dt <- dt[SPCD != BEECH_SPCD]
  dt[, .(
    n_seedlings = sum(seedling_count, na.rm = TRUE),
    cwm_temp    = sum(temp_mean   * seedling_count) / sum(seedling_count),
    cwm_precip  = sum(precip_mean * seedling_count) / sum(seedling_count),
    cwm_cwd     = sum(cwd_mean    * seedling_count) / sum(seedling_count)
  ), by = .(PLT_CN, INVYR, CONDID)]
}

cwm_with_beech <- cwm_one(trait_subset, exclude_beech = FALSE)
cwm_no_beech   <- cwm_one(trait_subset, exclude_beech = TRUE)

# ------------------------------------------------------------------------------
# 4. Build per-pair Deltas, with and without beech
# ------------------------------------------------------------------------------
# Recompute Deltas from scratch instead of subtracting from script 02 outputs,
# because the no-beech CWM may be NA on plots whose only seedlings were beech.

cat("[4/5] Building per-pair Deltas...\n")

# All MBB-disease (disturbed, control) pairs.
pairs <- matches[disturbance_class == "disease" &
                 region_east_west  == "East" &
                 forest_type_group == MBB_FOREST_TYPE_GROUP,
                 .(disturbed_id, match_rank,
                   d_PLT_CN = dist_PLT_CN, d_INVYR = dist_INVYR, d_CONDID = dist_CONDID,
                   c_PLT_CN = ctrl_PLT_CN, c_INVYR = ctrl_INVYR, c_CONDID = ctrl_CONDID)]

attach_cwm <- function(dt, side) {
  side_col <- paste0(substr(side, 1, 1), "_")
  cwm_d <- copy(cwm_with_beech)
  cwm_n <- copy(cwm_no_beech)
  setnames(cwm_d, c("PLT_CN", "INVYR", "CONDID"),
           paste0(side_col, c("PLT_CN", "INVYR", "CONDID")))
  setnames(cwm_n, c("PLT_CN", "INVYR", "CONDID"),
           paste0(side_col, c("PLT_CN", "INVYR", "CONDID")))
  setnames(cwm_d, c("cwm_temp", "cwm_precip", "cwm_cwd", "n_seedlings"),
           paste0(side_col, c("cwm_temp", "cwm_precip", "cwm_cwd", "n_seedlings"),
                  "_with"))
  setnames(cwm_n, c("cwm_temp", "cwm_precip", "cwm_cwd", "n_seedlings"),
           paste0(side_col, c("cwm_temp", "cwm_precip", "cwm_cwd", "n_seedlings"),
                  "_no"))
  dt <- merge(dt, cwm_d,
              by = paste0(side_col, c("PLT_CN", "INVYR", "CONDID")),
              all.x = TRUE)
  dt <- merge(dt, cwm_n,
              by = paste0(side_col, c("PLT_CN", "INVYR", "CONDID")),
              all.x = TRUE)
  dt
}

pairs <- attach_cwm(pairs, "d")
pairs <- attach_cwm(pairs, "c")

# Per-pair deltas.
pairs[, delta_temp_with := d_cwm_temp_with - c_cwm_temp_with]
pairs[, delta_temp_no   := d_cwm_temp_no   - c_cwm_temp_no]
pairs[, delta_cwd_with  := d_cwm_cwd_with  - c_cwm_cwd_with]
pairs[, delta_cwd_no    := d_cwm_cwd_no    - c_cwm_cwd_no]

# Collapse K matched controls into one synthetic control per disturbed plot,
# matching the analysis grain used in script 04.
per_dist <- pairs[, .(
  delta_temp_with = mean(delta_temp_with, na.rm = TRUE),
  delta_temp_no   = mean(delta_temp_no,   na.rm = TRUE),
  delta_cwd_with  = mean(delta_cwd_with,  na.rm = TRUE),
  delta_cwd_no    = mean(delta_cwd_no,    na.rm = TRUE),
  any_no_beech_dropped = any(is.na(d_cwm_temp_no) | is.na(c_cwm_temp_no))
), by = disturbed_id]

n_pairs_total       <- nrow(pairs)
n_pairs_drop_no     <- sum(is.na(pairs$delta_temp_no))
n_dist_total        <- nrow(per_dist)
n_dist_drop_no      <- sum(is.na(per_dist$delta_temp_no))

cat(glue("       pairs total: {format(n_pairs_total, big.mark=',')}\n"))
cat(glue("       pairs dropped in beech-excluded run (one side had only beech): ",
         "{format(n_pairs_drop_no, big.mark=',')}\n"))
cat(glue("       disturbed plots with usable beech-excluded delta: ",
         "{format(n_dist_total - n_dist_drop_no, big.mark=',')} of ",
         "{format(n_dist_total, big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 5. Bootstrap 95% CIs and write summary
# ------------------------------------------------------------------------------

cat("[5/5] Bootstrapping CIs and writing summary...\n")

boot_ci <- function(x, n_boot = N_BOOT, alpha = ALPHA) {
  x <- x[!is.na(x)]
  n <- length(x)
  if (n < 2L) return(list(mean = if (n == 1L) x else NA_real_,
                          lo = NA_real_, hi = NA_real_, n = n))
  means <- numeric(n_boot)
  for (b in seq_len(n_boot)) {
    means[b] <- mean(x[sample.int(n, n, replace = TRUE)])
  }
  list(mean = mean(x),
       lo   = unname(quantile(means, alpha / 2)),
       hi   = unname(quantile(means, 1 - alpha / 2)),
       n    = n)
}

set.seed(SEED)

ci_temp_with <- boot_ci(per_dist$delta_temp_with)
ci_temp_no   <- boot_ci(per_dist$delta_temp_no)
ci_cwd_with  <- boot_ci(per_dist$delta_cwd_with)
ci_cwd_no    <- boot_ci(per_dist$delta_cwd_no)

summary_tbl <- data.table(
  metric        = c("delta_temp", "delta_temp",
                    "delta_cwd",  "delta_cwd"),
  variant       = c("with_beech", "beech_excluded",
                    "with_beech", "beech_excluded"),
  n_plots       = c(ci_temp_with$n, ci_temp_no$n,
                    ci_cwd_with$n,  ci_cwd_no$n),
  mean          = round(c(ci_temp_with$mean, ci_temp_no$mean,
                          ci_cwd_with$mean,  ci_cwd_no$mean), 3),
  ci_lo         = round(c(ci_temp_with$lo, ci_temp_no$lo,
                          ci_cwd_with$lo,  ci_cwd_no$lo), 3),
  ci_hi         = round(c(ci_temp_with$hi, ci_temp_no$hi,
                          ci_cwd_with$hi,  ci_cwd_no$hi), 3)
)

cat("\n--- MBB disease delta CWM, with vs without beech ---\n")
print(summary_tbl)

# Plain-language reading
shrink_temp <- ci_temp_with$mean - ci_temp_no$mean
cat(glue("\nReading: removing beech reduces the warming signal by ",
         "{round(shrink_temp, 3)} deg C.\n"))
if (!is.na(ci_temp_no$mean) && abs(ci_temp_no$mean) < 0.05) {
  cat("Beech accounts for essentially the entire signal.\n")
} else if (!is.na(ci_temp_no$mean) && ci_temp_no$lo > 0) {
  cat("A residual positive signal remains after removing beech, so there is",
      "a real climate-driven turnover layered on top of the beech sprout",
      "response.\n", sep = " ")
} else if (!is.na(ci_temp_no$mean) && ci_temp_no$hi < 0) {
  cat("Removing beech flips the signal to negative -- without the beech",
      "sprout response, MBB disease plots actually recruit cooler-affinity",
      "species than matched controls.\n", sep = " ")
} else {
  cat("After removing beech, the remaining signal is not distinguishable",
      "from zero.\n", sep = " ")
}

out_dir  <- here("06_traits/data/processed")
out_file <- file.path(out_dir, "disease_mbb_excluding_beech_summary.parquet")
dir_create(out_dir)
write_parquet(as_tibble(summary_tbl), out_file, compression = "snappy")

cat(glue("\nDone. Output: {out_file}\n"))
