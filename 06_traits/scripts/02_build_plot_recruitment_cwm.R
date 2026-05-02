# ==============================================================================
# 02_build_plot_recruitment_cwm.R
# Build per-condition community-weighted means (CWM) of recruiting seedlings'
# climate-affinity traits.
#
# For each FIA condition (PLT_CN, INVYR, CONDID), takes the recruiting seedling
# species, weights each species by its raw seedling count, and computes the
# community-weighted mean of mean annual temperature, precipitation, and
# climate water deficit (CWD) using species-level realized-climate envelopes.
#
# These CWM trait values are the per-plot thermophilization indices that
# downstream analyses compare between disturbed plots and matched controls.
#
# Inputs:
#   05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet
#     Per-(PLT_CN, INVYR, CONDID, SUBP, SPCD) seedling counts. Aggregated to
#     condition x species before CWM.
#   06_traits/data/processed/species_climate_affinity.parquet
#     Per-species mean annual temp / precip / CWD from 1981-2010 baseline at
#     occurrence plots.
#
# Output:
#   06_traits/data/processed/plot_recruitment_cwm.parquet
#     One row per (PLT_CN, INVYR, CONDID). Columns include:
#       cwm_temp, cwm_precip, cwm_cwd            : weighted means
#       n_species_total, n_species_with_traits   : richness diagnostics
#       n_seedlings_total, n_seedlings_with_traits, frac_seedlings_with_traits
#       min_n_occurrences_used                   : trait-noise filter applied
#
# Trait quality filter:
#   Species with n_occurrences < MIN_OCCURRENCES (default 30) are excluded from
#   the CWM, since their trait estimates are too noisy. The fraction of
#   seedling abundance retained is reported per condition.
#
# Usage:
#   Rscript 06_traits/scripts/02_build_plot_recruitment_cwm.R
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

source(here("scripts/utils/load_config.R"))
config <- load_config()

MIN_OCCURRENCES <- 30L

cat("Per-Plot Recruitment CWM Traits\n")
cat("================================\n\n")
cat(glue("Trait filter: drop species with n_occurrences < {MIN_OCCURRENCES}\n\n"))

# ------------------------------------------------------------------------------
# 1. Species traits
# ------------------------------------------------------------------------------

cat("[1/4] Loading species climate-affinity traits...\n")

traits_path <- here("06_traits/data/processed/species_climate_affinity.parquet")
traits <- read_parquet(traits_path) |> as.data.table()

traits_keep <- traits[n_occurrences >= MIN_OCCURRENCES,
                      .(SPCD, n_occurrences,
                        temp_mean, precip_mean, cwd_mean)]

cat(glue("       species available: {nrow(traits)}\n"))
cat(glue("       species passing n_occurrences >= {MIN_OCCURRENCES}: {nrow(traits_keep)}\n"))

# ------------------------------------------------------------------------------
# 2. Seedling counts per (PLT_CN, INVYR, CONDID, SPCD)
# ------------------------------------------------------------------------------
# Source seedling product is per (PLT_CN, INVYR, CONDID, SUBP, SPCD); sum across
# SUBP so each species appears once per condition. Use treecount_total (raw
# microplot count) as the abundance weight.

cat("[2/4] Aggregating seedling counts per condition x species...\n")

seed_ds <- open_dataset(
  here(config$processed$fia$seedlings$output_dir),
  partitioning = "state"
)

seed <- seed_ds |>
  select(PLT_CN, INVYR, CONDID, SPCD, treecount_total) |>
  collect() |>
  as.data.table()

seed <- seed[!is.na(treecount_total) & treecount_total > 0]

seed_cond <- seed[, .(seedling_count = sum(treecount_total, na.rm = TRUE)),
                  by = .(PLT_CN, INVYR, CONDID, SPCD)]
rm(seed); gc(verbose = FALSE)

cat(glue("       (PLT_CN, INVYR, CONDID, SPCD) rows: {nrow(seed_cond)}\n"))

# ------------------------------------------------------------------------------
# 3. Join traits and compute CWM per condition
# ------------------------------------------------------------------------------
# Left-join traits so we can report which seedlings had no trait. Then split
# into trait/no-trait for the diagnostics, and CWM only over the trait subset.

cat("[3/4] Computing CWM per condition...\n")

setkey(seed_cond, SPCD)
setkey(traits_keep, SPCD)
seed_traits <- traits_keep[seed_cond, on = "SPCD"]

# Per-condition totals (all species, regardless of trait availability).
totals <- seed_traits[, .(
  n_species_total   = uniqueN(SPCD),
  n_seedlings_total = sum(seedling_count, na.rm = TRUE)
), by = .(PLT_CN, INVYR, CONDID)]

# CWM uses only species with usable traits.
trait_subset <- seed_traits[!is.na(temp_mean)]

cwm <- trait_subset[, .(
  n_species_with_traits   = uniqueN(SPCD),
  n_seedlings_with_traits = sum(seedling_count, na.rm = TRUE),
  cwm_temp   = sum(temp_mean   * seedling_count) / sum(seedling_count),
  cwm_precip = sum(precip_mean * seedling_count) / sum(seedling_count),
  cwm_cwd    = sum(cwd_mean    * seedling_count) / sum(seedling_count)
), by = .(PLT_CN, INVYR, CONDID)]

plot_cwm <- totals[cwm, on = c("PLT_CN", "INVYR", "CONDID")]
plot_cwm[, frac_seedlings_with_traits := n_seedlings_with_traits / n_seedlings_total]
plot_cwm[, min_n_occurrences_used := MIN_OCCURRENCES]

setcolorder(plot_cwm, c(
  "PLT_CN", "INVYR", "CONDID",
  "n_species_total", "n_species_with_traits",
  "n_seedlings_total", "n_seedlings_with_traits", "frac_seedlings_with_traits",
  "cwm_temp", "cwm_precip", "cwm_cwd",
  "min_n_occurrences_used"
))

cat(glue("       conditions with CWM: {nrow(plot_cwm)}\n"))
cat(glue("       median frac of seedlings retained: ",
         "{round(median(plot_cwm$frac_seedlings_with_traits, na.rm = TRUE), 3)}\n"))

# ------------------------------------------------------------------------------
# 4. Write
# ------------------------------------------------------------------------------

cat("[4/4] Writing plot recruitment CWM table...\n")

out_dir  <- here("06_traits/data/processed")
out_file <- file.path(out_dir, "plot_recruitment_cwm.parquet")
dir_create(out_dir)

write_parquet(as_tibble(plot_cwm), out_file, compression = "snappy")

cat("\nDone.\n")
cat(glue("Output: {out_file} ({file_size(out_file)})\n"))
cat(glue("Rows: {nrow(plot_cwm)}\n"))
