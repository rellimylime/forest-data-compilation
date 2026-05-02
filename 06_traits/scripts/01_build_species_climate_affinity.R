# ==============================================================================
# 01_build_species_climate_affinity.R
# Build species-level realized-climate envelopes for FIA tree species.
#
# For each FIA tree species (SPCD), summarises the 1981-2010 baseline climate
# at every FIA plot location where the species occurs as a live tree. Produces
# mean / p10 / p90 / min / max for annual mean temperature, annual precipitation,
# and annual climate water deficit (CWD). Also records sample size diagnostics.
#
# Inputs:
#   05_fia/data/processed/site_climate/site_climate.parquet
#     Long-format monthly TerraClimate (1958-present) keyed by site_id, where
#     site_id = stable_plot_id (set by 06a_build_fia_site_list.R).
#   05_fia/data/processed/cond/state={ST}/...
#     PLT_CN -> stable_plot_id + STATECD lookup.
#   05_fia/data/processed/trees/state={ST}/...
#     Per-plot live tree presence by species (STATUSCD == 1).
#   05_fia/lookups/ref_species.parquet
#     Species names and softwood/hardwood/woodland labels.
#
# Output:
#   06_traits/data/processed/species_climate_affinity.parquet
#     One row per (species_source, species_code). Trait columns are unfiltered;
#     downstream models should impose a minimum n_occurrences threshold before
#     using rare-species traits.
#
# Usage:
#   Rscript 06_traits/scripts/01_build_species_climate_affinity.R
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

source(here("scripts/utils/load_config.R"))
config <- load_config()

BASELINE_START <- 1981L
BASELINE_END   <- 2010L
TRAIT_METHOD   <- "fia_occurrence_climate_1981_2010"
SPECIES_SOURCE <- "FIA_SPCD"

cat("Species Climate-Affinity Traits\n")
cat("================================\n\n")
cat(glue("Baseline window: {BASELINE_START}-{BASELINE_END}\n\n"))

# ------------------------------------------------------------------------------
# 1. Site-level baseline climate (one row per site_id)
# ------------------------------------------------------------------------------
# Read monthly site_climate, filter to baseline window and the four variables we
# need (tmmx, tmmn, pr, def), then pivot wide so each row is one site x month.
# Annual summaries: mean of monthly mean-temp for temperature, sum of monthly
# values for precipitation and CWD. Final per-site baseline is the mean across
# the 30 baseline years.

cat("[1/5] Aggregating site climate to 1981-2010 baseline...\n")

sc_path <- here("05_fia/data/processed/site_climate/site_climate.parquet")

sc <- open_dataset(sc_path) |>
  filter(
    year >= BASELINE_START, year <= BASELINE_END,
    variable %in% c("tmmx", "tmmn", "pr", "def")
  ) |>
  select(site_id, year, month, variable, value) |>
  collect() |>
  as.data.table()

sc_wide <- dcast(sc, site_id + year + month ~ variable, value.var = "value")
rm(sc); gc(verbose = FALSE)

# Monthly mean temperature is the average of TerraClimate's monthly tmmx / tmmn.
sc_wide[, tmean_month := (tmmx + tmmn) / 2]

annual <- sc_wide[, .(
  temp_annual   = mean(tmean_month, na.rm = TRUE),
  precip_annual = sum(pr,           na.rm = TRUE),
  cwd_annual    = sum(def,          na.rm = TRUE)
), by = .(site_id, year)]
rm(sc_wide); gc(verbose = FALSE)

site_baseline <- annual[, .(
  site_temp_mean   = mean(temp_annual,   na.rm = TRUE),
  site_precip_mean = mean(precip_annual, na.rm = TRUE),
  site_cwd_mean    = mean(cwd_annual,    na.rm = TRUE),
  n_baseline_years = .N
), by = site_id]
rm(annual)

cat(glue("       sites with baseline climate: {nrow(site_baseline)}\n"))

# ------------------------------------------------------------------------------
# 2. PLT_CN -> stable_plot_id + STATECD, joined to site climate
# ------------------------------------------------------------------------------
# site_id is set to stable_plot_id by 06a_build_fia_site_list.R, so the join is
# direct. cond has multiple rows per PLT_CN (one per CONDID); collapse to one.

cat("[2/5] Mapping plot visits to baseline climate...\n")

cond_meta <- open_dataset(
  here(config$processed$fia$cond$output_dir),
  partitioning = "state"
) |>
  select(PLT_CN, stable_plot_id, STATECD) |>
  distinct() |>
  collect() |>
  as.data.table()

setkey(cond_meta, PLT_CN)
plot_climate <- site_baseline[cond_meta, on = c(site_id = "stable_plot_id"),
                              nomatch = NULL]
setnames(plot_climate, "site_id", "stable_plot_id")

cat(glue("       PLT_CNs with climate: {nrow(plot_climate)}\n"))

# ------------------------------------------------------------------------------
# 3. Live-tree occurrences
# ------------------------------------------------------------------------------
# Trees extract is already aggregated; we only need binary occurrence per
# (PLT_CN, SPCD) for live trees. Inner-join to plot_climate so any plots
# without baseline climate are silently dropped.

cat("[3/5] Collecting live-tree occurrences...\n")

trees <- open_dataset(
  here(config$processed$fia$trees$output_dir),
  partitioning = "state"
) |>
  filter(STATUSCD == 1) |>
  select(PLT_CN, SPCD) |>
  distinct() |>
  collect() |>
  as.data.table()

cat(glue("       distinct (PLT_CN, SPCD) live-tree rows: {nrow(trees)}\n"))

setkey(trees, PLT_CN)
occ <- plot_climate[trees, on = "PLT_CN", nomatch = NULL]

# Equal weight per plot location: dedupe to (stable_plot_id, SPCD) so a
# revisited plot does not double-count toward a species' envelope.
occ_unique <- unique(occ, by = c("stable_plot_id", "SPCD"))
rm(trees, occ); gc(verbose = FALSE)

cat(glue("       distinct (stable_plot_id, SPCD) occurrences: {nrow(occ_unique)}\n"))

# ------------------------------------------------------------------------------
# 4. Per-species envelope statistics
# ------------------------------------------------------------------------------

cat("[4/5] Computing per-species envelopes...\n")

q10 <- function(x) quantile(x, 0.10, na.rm = TRUE, names = FALSE)
q90 <- function(x) quantile(x, 0.90, na.rm = TRUE, names = FALSE)

trait_stats <- occ_unique[, .(
  n_occurrences = .N,
  n_states      = uniqueN(STATECD),

  temp_mean = mean(site_temp_mean, na.rm = TRUE),
  temp_p10  = q10(site_temp_mean),
  temp_p90  = q90(site_temp_mean),
  temp_min  = min(site_temp_mean, na.rm = TRUE),
  temp_max  = max(site_temp_mean, na.rm = TRUE),

  precip_mean = mean(site_precip_mean, na.rm = TRUE),
  precip_p10  = q10(site_precip_mean),
  precip_p90  = q90(site_precip_mean),
  precip_min  = min(site_precip_mean, na.rm = TRUE),
  precip_max  = max(site_precip_mean, na.rm = TRUE),

  cwd_mean = mean(site_cwd_mean, na.rm = TRUE),
  cwd_p10  = q10(site_cwd_mean),
  cwd_p90  = q90(site_cwd_mean),
  cwd_min  = min(site_cwd_mean, na.rm = TRUE),
  cwd_max  = max(site_cwd_mean, na.rm = TRUE)
), by = SPCD]
rm(occ_unique); gc(verbose = FALSE)

cat(glue("       species with traits: {nrow(trait_stats)}\n"))

# ------------------------------------------------------------------------------
# 5. Join species labels and write
# ------------------------------------------------------------------------------

cat("[5/5] Joining species labels and writing trait table...\n")

species <- open_dataset(here("05_fia/lookups/ref_species.parquet")) |>
  select(SPCD, COMMON_NAME, SCIENTIFIC_NAME, GENUS, SPECIES,
         SFTWD_HRDWD, WOODLAND) |>
  collect() |>
  as.data.table()

trait_table <- species[trait_stats, on = "SPCD"]
trait_table[, `:=`(
  species_source    = SPECIES_SOURCE,
  species_code      = as.character(SPCD),
  occurrence_period = paste0(BASELINE_START, "-", BASELINE_END),
  trait_method      = TRAIT_METHOD
)]

setcolorder(trait_table, c(
  "species_source", "species_code", "SPCD",
  "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
  "SFTWD_HRDWD", "WOODLAND",
  "n_occurrences", "n_states",
  "occurrence_period", "trait_method"
))

out_dir  <- here("06_traits/data/processed")
out_file <- file.path(out_dir, "species_climate_affinity.parquet")
dir_create(out_dir)

write_parquet(as_tibble(trait_table), out_file, compression = "snappy")

cat("\nDone.\n")
cat(glue("Output: {out_file} ({file_size(out_file)})\n"))
cat(glue("Rows: {nrow(trait_table)}\n"))
