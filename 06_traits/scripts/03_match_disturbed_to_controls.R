# ==============================================================================
# 03_match_disturbed_to_controls.R
# For each disturbed FIA condition, find K nearest control conditions in the
# same forest type group + region (East/West) + INVYR window, scored by
# baseline-climate similarity.
#
# Inputs:
#   05_fia/data/processed/site_climate/site_climate.parquet
#     Monthly TerraClimate; aggregated here to per-site 1981-2010 baseline.
#   05_fia/data/processed/summaries/plot_disturbance_classification.parquet
#     Per (PLT_CN, INVYR, CONDID) disturbance class + control eligibility +
#     forest_type_group + region_east_west + stable_plot_id.
#   06_traits/data/processed/plot_recruitment_cwm.parquet
#     Per (PLT_CN, INVYR, CONDID) recruitment CWM (temp, precip, cwd).
#
# Output:
#   06_traits/data/processed/plot_matches.parquet
#     One row per (disturbed condition, matched control). Carries both sides'
#     CWMs so downstream stratified summaries compute Delta directly.
#
# Matching design:
#   - Hard match: forest_type_group, region_east_west.
#   - INVYR window: |INVYR_dist - INVYR_ctrl| <= INVYR_WINDOW (default 5 yr).
#   - Climate similarity: Euclidean distance in globally-standardized
#     (site_temp_mean, site_precip_mean) space.
#   - Caliper: reject controls more than CALIPER_SD (default 2.0) away. A
#     disturbed plot with no controls inside the caliper is reported as
#     unmatched, not force-matched to a poor control.
#   - K = 5 nearest controls per disturbed condition.
#   - With replacement: same control may serve multiple disturbed plots. This
#     prioritizes match quality over independence; downstream uncertainty is
#     handled by bootstrap CIs.
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

source(here("scripts/utils/load_config.R"))
config <- load_config()

K_CONTROLS     <- 5L
INVYR_WINDOW   <- 5L
CALIPER_SD     <- 2.0
BASELINE_START <- 1981L
BASELINE_END   <- 2010L

cat("Disturbed-to-Control Matching\n")
cat("=============================\n\n")
cat(glue("K = {K_CONTROLS}, INVYR window = +/-{INVYR_WINDOW} yr, ",
         "caliper = {CALIPER_SD} SD\n\n"))

# ------------------------------------------------------------------------------
# 1. Site baseline climate (one row per site_id)
# ------------------------------------------------------------------------------
# Same recipe as 01_build_species_climate_affinity.R: monthly tmmx/tmmn averaged
# to monthly tmean, then annual temp = mean of months, annual precip = sum of
# months. Final per-site baseline = mean across the 30 baseline years.

cat("[1/5] Building site baseline climate...\n")

sc_path <- here("05_fia/data/processed/site_climate/site_climate.parquet")

sc <- open_dataset(sc_path) |>
  filter(year >= BASELINE_START, year <= BASELINE_END,
         variable %in% c("tmmx", "tmmn", "pr")) |>
  select(site_id, year, month, variable, value) |>
  collect() |>
  as.data.table()

sc_wide <- dcast(sc, site_id + year + month ~ variable, value.var = "value")
rm(sc); gc(verbose = FALSE)

sc_wide[, tmean_month := (tmmx + tmmn) / 2]

annual <- sc_wide[, .(temp_annual   = mean(tmean_month, na.rm = TRUE),
                      precip_annual = sum(pr,           na.rm = TRUE)),
                  by = .(site_id, year)]
rm(sc_wide); gc(verbose = FALSE)

site_baseline <- annual[, .(site_temp_mean   = mean(temp_annual,   na.rm = TRUE),
                            site_precip_mean = mean(precip_annual, na.rm = TRUE)),
                        by = site_id]
rm(annual)

cat(glue("       sites with baseline: {nrow(site_baseline)}\n"))

# ------------------------------------------------------------------------------
# 2. Load disturbance classification + CWM, join climate
# ------------------------------------------------------------------------------

cat("[2/5] Loading disturbance classification and recruitment CWM...\n")

dist_path <- here("05_fia/data/processed/summaries/plot_disturbance_classification.parquet")
cwm_path  <- here("06_traits/data/processed/plot_recruitment_cwm.parquet")

dist_cols <- c("PLT_CN", "INVYR", "CONDID", "stable_plot_id",
               "forest_type_group", "region_east_west",
               "disturbance_class", "disturbance_class_primary",
               "is_high_severity_proxy", "disturbed_vs_control",
               "is_control_candidate", "is_natural_disturbance_candidate",
               "disturbance_year_latest", "time_since_disturbance")

dist <- read_parquet(dist_path) |> as.data.table()
dist <- dist[, ..dist_cols]

cwm <- read_parquet(cwm_path) |> as.data.table()

# Inner join: only conditions with both classification AND a recruitment CWM
# can participate (need seedlings to compute thermophilization).
plots <- merge(dist, cwm, by = c("PLT_CN", "INVYR", "CONDID"))
rm(dist, cwm); gc(verbose = FALSE)

plots <- merge(plots, site_baseline,
               by.x = "stable_plot_id", by.y = "site_id",
               all.x = TRUE)

cat(glue("       conditions with class + CWM: {nrow(plots)}\n"))
cat(glue("       with baseline climate joined: ",
         "{sum(!is.na(plots$site_temp_mean))}\n"))

# ------------------------------------------------------------------------------
# 3. Build eligible disturbed and control pools
# ------------------------------------------------------------------------------

elig <- plots[disturbed_vs_control %in% c("disturbed", "control") &
              !is.na(site_temp_mean) & !is.na(site_precip_mean) &
              !is.na(forest_type_group) & forest_type_group != "" &
              !is.na(region_east_west)]
rm(plots); gc(verbose = FALSE)

# Standardize climate globally so the caliper has a single, interpretable scale.
temp_mu <- mean(elig$site_temp_mean)
temp_sd <- sd(elig$site_temp_mean)
prec_mu <- mean(elig$site_precip_mean)
prec_sd <- sd(elig$site_precip_mean)
elig[, temp_z := (site_temp_mean   - temp_mu) / temp_sd]
elig[, prec_z := (site_precip_mean - prec_mu) / prec_sd]

n_dist <- sum(elig$disturbed_vs_control == "disturbed")
n_ctrl <- sum(elig$disturbed_vs_control == "control")
cat(glue("       eligible disturbed: {format(n_dist, big.mark=',')}\n"))
cat(glue("       eligible controls:  {format(n_ctrl, big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 4. Match within (forest_type_group x region) strata
# ------------------------------------------------------------------------------

cat("[3/5] Matching within forest_type_group x region strata...\n")

disturbed <- elig[disturbed_vs_control == "disturbed"]
controls  <- elig[disturbed_vs_control == "control"]
rm(elig); gc(verbose = FALSE)

disturbed[, disturbed_id := paste(PLT_CN, INVYR, CONDID, sep = "_")]
controls[, control_id   := paste(PLT_CN, INVYR, CONDID, sep = "_")]

caliper_sq <- CALIPER_SD^2

match_one_stratum <- function(d, c) {
  # d, c are data.tables already filtered to a single stratum.
  # Returns a data.table of matched (disturbed_id, control_id, ...) rows.
  out_rows <- vector("list", nrow(d))
  for (i in seq_len(nrow(d))) {
    di <- d[i]
    cand <- c[abs(INVYR - di$INVYR) <= INVYR_WINDOW]
    if (nrow(cand) == 0L) next

    dist_sq <- (cand$temp_z - di$temp_z)^2 + (cand$prec_z - di$prec_z)^2
    keep <- dist_sq <= caliper_sq
    if (!any(keep)) next

    cand <- cand[keep]
    dist_sq <- dist_sq[keep]
    ord <- order(dist_sq)
    take <- head(ord, K_CONTROLS)

    matched <- cand[take]
    matched[, `:=`(
      disturbed_id   = di$disturbed_id,
      match_distance = sqrt(dist_sq[take]),
      match_rank     = seq_along(take)
    )]
    out_rows[[i]] <- matched
  }
  rbindlist(out_rows, fill = TRUE)
}

strata <- unique(disturbed[, .(forest_type_group, region_east_west)])
setorder(strata, forest_type_group, region_east_west)

matches_list <- vector("list", nrow(strata))
for (s in seq_len(nrow(strata))) {
  ftg <- strata$forest_type_group[s]
  reg <- strata$region_east_west[s]
  d_s <- disturbed[forest_type_group == ftg & region_east_west == reg]
  c_s <- controls[ forest_type_group == ftg & region_east_west == reg]
  cat(glue("       [{s}/{nrow(strata)}] {ftg} | {reg}: ",
           "{nrow(d_s)} disturbed, {nrow(c_s)} controls\n"))
  if (nrow(c_s) == 0L) next
  matches_list[[s]] <- match_one_stratum(d_s, c_s)
}
matches <- rbindlist(matches_list, fill = TRUE)
rm(matches_list); gc(verbose = FALSE)

# ------------------------------------------------------------------------------
# 5. Decorate output (one row per matched pair, both sides' CWMs attached)
# ------------------------------------------------------------------------------

cat("[4/5] Building output table...\n")

dist_side <- disturbed[, .(
  disturbed_id,
  dist_PLT_CN          = PLT_CN,
  dist_INVYR           = INVYR,
  dist_CONDID          = CONDID,
  dist_stable_plot_id  = stable_plot_id,
  forest_type_group, region_east_west,
  disturbance_class, disturbance_class_primary,
  is_high_severity_proxy,
  disturbance_year_latest, time_since_disturbance,
  dist_cwm_temp        = cwm_temp,
  dist_cwm_precip      = cwm_precip,
  dist_cwm_cwd         = cwm_cwd,
  dist_n_seedlings     = n_seedlings_with_traits,
  dist_frac_with_traits = frac_seedlings_with_traits,
  dist_temp_z          = temp_z,
  dist_prec_z          = prec_z
)]

ctrl_side <- matches[, .(
  disturbed_id, control_id, match_rank, match_distance,
  ctrl_PLT_CN          = PLT_CN,
  ctrl_INVYR           = INVYR,
  ctrl_CONDID          = CONDID,
  ctrl_stable_plot_id  = stable_plot_id,
  ctrl_cwm_temp        = cwm_temp,
  ctrl_cwm_precip      = cwm_precip,
  ctrl_cwm_cwd         = cwm_cwd,
  ctrl_n_seedlings     = n_seedlings_with_traits,
  ctrl_temp_z          = temp_z,
  ctrl_prec_z          = prec_z
)]

out <- merge(ctrl_side, dist_side, by = "disturbed_id")
rm(dist_side, ctrl_side, matches); gc(verbose = FALSE)

# Pre-compute the Deltas so downstream stratified summaries don't redo it.
out[, delta_cwm_temp   := dist_cwm_temp   - ctrl_cwm_temp]
out[, delta_cwm_precip := dist_cwm_precip - ctrl_cwm_precip]
out[, delta_cwm_cwd    := dist_cwm_cwd    - ctrl_cwm_cwd]

setcolorder(out, c(
  "disturbed_id", "control_id", "match_rank", "match_distance",
  "forest_type_group", "region_east_west",
  "disturbance_class", "disturbance_class_primary", "is_high_severity_proxy",
  "disturbance_year_latest", "time_since_disturbance",
  "dist_PLT_CN", "dist_INVYR", "dist_CONDID", "dist_stable_plot_id",
  "ctrl_PLT_CN", "ctrl_INVYR", "ctrl_CONDID", "ctrl_stable_plot_id",
  "dist_cwm_temp", "ctrl_cwm_temp", "delta_cwm_temp",
  "dist_cwm_precip", "ctrl_cwm_precip", "delta_cwm_precip",
  "dist_cwm_cwd",  "ctrl_cwm_cwd",  "delta_cwm_cwd",
  "dist_n_seedlings", "ctrl_n_seedlings",
  "dist_frac_with_traits",
  "dist_temp_z", "dist_prec_z", "ctrl_temp_z", "ctrl_prec_z"
))

# Diagnostics
matched_disturbed <- uniqueN(out$disturbed_id)
unmatched <- n_dist - matched_disturbed
cat(glue("       disturbed total:           {format(n_dist, big.mark=',')}\n"))
cat(glue("       disturbed with >=1 match:  {format(matched_disturbed, big.mark=',')} ",
         "({round(100 * matched_disturbed / n_dist, 1)}%)\n"))
cat(glue("       unmatched (no controls in caliper / window): ",
         "{format(unmatched, big.mark=',')}\n"))
cat(glue("       avg matches per matched disturbed: ",
         "{round(nrow(out) / matched_disturbed, 2)}\n"))
cat(glue("       median match distance (SD units): ",
         "{round(median(out$match_distance, na.rm = TRUE), 3)}\n"))

# Per-class match coverage
class_summary <- out[, .(
  n_pairs     = .N,
  n_disturbed = uniqueN(disturbed_id),
  median_dist = round(median(match_distance, na.rm = TRUE), 3)
), by = .(disturbance_class, region_east_west)][order(disturbance_class, region_east_west)]
cat("\nPer-class match coverage:\n")
print(class_summary)

# ------------------------------------------------------------------------------
# 6. Write
# ------------------------------------------------------------------------------

cat("\n[5/5] Writing matched pairs...\n")

out_dir  <- here("06_traits/data/processed")
out_file <- file.path(out_dir, "plot_matches.parquet")
dir_create(out_dir)

write_parquet(as_tibble(out), out_file, compression = "snappy")

cat("\nDone.\n")
cat(glue("Output: {out_file} ({file_size(out_file)})\n"))
cat(glue("Rows: {format(nrow(out), big.mark=',')}\n"))
