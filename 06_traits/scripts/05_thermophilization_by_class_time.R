# ==============================================================================
# 05_thermophilization_by_class_time.R
# Stratified thermophilization Deltas by (disturbance_class x region x time bin).
#
# The first-pass time-bin output from 04_stratified_thermophilization.R pooled
# all disturbance classes within each time bin, which masked the signal because
# fire (positive delta_temp) and insect (negative delta_temp) cancel. This
# script keeps the same per-disturbed-plot Delta unit of analysis but adds
# disturbance_class to the by-stratum grouping so each row is one
# (class x region x time bin) cell.
#
# Inputs:
#   06_traits/data/processed/plot_matches.parquet
#     One row per (disturbed condition, matched control) with pre-computed
#     delta_cwm_temp, delta_cwm_precip, delta_cwm_cwd and
#     time_since_disturbance.
#
# Outputs:
#   06_traits/data/processed/thermophilization_by_class_time_region.parquet
#     Mean Delta + bootstrap 95% CI for each (disturbance_class x region x
#     time_bin) cell.
#   06_traits/data/processed/disturbance_year_coverage.parquet
#     Per (disturbance_class x region) breakdown of how many disturbed plots
#     have a usable disturbance_year_latest vs how many are NA. Diagnostic
#     for the "~32K plots have unknown time" caveat from the plan.
#
# Sign convention as in 04_stratified_thermophilization.R:
#   delta_temp > 0   -> recruits favor warmer-climate species
#   delta_cwd  > 0   -> recruits favor higher-deficit (drier) species
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
MIN_N_CELL  <- 10L  # cells with fewer than this many disturbed plots are still
                    # written but flagged so plotting code can grey them out.

cat("Stratified Thermophilization: class x region x time\n")
cat("====================================================\n\n")
cat(glue("Bootstrap: {N_BOOT} resamples, {(1 - ALPHA) * 100}% CI\n"))
cat(glue("Cells with n < {MIN_N_CELL} are flagged but kept\n\n"))

# ------------------------------------------------------------------------------
# 1. Load matches and collapse to one Delta per disturbed plot
# ------------------------------------------------------------------------------

cat("[1/4] Loading matches and aggregating to per-disturbed-plot deltas...\n")

matches <- read_parquet(here("06_traits/data/processed/plot_matches.parquet")) |>
  as.data.table()

per_dist <- matches[, .(
  region_east_west          = first(region_east_west),
  disturbance_class         = first(disturbance_class),
  disturbance_class_primary = first(disturbance_class_primary),
  is_high_severity_proxy    = first(is_high_severity_proxy),
  time_since_disturbance    = first(time_since_disturbance),
  n_matched_controls        = .N,
  delta_temp                = mean(delta_cwm_temp,   na.rm = TRUE),
  delta_precip              = mean(delta_cwm_precip, na.rm = TRUE),
  delta_cwd                 = mean(delta_cwm_cwd,    na.rm = TRUE)
), by = disturbed_id]

cat(glue("       disturbed plots: {format(nrow(per_dist), big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 2. Disturbance-year coverage diagnostic
# ------------------------------------------------------------------------------
# Reports how often time_since_disturbance is NA per (class x region). If a
# given cell is mostly NA, the time-binned summary for that cell is unreliable.

cat("[2/4] Building disturbance-year coverage diagnostic...\n")

coverage <- per_dist[, .(
  n_total          = .N,
  n_known_year     = sum(!is.na(time_since_disturbance)),
  n_unknown_year   = sum(is.na(time_since_disturbance)),
  frac_known_year  = sum(!is.na(time_since_disturbance)) / .N
), by = .(disturbance_class, region_east_west)]
setorder(coverage, disturbance_class, region_east_west)

# ------------------------------------------------------------------------------
# 3. Bootstrap helpers (same recipe as script 04)
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

# ------------------------------------------------------------------------------
# 4. Bin time and stratify by class x region x time_bin
# ------------------------------------------------------------------------------

cat("[3/4] Binning time and computing stratified deltas...\n")

per_dist[, time_bin := fcase(
  is.na(time_since_disturbance),                                              "unknown",
  time_since_disturbance >= 0L  & time_since_disturbance < 5L,                "0-5_yr",
  time_since_disturbance >= 5L  & time_since_disturbance < 10L,               "5-10_yr",
  time_since_disturbance >= 10L & time_since_disturbance < 20L,               "10-20_yr",
  time_since_disturbance >= 20L,                                              "20+_yr",
  default = "unknown"
)]

set.seed(SEED)

class_time <- per_dist[time_bin != "unknown",
                       boot_three(.SD),
                       by = .(disturbance_class, region_east_west, time_bin)]
class_time[, sparse_cell := n_plots < MIN_N_CELL]

time_bin_order <- c("0-5_yr", "5-10_yr", "10-20_yr", "20+_yr")
class_time[, time_bin := factor(time_bin, levels = time_bin_order)]
setorder(class_time, disturbance_class, region_east_west, time_bin)

# ------------------------------------------------------------------------------
# 5. Write + print
# ------------------------------------------------------------------------------

cat("[4/4] Writing outputs and printing summary...\n")

out_dir <- here("06_traits/data/processed")
dir_create(out_dir)

write_parquet(as_tibble(class_time),
              file.path(out_dir, "thermophilization_by_class_time_region.parquet"),
              compression = "snappy")
write_parquet(as_tibble(coverage),
              file.path(out_dir, "disturbance_year_coverage.parquet"),
              compression = "snappy")

fmt <- function(dt) {
  num_cols <- setdiff(names(dt), c("disturbance_class", "region_east_west",
                                   "time_bin", "n_plots", "sparse_cell",
                                   "n_total", "n_known_year", "n_unknown_year"))
  out <- copy(dt)
  for (col in num_cols) {
    if (is.numeric(out[[col]])) set(out, j = col, value = round(out[[col]], 3))
  }
  out
}

cat("\n--- Disturbance-year coverage (fraction of disturbed plots with known year) ---\n")
print(fmt(coverage))

cat("\n--- Thermophilization by class x region x time bin ---\n")
print(fmt(class_time), nrows = 100)

cat("\nDone.\n")
cat(glue("Outputs:\n"))
cat(glue("  {out_dir}/thermophilization_by_class_time_region.parquet\n"))
cat(glue("  {out_dir}/disturbance_year_coverage.parquet\n"))
