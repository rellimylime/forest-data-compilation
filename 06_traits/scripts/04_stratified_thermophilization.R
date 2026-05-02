# ==============================================================================
# 04_stratified_thermophilization.R
# Stratified thermophilization summaries with bootstrap 95% CIs.
#
# For each disturbed FIA condition, the K matched controls are collapsed into
# one synthetic control (mean of the K). The thermophilization index is then
# Delta = dist_cwm - synthetic_ctrl_cwm, computed for temperature, precip, and
# CWD. The unit of analysis is the disturbed plot (not the matched pair),
# because the K controls share a single matched plot and so are not independent.
#
# Output (3 tables):
#   06_traits/data/processed/thermophilization_by_class_region.parquet
#     Mean Delta + bootstrap 95% CI for each (disturbance_class x region) cell.
#   06_traits/data/processed/thermophilization_high_severity.parquet
#     Same, restricted to high-severity (crown fire) disturbances by region.
#   06_traits/data/processed/thermophilization_by_time_region.parquet
#     Same, binned by time_since_disturbance (0-5, 5-10, 10-20, 20+ yr).
#
# Sign convention:
#   delta_temp   > 0  -> recruits favor warmer-climate species
#   delta_precip > 0  -> recruits favor wetter-climate species
#   delta_cwd    > 0  -> recruits favor higher-deficit (drier) species
#   The boss's hypothesis is positive delta_temp and/or positive delta_cwd in
#   the West, especially after high-severity fire.
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

N_BOOT <- 1000L
ALPHA  <- 0.05
SEED   <- 42L

cat("Stratified Thermophilization Summary\n")
cat("====================================\n\n")
cat(glue("Bootstrap: {N_BOOT} resamples, {(1 - ALPHA) * 100}% CI\n\n"))

# ------------------------------------------------------------------------------
# 1. Load matches and collapse to one row per disturbed plot
# ------------------------------------------------------------------------------

cat("[1/4] Loading matches and aggregating to per-disturbed-plot deltas...\n")

matches <- read_parquet(here("06_traits/data/processed/plot_matches.parquet")) |>
  as.data.table()

# Each matched pair already has delta_cwm_* = dist_cwm - ctrl_cwm. Mean over the
# K matched controls gives the per-disturbed-plot Delta vs the synthetic control.
per_dist <- matches[, .(
  forest_type_group         = first(forest_type_group),
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

cat(glue("       disturbed plots with deltas: {format(nrow(per_dist), big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 2. Bootstrap helper
# ------------------------------------------------------------------------------
# Returns mean and percentile CI. NAs in x dropped before resampling.

boot_ci <- function(x, n_boot = N_BOOT, alpha = ALPHA) {
  x <- x[!is.na(x)]
  n <- length(x)
  if (n < 2L) {
    return(list(mean = if (n == 1L) x else NA_real_, lo = NA_real_, hi = NA_real_, n = n))
  }
  means <- numeric(n_boot)
  for (b in seq_len(n_boot)) {
    means[b] <- mean(x[sample.int(n, n, replace = TRUE)])
  }
  list(
    mean = mean(x),
    lo   = unname(quantile(means, alpha / 2)),
    hi   = unname(quantile(means, 1 - alpha / 2)),
    n    = n
  )
}

# Compact wrapper that returns a 1-row data.table with bootstrap stats for all
# three metrics. Used inside data.table by-group expressions.
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
# 3. Three stratified summaries
# ------------------------------------------------------------------------------

cat("[2/4] Stratified summary: disturbance_class x region...\n")
class_region <- per_dist[, boot_three(.SD), by = .(disturbance_class, region_east_west)]
setorder(class_region, disturbance_class, region_east_west)

cat("[3/4] High-severity (crown fire) only, by region...\n")
high_sev <- per_dist[is_high_severity_proxy == TRUE,
                     boot_three(.SD), by = .(region_east_west)]
setorder(high_sev, region_east_west)

cat("[4/4] Time-since-disturbance bins x region...\n")
per_dist[, time_bin := fcase(
  is.na(time_since_disturbance),                      "unknown",
  time_since_disturbance >= 0L  & time_since_disturbance < 5L,  "0-5_yr",
  time_since_disturbance >= 5L  & time_since_disturbance < 10L, "5-10_yr",
  time_since_disturbance >= 10L & time_since_disturbance < 20L, "10-20_yr",
  time_since_disturbance >= 20L,                      "20+_yr",
  default = "unknown"
)]
time_region <- per_dist[time_bin != "unknown",
                        boot_three(.SD), by = .(time_bin, region_east_west)]
time_bin_order <- c("0-5_yr", "5-10_yr", "10-20_yr", "20+_yr")
time_region[, time_bin := factor(time_bin, levels = time_bin_order)]
setorder(time_region, time_bin, region_east_west)

# ------------------------------------------------------------------------------
# 4. Write + print
# ------------------------------------------------------------------------------

out_dir <- here("06_traits/data/processed")
dir_create(out_dir)

write_parquet(as_tibble(class_region),
              file.path(out_dir, "thermophilization_by_class_region.parquet"),
              compression = "snappy")
write_parquet(as_tibble(high_sev),
              file.path(out_dir, "thermophilization_high_severity.parquet"),
              compression = "snappy")
write_parquet(as_tibble(time_region),
              file.path(out_dir, "thermophilization_by_time_region.parquet"),
              compression = "snappy")

# Pretty-print rounded results so the headline numbers are readable in stdout.
fmt <- function(dt) {
  num_cols <- setdiff(names(dt), c("disturbance_class", "region_east_west",
                                   "time_bin", "n_plots"))
  out <- copy(dt)
  for (col in num_cols) set(out, j = col, value = round(out[[col]], 3))
  out
}

cat("\n--- Thermophilization by disturbance class x region ---\n")
print(fmt(class_region))

cat("\n--- High-severity (crown fire) only, by region ---\n")
print(fmt(high_sev))

cat("\n--- Time-since-disturbance x region ---\n")
print(fmt(time_region))

cat("\nDone.\n")
cat(glue("Outputs written to: {out_dir}/\n"))
cat("  thermophilization_by_class_region.parquet\n")
cat("  thermophilization_high_severity.parquet\n")
cat("  thermophilization_by_time_region.parquet\n")
