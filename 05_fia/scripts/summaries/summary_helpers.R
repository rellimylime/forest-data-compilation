# ------------------------------------------------------------------------------
# Shared summary helpers
# ------------------------------------------------------------------------------

# Helper: Shannon diversity index
# dt must have group_cols + value_col (one row per species within group)
# Returns data.table with group_cols + shannon_h
# ------------------------------------------------------------------------------

compute_shannon_h <- function(dt, group_cols, value_col) {
  # Work on a copy so helper columns do not modify the caller's data.table.
  dt <- copy(dt)

  # Compute total abundance within each plot visit before calculating proportions.
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]

  # Convert each species or stratum value to a relative abundance.
  dt[total > 0, p_i := get(value_col) / total]

  # Keep Shannon contributions only where the proportion is valid and positive.
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]

  # Sum species contributions back to one diversity value per plot visit.
  dt[, .(shannon_h = sum(h_i, na.rm = TRUE)), by = group_cols]
}

# Sum optional fields while preserving NA when the whole field is unavailable.
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

# ------------------------------------------------------------------------------

