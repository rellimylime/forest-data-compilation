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
# Freshness / rebuild contract.
#
# The implementation is shared with the other numbered stages and lives in
# scripts/utils/build_freshness.R. These wrappers keep the fia_ names and the
# "fia_force_rebuild" option that the 05 orchestrator sets from --force, so
# every existing summary builder keeps working unchanged.
# ------------------------------------------------------------------------------
source(here::here("scripts/utils/build_freshness.R"))

# TRUE when a rebuild was forced for this product (via --force / --force=<name>).
fia_force_requested <- function(label) {
  build_force_requested(label, option = "fia_force_rebuild")
}

fia_should_rebuild <- function(out_path,
                               input_paths = character(0),
                               required_cols = NULL,
                               force = NULL,
                               label = basename(out_path)) {
  build_should_rebuild(
    out_path = out_path,
    input_paths = input_paths,
    required_cols = required_cols,
    force = force,
    label = label,
    option = "fia_force_rebuild"
  )
}

# ------------------------------------------------------------------------------

