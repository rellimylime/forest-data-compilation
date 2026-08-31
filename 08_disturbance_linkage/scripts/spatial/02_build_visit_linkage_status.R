#!/usr/bin/env Rscript

# Build one coordinate-status row per FIA plot visit in the condition-backed
# foundational visit set. Spatial linkage is allowed only when a stable plot has
# exactly one distinct usable public coordinate across all of its visits.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
source_path <- here(link_cfg$inputs$fia_plot_source)
output_path <- here(
  link_cfg$output_dir,
  link_cfg$files$fia_visit_spatial_linkage_status
)
footprints_path <- here(
  link_cfg$output_dir,
  link_cfg$files$plot_footprints
)

if (!file.exists(source_path)) {
  stop("FIA plot source is missing: ", source_path)
}
if (!file.exists(footprints_path)) {
  stop(
    "Existing plot footprints are missing. Build them before coordinate status: ",
    footprints_path
  )
}

# Start from condition-backed visits so spatial status joins directly to FIA products.
visits <- as.data.table(read_parquet(
  source_path,
  col_select = c("stable_plot_id", "PLT_CN", "INVYR", "LAT", "LON")
))
visits <- unique(visits)

# Reject visits whose conditions disagree about their public coordinate.
visit_conflicts <- visits[, .N, by = .(stable_plot_id, PLT_CN, INVYR)][N > 1L]
if (nrow(visit_conflicts) > 0L) {
  stop(
    "The FIA plot source has multiple coordinate records for ",
    nrow(visit_conflicts),
    " stable_plot_id x PLT_CN x INVYR visits."
  )
}

visits[, has_usable_public_coordinate :=
  !is.na(LAT) & !is.na(LON) &
  between(LAT, -90, 90) & between(LON, -180, 180)]

# FIA coordinates in this physical source are stored at six decimal places.
# Formatting before comparison makes that documented physical precision—not
# floating-point representation—the coordinate identity rule.
visits[, coordinate_pair_6dp := fifelse(
  has_usable_public_coordinate,
  paste(sprintf("%.6f", LAT), sprintf("%.6f", LON), sep = ","),
  NA_character_
)]

# Apply the coordinate decision to the entire stable plot, not one visit at a time.
plot_status <- visits[, {
  pairs <- sort(unique(coordinate_pair_6dp[!is.na(coordinate_pair_6dp)]))
  n_pairs <- length(pairs)
  if (n_pairs == 1L) {
    values <- strsplit(pairs, ",", fixed = TRUE)[[1]]
    linkage_lat <- as.numeric(values[[1]])
    linkage_lon <- as.numeric(values[[2]])
  } else {
    linkage_lat <- NA_real_
    linkage_lon <- NA_real_
  }
  list(
    n_distinct_public_coordinates = n_pairs,
    linkage_LAT = linkage_lat,
    linkage_LON = linkage_lon
  )
}, by = stable_plot_id]

plot_status[, `:=`(
  eligible_spatial_linkage = n_distinct_public_coordinates == 1L,
  spatial_linkage_exclusion_reason = fcase(
    n_distinct_public_coordinates == 0L, "no_usable_public_coordinate",
    n_distinct_public_coordinates > 1L, "multiple_public_coordinate_pairs",
    default = NA_character_
  )
)]

# Copy the stable-plot decision back to every visit for simple downstream joins.
status <- merge(
  visits,
  plot_status,
  by = "stable_plot_id",
  all.x = TRUE,
  sort = FALSE
)
status[, coordinate_pair_6dp := NULL]
setcolorder(status, c(
  "stable_plot_id", "PLT_CN", "INVYR", "LAT", "LON",
  "has_usable_public_coordinate", "linkage_LAT", "linkage_LON",
  "n_distinct_public_coordinates", "eligible_spatial_linkage",
  "spatial_linkage_exclusion_reason"
))
setorder(status, stable_plot_id, INVYR, PLT_CN)

# Reuse, but verify, the existing 800 m footprint product.
# Confirm that the existing footprint file follows the same eligibility rule.
footprint_ids <- st_read(
  footprints_path,
  query = "SELECT stable_plot_id FROM plot_footprints",
  quiet = TRUE
)$stable_plot_id
eligible_ids <- plot_status[
  eligible_spatial_linkage == TRUE,
  stable_plot_id
]
if (!setequal(as.character(footprint_ids), as.character(eligible_ids))) {
  stop(
    "Existing plot footprints do not match the single-coordinate stable plots. ",
    "Rebuild plot_footprints.gpkg before proceeding."
  )
}

write_parquet_atomic(status, output_path)

cat("FIA visit spatial-linkage status\n")
cat(glue("Rows (plot visits): {format(nrow(status), big.mark = ',')}\n"))
cat(glue(
  "Stable plots: {format(uniqueN(status$stable_plot_id), big.mark = ',')}\n"
))
cat(glue(
  "Eligible stable plots: {format(length(eligible_ids), big.mark = ',')}\n"
))
cat(glue(
  "Excluded multi-coordinate plots: ",
  "{format(plot_status[n_distinct_public_coordinates > 1L, .N], big.mark = ',')}\n"
))
cat(glue(
  "Excluded plots without a usable coordinate: ",
  "{format(plot_status[n_distinct_public_coordinates == 0L, .N], big.mark = ',')}\n"
))
cat(glue("Wrote: {output_path}\n"))
