#!/usr/bin/env Rscript

# Validate the downloaded national MTBS Burn Area Boundary shapefile and write
# the canonical GeoPackage consumed by the event-linkage workflow. The original
# ZIP and extracted shapefile remain unchanged as source evidence.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
source_cfg <- link_cfg$mtbs$source
archive_path <- here(link_cfg$inputs$mtbs_fire_perimeters_archive)
shapefile_path <- here(link_cfg$inputs$mtbs_fire_perimeters_shapefile)
output_path <- here(link_cfg$inputs$mtbs_fire_perimeters)
qa_path <- here(link_cfg$qa_dir, "mtbs_perimeter_source_validation.csv")

# Require both the downloaded archive and its extracted vector layer.
for (path in c(archive_path, shapefile_path)) {
  if (!file.exists(path)) stop("Required MTBS source is missing: ", path)
}
if (!requireNamespace("openssl", quietly = TRUE)) {
  stop("Package 'openssl' is required to validate the MTBS archive SHA-256.")
}

# Verify that the local archive is the exact configured source snapshot.
archive_hash <- as.character(openssl::sha256(file(archive_path)))
if (!isTRUE(
  unname(toupper(archive_hash)) == unname(toupper(source_cfg$archive_sha256))
)) {
  stop(
    "MTBS archive SHA-256 does not match config. Expected ",
    source_cfg$archive_sha256,
    "; found ", archive_hash
  )
}

cat("Reading national MTBS burned-area boundaries...\n")
# Read the source without changing its geometry or attributes.
fires <- st_read(shapefile_path, quiet = TRUE)
required_fields <- c(
  "FIRE_ID", "FIRE_NAME", "YEAR", "FIRE_TYPE", "ACRES", "MAP_ID",
  "MAP_PROG", "IG_DATE"
)
missing_fields <- setdiff(required_fields, names(fires))
if (length(missing_fields) > 0L) {
  stop("MTBS source fields missing: ", paste(missing_fields, collapse = ", "))
}
if (is.na(st_crs(fires))) stop("MTBS source has no coordinate reference system.")

# Check expected national coverage before promoting the source as canonical.
attrs <- as.data.table(st_drop_geometry(fires))
checks <- data.table(
  check = c(
    "feature_count", "unique_fire_id", "unique_map_id", "missing_fire_id",
    "missing_map_id", "minimum_year", "maximum_year", "mtbs_program_rows",
    "invalid_or_empty_geometry"
  ),
  value = as.character(c(
    nrow(fires), uniqueN(attrs$FIRE_ID), uniqueN(attrs$MAP_ID),
    sum(is.na(attrs$FIRE_ID) | attrs$FIRE_ID == ""),
    sum(is.na(attrs$MAP_ID)), min(attrs$YEAR, na.rm = TRUE),
    max(attrs$YEAR, na.rm = TRUE), sum(attrs$MAP_PROG == "MTBS", na.rm = TRUE),
    sum(is.na(st_geometry(fires)) | st_is_empty(fires))
  )),
  expected = c(
    NA, nrow(fires), nrow(fires), 0, 0, NA, NA, nrow(fires), 0
  )
)
checks[, passed := is.na(expected) | as.numeric(value) == expected]
dir_create(dirname(qa_path))
if (any(checks$passed == FALSE)) {
  fwrite(checks, qa_path)
  stop("MTBS source validation failed; see ", qa_path)
}

dir_create(dirname(output_path))
# Write to a temporary GeoPackage so a failed conversion cannot replace the source.
temporary_path <- tempfile(
  pattern = "mtbs_fire_perimeters_tmp_",
  tmpdir = dirname(output_path),
  fileext = ".gpkg"
)
on.exit(if (file.exists(temporary_path)) file_delete(temporary_path), add = TRUE)
st_write(
  fires,
  temporary_path,
  layer = "mtbs_fire_perimeters",
  driver = "GPKG",
  quiet = TRUE
)
# Replace the canonical vector only after every validation passes.
if (file.exists(output_path)) file_delete(output_path)
file_move(temporary_path, output_path)
if (!file.exists(output_path)) {
  stop("Could not move validated MTBS GeoPackage into place: ", output_path)
}
fwrite(checks, qa_path)

cat(glue("Features: {format(nrow(fires), big.mark = ',')}\n"))
cat(glue("Years: {min(attrs$YEAR)}-{max(attrs$YEAR)}\n"))
cat(glue("Canonical GeoPackage: {output_path}\n"))
cat(glue("Source validation: {qa_path}\n"))
