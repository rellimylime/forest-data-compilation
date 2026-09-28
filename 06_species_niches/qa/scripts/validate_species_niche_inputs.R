#!/usr/bin/env Rscript

# Verify that the tracked species climate niche inputs are byte-for-byte the
# products documented by their provenance manifest. This check is read-only.

suppressPackageStartupMessages({
  library(arrow)
  library(digest)
  library(here)
})

repo_root <- normalizePath(here(), winslash = "/", mustWork = TRUE)
setwd(repo_root)

manifest_path <- file.path(
  "06_species_niches", "data", "processed", "species_niche_manifest.csv"
)
if (!file.exists(manifest_path)) {
  stop("Missing species niche provenance manifest: ", manifest_path)
}

manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
required_fields <- c(
  "global_niche_file", "global_niche_sha256",
  "study_area_niche_file", "study_area_niche_sha256",
  "global_species", "study_area_species",
  "r_version", "sf_version", "geos_version", "gdal_version", "proj_version"
)
if (nrow(manifest) != 1L || !all(required_fields %in% names(manifest))) {
  stop(
    "species_niche_manifest.csv must contain exactly one row and all ",
    "required provenance fields."
  )
}

validate_niche <- function(file_field, hash_field, count_field) {
  path <- manifest[[file_field]][[1L]]
  if (is.na(path) || !nzchar(path) || !file.exists(path)) {
    stop("Missing tracked species niche file: ", path)
  }

  expected_hash <- tolower(manifest[[hash_field]][[1L]])
  observed_hash <- digest(path, algo = "sha256", file = TRUE)
  if (!identical(observed_hash, expected_hash)) {
    stop(
      "SHA-256 mismatch for ", path, ": expected ", expected_hash,
      ", observed ", observed_hash
    )
  }

  observed_species <- nrow(read_parquet(path, col_select = "species_key"))
  expected_species <- as.integer(manifest[[count_field]][[1L]])
  if (is.na(expected_species) || observed_species != expected_species) {
    stop(
      "Species count mismatch for ", path, ": expected ", expected_species,
      ", observed ", observed_species
    )
  }

  observed_species
}

global_species <- validate_niche(
  "global_niche_file", "global_niche_sha256", "global_species"
)
study_area_species <- validate_niche(
  "study_area_niche_file", "study_area_niche_sha256", "study_area_species"
)

cat(
  "Species niche inputs validated: global=", global_species,
  ", study_area=", study_area_species, " species.\n",
  sep = ""
)
cat(
  "Recorded geometry stack: R ", manifest$r_version[[1L]],
  "; sf ", manifest$sf_version[[1L]],
  "; GEOS ", manifest$geos_version[[1L]],
  "; GDAL ", manifest$gdal_version[[1L]],
  "; PROJ ", manifest$proj_version[[1L]], ".\n",
  sep = ""
)
