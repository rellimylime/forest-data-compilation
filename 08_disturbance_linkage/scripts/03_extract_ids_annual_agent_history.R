#!/usr/bin/env Rscript

# Build two neutral, year-partitioned IDS products:
#   1. exact-agent detections: stable_plot_id x survey_year x DCA_CODE
#   2. surveyed-area coverage: stable_plot_id x survey_year
#
# Existing year files are skipped unless --overwrite is supplied. Use
# --year=YYYY for a single-year run or validation.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/ids_overlap.R"))
source(here("scripts/utils/parquet_atomic.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
area_crs <- link_cfg$area_crs
output_root <- here(link_cfg$output_dir)
qa_dir <- here(link_cfg$qa_dir)
footprints_path <- file.path(
  output_root,
  link_cfg$files$plot_footprints
)
ids_path <- here(link_cfg$inputs$ids_layers)
agent_dir <- file.path(
  output_root,
  link_cfg$files$ids_annual_agent_evidence
)
coverage_dir <- file.path(
  output_root,
  link_cfg$files$ids_annual_survey_coverage
)

args <- commandArgs(trailingOnly = TRUE)
year_arg <- args[grepl("^--year=", args)]
selected_year <- if (length(year_arg) == 1L) {
  as.integer(sub("^--year=", "", year_arg[[1]]))
} else {
  NA_integer_
}
overwrite <- "--overwrite" %in% args
if (
  length(year_arg) > 1L ||
  (length(year_arg) == 1L && is.na(selected_year))
) {
  stop("Use at most one integer --year=YYYY argument.")
}

for (path in c(footprints_path, ids_path)) {
  if (!file.exists(path)) {
    stop("Required IDS evidence input is missing: ", path)
  }
}

dir_create(agent_dir)
dir_create(coverage_dir)
dir_create(qa_dir)

footprints <- st_read(footprints_path, quiet = TRUE)
if (anyDuplicated(footprints$stable_plot_id)) {
  stop("Plot footprints must be unique by stable_plot_id.")
}
footprints <- st_transform(footprints, area_crs)
footprint_areas <- data.table(
  stable_plot_id = as.character(footprints$stable_plot_id),
  footprint_area_m2 = as.numeric(st_area(footprints))
)

damage_years <- as.integer(st_read(
  ids_path,
  query = paste(
    "SELECT DISTINCT SURVEY_YEAR FROM damage_areas",
    "WHERE SURVEY_YEAR IS NOT NULL ORDER BY SURVEY_YEAR"
  ),
  quiet = TRUE
)$SURVEY_YEAR)
coverage_years <- as.integer(st_read(
  ids_path,
  query = paste(
    "SELECT DISTINCT SURVEY_YEAR FROM surveyed_areas",
    "WHERE SURVEY_YEAR IS NOT NULL ORDER BY SURVEY_YEAR"
  ),
  quiet = TRUE
)$SURVEY_YEAR)

survey_years <- sort(unique(c(damage_years, coverage_years)))
if (!is.na(selected_year)) {
  if (!selected_year %in% survey_years) {
    stop("Requested IDS survey year is unavailable: ", selected_year)
  }
  survey_years <- selected_year
}

source_info <- file.info(ids_path)
source_modified_utc <- format(
  source_info$mtime,
  "%Y-%m-%dT%H:%M:%SZ",
  tz = "UTC"
)
ids_source_snapshot <- paste0(
  basename(ids_path),
  "|bytes=", source_info$size,
  "|modified_utc=", source_modified_utc
)

dca_lookup <- fread(here("01_ids/lookups/dca_code_lookup.csv"))
setnames(
  dca_lookup,
  c("DCA_CODE", "DCA_COMMON_NAME"),
  c("dca_code", "agent_label"),
  skip_absent = TRUE
)
dca_lookup[, dca_code := as.integer(dca_code)]

collapse_values <- function(values) {
  values <- sort(unique(as.character(values[!is.na(values)])))
  if (length(values) == 0L) NA_character_ else paste(values, collapse = ";")
}

summarize_coverage_hits <- function(intersections) {
  if (nrow(intersections) == 0L) {
    return(data.table())
  }
  # Union overlapping survey pieces so their area is not counted twice.
  geom <- st_geometry(intersections)
  attrs <- as.data.table(st_drop_geometry(intersections))
  attrs[, geometry_row := seq_len(.N)]
  attrs[, {
    feature_ids <- sort(unique(
      as.character(SURVEY_FEATURE_ID[!is.na(SURVEY_FEATURE_ID)])
    ))
    list(
      n_source_survey_features = length(feature_ids),
      source_survey_feature_ids = if (length(feature_ids) == 0L) {
        NA_character_
      } else {
        paste(feature_ids, collapse = ";")
      },
      surveyed_overlap_area_m2 = as.numeric(st_area(st_union(geom[geometry_row])))
    )
  }, by = stable_plot_id]
}

summarize_survey_coverage <- function(footprints, surveyed, footprint_areas) {
  # Quickly identify plot buffers that touch the year's survey coverage.
  intersecting_features <- st_intersects(footprints, surveyed)

  # Drop untouched buffers before doing expensive geometry calculations.
  hit_rows <- which(lengths(intersecting_features) > 0L)
  if (length(hit_rows) == 0L) return(data.table())

  # Find buffers completely covered by one survey polygon.
  covering_features <- st_covered_by(footprints[hit_rows, ], surveyed)
  full_rows <- hit_rows[lengths(covering_features) > 0L]
  boundary_rows <- setdiff(hit_rows, full_rows)
  results <- list()

  # Fully covered buffers use their known area without clipping polygons.
  if (length(full_rows) > 0L) {
    full_feature_ids <- lapply(
      intersecting_features[full_rows],
      function(index) {
        values <- as.character(surveyed$SURVEY_FEATURE_ID[index])
        sort(unique(values[!is.na(values)]))
      }
    )
    full_plot_ids <- as.character(footprints$stable_plot_id[full_rows])
    results[["full"]] <- data.table(
      stable_plot_id = full_plot_ids,
      n_source_survey_features = as.integer(lengths(full_feature_ids)),
      source_survey_feature_ids = vapply(
        full_feature_ids,
        function(values) {
          if (length(values) == 0L) NA_character_ else paste(values, collapse = ";")
        },
        character(1)
      ),
      surveyed_overlap_area_m2 = footprint_areas$footprint_area_m2[
        match(full_plot_ids, footprint_areas$stable_plot_id)
      ]
    )
  }

  # Only boundary-crossing buffers require an exact clipped-area calculation.
  if (length(boundary_rows) > 0L) {
    boundary_clip <- suppressWarnings(st_intersection(
      footprints[boundary_rows, "stable_plot_id"],
      surveyed["SURVEY_FEATURE_ID"]
    ))
    results[["boundary"]] <- summarize_coverage_hits(boundary_clip)
    rm(boundary_clip)
  }

  rbindlist(results, use.names = TRUE)
}

qa_rows <- vector("list", length(survey_years))

# Process one survey year at a time so interrupted runs can resume cleanly.
for (i in seq_along(survey_years)) {
  year <- survey_years[[i]]
  agent_expected <- year %in% damage_years
  agent_path <- file.path(agent_dir, sprintf("survey_year=%d.parquet", year))
  coverage_path <- file.path(
    coverage_dir,
    sprintf("survey_year=%d.parquet", year)
  )
  coverage_exists <- file.exists(coverage_path)
  agent_exists <- !agent_expected || file.exists(agent_path)
  rebuild_coverage <- overwrite || !coverage_exists
  # Rebuild agents when their embedded coverage values have changed.
  rebuild_agent <- agent_expected &&
    (overwrite || !agent_exists || rebuild_coverage)

  # Skip complete years while still reading their small QA counts.
  if (!rebuild_coverage && !rebuild_agent) {
    cat(glue(
      "[{i}/{length(survey_years)}] IDS {year}: existing partitions retained\n"
    ))
    coverage_existing <- as.data.table(read_parquet(
      coverage_path,
      col_select = c("coverage_relationship")
    ))
    n_agents <- if (agent_expected) {
      nrow(read_parquet(
        agent_path,
        col_select = c("stable_plot_id")
      ))
    } else {
      0L
    }
    qa_rows[[i]] <- data.table(
      survey_year = year,
      n_agent_rows = as.integer(n_agents),
      n_full_coverage = coverage_existing[
        coverage_relationship == "full_surveyed_area_intersection",
        .N
      ],
      n_partial_coverage = coverage_existing[
        coverage_relationship == "partial_surveyed_area_intersection",
        .N
      ],
      n_no_coverage_intersection = coverage_existing[
        coverage_relationship == "no_surveyed_area_intersection",
        .N
      ],
      n_coverage_unknown = coverage_existing[
        coverage_relationship == "coverage_unknown",
        .N
      ]
    )
    next
  }

  cat(glue("[{i}/{length(survey_years)}] IDS {year}: processing\n"))
  surveyed <- NULL
  coverage_hits <- NULL

  # Reuse valid coverage when only the agent calculation was interrupted.
  if (!rebuild_coverage) {
    coverage <- as.data.table(read_parquet(coverage_path))
    cat("  coverage: existing partition retained\n")
  } else {
    coverage_started <- Sys.time()
    surveyed_sql <- sprintf(
      paste0(
        "SELECT fid, geom, SURVEY_YEAR, REGION_ID, SOURCE_FILE, ",
        "SURVEY_FEATURE_ID FROM surveyed_areas WHERE SURVEY_YEAR = %d"
      ),
      year
    )
    # Query only the current year instead of loading the national layer.
    surveyed <- st_read(ids_path, query = surveyed_sql, quiet = TRUE)
    coverage_source_available <- nrow(surveyed) > 0L
    coverage_hits <- data.table()
    if (coverage_source_available) {
      surveyed <- st_transform(surveyed, area_crs)
      coverage_hits <- summarize_survey_coverage(
        footprints,
        surveyed,
        footprint_areas
      )
    }

    # Keep one coverage row for every eligible plot, including zero intersections.
    coverage <- merge(
      copy(footprint_areas),
      coverage_hits,
      by = "stable_plot_id",
      all.x = TRUE,
      sort = FALSE
    )
    coverage[, `:=`(
      survey_year = as.integer(year),
      n_source_survey_features = fifelse(
        is.na(n_source_survey_features),
        0L,
        as.integer(n_source_survey_features)
      ),
      surveyed_overlap_area_m2 = fifelse(
        is.na(surveyed_overlap_area_m2),
        0,
        surveyed_overlap_area_m2
      ),
      coverage_source_available = coverage_source_available
    )]
    coverage[, surveyed_overlap_fraction :=
      pmin(1, surveyed_overlap_area_m2 / footprint_area_m2)]
    # Keep full, partial, absent, and unknown survey coverage distinct.
    coverage[, coverage_relationship := fcase(
      coverage_source_available == FALSE, "coverage_unknown",
      surveyed_overlap_fraction >= 1 - 1e-6,
        "full_surveyed_area_intersection",
      surveyed_overlap_fraction > 1e-6,
        "partial_surveyed_area_intersection",
      default = "no_surveyed_area_intersection"
    )]
    coverage[, `:=`(
      linkage_method = "IDS_surveyed_area_overlap_with_800m_public_FIA_buffer",
      source_layer = "surveyed_areas",
      source_snapshot = ids_source_snapshot
    )]
    setcolorder(coverage, c(
      "stable_plot_id", "survey_year", "coverage_relationship",
      "coverage_source_available", "n_source_survey_features",
      "source_survey_feature_ids", "surveyed_overlap_area_m2",
      "surveyed_overlap_fraction", "footprint_area_m2",
      "linkage_method", "source_layer", "source_snapshot"
    ))
    setorder(coverage, stable_plot_id)
    if (anyDuplicated(coverage[, .(stable_plot_id, survey_year)])) {
      stop("IDS coverage output is not unique for ", year)
    }
    write_parquet_atomic(coverage, coverage_path)
    cat(glue(
      "  coverage: {round(difftime(Sys.time(), coverage_started, units = 'secs'), 1)} seconds\n"
    ))
  }

  n_agent_rows <- 0L
  # Damage polygons are processed separately from surveyed-area coverage.
  if (rebuild_agent) {
    agent_started <- Sys.time()
    damage_sql <- sprintf(
      paste0(
        "SELECT fid, geom, OBSERVATION_ID, DAMAGE_AREA_ID, DCA_CODE, ",
        "SURVEY_YEAR, REGION_ID, HOST_CODE, DAMAGE_TYPE_CODE, ACRES, ",
        "AREA_TYPE, PERCENT_AFFECTED_CODE, PERCENT_MID, LEGACY_TPA, ",
        "LEGACY_NO_TREES, LEGACY_SEVERITY_CODE ",
        "FROM damage_areas WHERE SURVEY_YEAR = %d"
      ),
      year
    )
    damage <- st_read(ids_path, query = damage_sql, quiet = TRUE)
    damage <- st_transform(damage, area_crs)
    names(damage)[names(damage) == "OBSERVATION_ID"] <-
      "source_observation_id"
    names(damage)[names(damage) == "DAMAGE_AREA_ID"] <- "source_feature_id"
    names(damage)[names(damage) == "DCA_CODE"] <- "dca_code"
    names(damage)[names(damage) == "SURVEY_YEAR"] <- "survey_year"
    names(damage)[names(damage) == "ACRES"] <- "source_polygon_acres"
    names(damage)[names(damage) == "HOST_CODE"] <- "host_code"
    names(damage)[names(damage) == "DAMAGE_TYPE_CODE"] <- "damage_type_code"
    names(damage)[names(damage) == "PERCENT_AFFECTED_CODE"] <-
      "percent_affected_code"
    names(damage)[names(damage) == "PERCENT_MID"] <- "percent_mid"
    names(damage)[names(damage) == "LEGACY_TPA"] <- "legacy_tpa"
    names(damage)[names(damage) == "LEGACY_NO_TREES"] <- "legacy_no_trees"
    names(damage)[names(damage) == "LEGACY_SEVERITY_CODE"] <-
      "legacy_severity_code"

    # Retain every damage polygon that touches an eligible 800 m plot buffer.
    clipped <- suppressWarnings(st_intersection(
      footprints["stable_plot_id"],
      damage[c(
        "survey_year", "dca_code", "source_feature_id",
        "source_observation_id", "host_code", "damage_type_code",
        "source_polygon_acres", "percent_affected_code", "percent_mid",
        "legacy_tpa", "legacy_no_trees", "legacy_severity_code"
      )]
    ))

    # Collapse polygon pieces to one plot, year, and exact DCA code.
    agents <- summarize_ids_intersections(clipped, footprint_areas)
    if (nrow(agents) > 0L) {
      attrs <- as.data.table(st_drop_geometry(clipped))
      agent_attrs <- attrs[, .(
        host_codes = collapse_values(host_code),
        damage_type_codes = collapse_values(damage_type_code),
        percent_affected_codes = collapse_values(percent_affected_code),
        ids_percent_mid_min = if (all(is.na(percent_mid))) {
          NA_real_
        } else {
          as.numeric(min(percent_mid, na.rm = TRUE))
        },
        ids_percent_mid_max = if (all(is.na(percent_mid))) {
          NA_real_
        } else {
          as.numeric(max(percent_mid, na.rm = TRUE))
        },
        ids_legacy_tpa_min = if (all(is.na(legacy_tpa))) {
          NA_real_
        } else {
          as.numeric(min(legacy_tpa, na.rm = TRUE))
        },
        ids_legacy_tpa_max = if (all(is.na(legacy_tpa))) {
          NA_real_
        } else {
          as.numeric(max(legacy_tpa, na.rm = TRUE))
        },
        ids_legacy_no_trees_sum = if (all(is.na(legacy_no_trees))) {
          NA_real_
        } else {
          as.numeric(sum(legacy_no_trees, na.rm = TRUE))
        },
        ids_legacy_severity_codes = collapse_values(legacy_severity_code)
      ), by = .(stable_plot_id, survey_year, dca_code)]
      # Add exact agent labels without grouping agents into broader classes.
      agents <- merge(
        agents,
        agent_attrs,
        by = c("stable_plot_id", "survey_year", "dca_code"),
        all.x = TRUE,
        sort = FALSE
      )
      # Attach annual coverage so detection and survey effort can be interpreted together.
      agents <- merge(
        agents,
        dca_lookup[, .(dca_code, agent_label)],
        by = "dca_code",
        all.x = TRUE,
        sort = FALSE
      )
      agents <- merge(
        agents,
        coverage[, .(
          stable_plot_id, survey_year, coverage_relationship,
          surveyed_overlap_area_m2, surveyed_overlap_fraction
        )],
        by = c("stable_plot_id", "survey_year"),
        all.x = TRUE,
        sort = FALSE
      )
      agents[, `:=`(
        exact_agent_grouping_applied = FALSE,
        linkage_method =
          "IDS_damage_area_overlap_with_800m_public_FIA_buffer",
        source_layer = "damage_areas",
        source_snapshot = ids_source_snapshot
      )]
      setorder(agents, stable_plot_id, dca_code)
      if (anyDuplicated(agents[, .(
        stable_plot_id, survey_year, dca_code
      )])) {
        stop("IDS exact-agent output is not unique for ", year)
      }
    }
    # Publish the year only after its full agent table passes key checks.
    write_parquet_atomic(agents, agent_path)
    n_agent_rows <- nrow(agents)
    cat(glue(
      "  agents: {round(difftime(Sys.time(), agent_started, units = 'secs'), 1)} seconds\n"
    ))
    rm(damage, clipped, agents)
  } else if (agent_expected) {
    n_agent_rows <- nrow(read_parquet(
      agent_path,
      col_select = c("stable_plot_id")
    ))
    cat("  agents: existing partition retained\n")
  }

  qa_rows[[i]] <- coverage[, .(
    survey_year = year,
    n_agent_rows = n_agent_rows,
    n_full_coverage = sum(
      coverage_relationship == "full_surveyed_area_intersection"
    ),
    n_partial_coverage = sum(
      coverage_relationship == "partial_surveyed_area_intersection"
    ),
    n_no_coverage_intersection = sum(
      coverage_relationship == "no_surveyed_area_intersection"
    ),
    n_coverage_unknown = sum(coverage_relationship == "coverage_unknown")
  )]

  # Release annual geometries before starting the next year.
  rm(surveyed, coverage, coverage_hits)
  gc(verbose = FALSE)
}

qa <- rbindlist(qa_rows, fill = TRUE)
setorder(qa, survey_year)
fwrite(qa, file.path(qa_dir, "ids_annual_evidence_by_year.csv"))

cat(glue(
  "IDS exact-agent evidence: {agent_dir}\n",
  "IDS survey coverage: {coverage_dir}\n",
  "Year QA: {file.path(qa_dir, 'ids_annual_evidence_by_year.csv')}\n"
))
