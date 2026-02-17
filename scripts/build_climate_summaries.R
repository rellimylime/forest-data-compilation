# ==============================================================================
# scripts/build_climate_summaries.R
#
# Compute observation-level climate summaries (area-weighted means).
# Uses Arrow lazy evaluation + data.table for fast joins/aggregations.
# Includes pre-flight benchmarking and parallel feasibility testing.
#
# Usage:
#   Rscript scripts/build_climate_summaries.R <dataset> [n_workers]
#   Rscript scripts/build_climate_summaries.R terraclimate      # auto-detect workers
#   Rscript scripts/build_climate_summaries.R terraclimate 4    # force 4 workers
#   Rscript scripts/build_climate_summaries.R terraclimate 1    # force sequential
#
# Output: processed/climate/<dataset>/damage_areas_summaries/ (per-variable parquet files)
# ==============================================================================

library(here)
library(yaml)
library(dplyr)
library(arrow)
library(data.table)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/time_utils.R"))

config <- load_config()

# ==============================================================================
# Helper functions
# ==============================================================================

get_available_mem_gb <- function() {
  tryCatch({
    meminfo <- readLines("/proc/meminfo")
    line <- grep("^MemAvailable:", meminfo, value = TRUE)
    as.numeric(gsub("[^0-9]", "", line)) / 1024 / 1024
  }, error = function(e) NA_real_)
}

get_rss_mb <- function() {
  tryCatch({
    status <- readLines("/proc/self/status")
    line <- grep("^VmRSS:", status, value = TRUE)
    as.numeric(gsub("[^0-9]", "", line)) / 1024
  }, error = function(e) NA_real_)
}

format_time <- function(secs) {
  if (secs < 60) return(sprintf("%.1fs", secs))
  if (secs < 3600) return(sprintf("%.1fm", secs / 60))
  sprintf("%.1fh", secs / 3600)
}

make_progress_bar <- function(pct, width = 20) {
  filled <- floor(pct / (100 / width))
  sprintf("[%s%s]",
          paste(rep("\u2588", filled), collapse = ""),
          paste(rep("\u2591", width - filled), collapse = ""))
}

# Process a single variable x year chunk. Returns list with result, time, rows, status, timings.
# source_file: path to the wide-format yearly parquet file (e.g., terraclimate_2020.parquet)
process_one_chunk <- function(var, year, source_file, pixel_map_dt,
                              time_cols, group_id, progress_log = NULL) {
  chunk_start <- Sys.time()
  timings <- list()

  # Read yearly source file and extract the single variable column
  t0 <- Sys.time()
  src_cols <- c("pixel_id", "month", var)
  if ("day" %in% time_cols) src_cols <- c(src_cols, "day")
  wide <- as.data.table(read_parquet(source_file,
                                     col_select = all_of(src_cols)))
  setnames(wide, var, "value")
  wide[, `:=`(variable = var,
              calendar_year = as.integer(year),
              calendar_month = as.integer(month))]
  wide[, month := NULL]
  # Add water year columns
  wide[, `:=`(
    water_year = fifelse(calendar_month >= 10L,
                         calendar_year + 1L, calendar_year),
    water_year_month = fifelse(calendar_month >= 10L,
                               calendar_month - 9L,
                               calendar_month + 3L)
  )]
  timings$arrow <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  timings$arrow_rows <- nrow(wide)

  if (nrow(wide) == 0) {
    return(list(result = NULL, time = 0, rows = 0, status = "empty", timings = timings))
  }

  # data.table join
  t0 <- Sys.time()
  chunk_dt <- wide
  rm(wide)
  setkey(chunk_dt, pixel_id)
  joined_dt <- pixel_map_dt[chunk_dt, nomatch = 0, allow.cartesian = TRUE]
  rm(chunk_dt)
  timings$join <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  timings$join_rows <- nrow(joined_dt)

  if (nrow(joined_dt) == 0) {
    return(list(result = NULL, time = 0, rows = 0, status = "no_match", timings = timings))
  }

  # Aggregation
  t0 <- Sys.time()
  group_cols <- c(group_id, time_cols, "variable")
  if ("OBSERVATION_ID" %in% names(joined_dt) && group_id != "OBSERVATION_ID") {
    group_cols <- c("OBSERVATION_ID", group_cols)
  }

  summary_dt <- joined_dt[,
    .(weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                      sum(coverage_fraction[!is.na(value)]),
      value_min = min(value, na.rm = TRUE),
      value_max = max(value, na.rm = TRUE),
      n_pixels = .N,
      n_pixels_with_data = sum(!is.na(value)),
      sum_coverage_fraction = sum(coverage_fraction)),
    by = group_cols
  ]
  rm(joined_dt)
  timings$agg <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  total_time <- as.numeric(difftime(Sys.time(), chunk_start, units = "secs"))

  # Write to progress log (atomic append, safe for parallel)
  if (!is.null(progress_log)) {
    log_msg <- sprintf("[%s] %-6s | %d | %6.1fs | %s rows\n",
                       format(Sys.time(), "%H:%M:%S"), var, year,
                       total_time, format(nrow(summary_dt), big.mark = ","))
    cat(log_msg, file = progress_log, append = TRUE)
  }

  gc(verbose = FALSE)

  list(
    result = summary_dt,
    time = total_time,
    rows = nrow(summary_dt),
    status = "ok",
    timings = timings
  )
}

# ==============================================================================
# Configuration
# ==============================================================================

args <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1) args[1] else "terraclimate"
n_workers_override <- if (length(args) >= 2) as.integer(args[2]) else NULL

dataset_configs <- list(
  terraclimate = list(
    pixel_map = here("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
    source_dir = here("02_terraclimate/data/processed/pixel_values"),
    source_prefix = "terraclimate",
    variables = names(config$raw$terraclimate$variables),
    time_cols = c("year", "month")
  ),
  prism = list(
    pixel_map = here("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
    source_dir = here("03_prism/data/processed/pixel_values"),
    source_prefix = "prism",
    variables = names(config$raw$prism$variables),
    time_cols = c("year", "month")
  ),
  worldclim = list(
    pixel_map = here("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
    source_dir = here("04_worldclim/data/processed/pixel_values"),
    source_prefix = "worldclim",
    variables = names(config$raw$worldclim$variables),
    time_cols = c("year", "month")
  ),
  era5 = list(
    pixel_map = here("05_era5/data/processed/pixel_maps/damage_areas_pixel_map.parquet"),
    source_dir = here("05_era5/data/processed/pixel_values"),
    source_prefix = "era5",
    variables = names(config$raw$era5$variables),
    time_cols = c("year", "month", "day")
  )
)

if (!dataset %in% names(dataset_configs)) {
  stop("Unknown dataset: ", dataset,
       ". Choose from: ", paste(names(dataset_configs), collapse = ", "))
}

ds_config <- dataset_configs[[dataset]]
pixel_map_file <- ds_config$pixel_map
source_dir <- ds_config$source_dir
source_prefix <- ds_config$source_prefix
output_dir <- here("processed/climate", dataset, "damage_areas_summaries")
progress_log <- here("processed/climate", dataset, ".progress_log")

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Desired output summary columns (used to detect what needs reprocessing)
summary_cols <- c("weighted_mean", "value_min", "value_max",
                  "n_pixels", "n_pixels_with_data", "sum_coverage_fraction")

cat("\n")
cat("================================================================================\n")
cat(sprintf(" Climate Summaries: %s\n", toupper(dataset)))
cat("================================================================================\n\n")

script_start_time <- Sys.time()

# ==============================================================================
# Step 1/7: Load pixel map
# ==============================================================================

cat("Step 1/7: Loading pixel map\n")
cat(strrep("-", 60), "\n")

if (!file.exists(pixel_map_file)) stop("Pixel map not found: ", pixel_map_file)

step_start <- Sys.time()
pixel_map <- read_parquet(pixel_map_file)

has_obs_id <- "OBSERVATION_ID" %in% names(pixel_map)
has_da_id <- "DAMAGE_AREA_ID" %in% names(pixel_map)
if (has_da_id) {
  group_id <- "DAMAGE_AREA_ID"
} else if (has_obs_id) {
  group_id <- "OBSERVATION_ID"
} else {
  stop("Pixel map must contain DAMAGE_AREA_ID or OBSERVATION_ID")
}

cat(sprintf("  Rows: %s | Grouping by: %s\n",
            format(nrow(pixel_map), big.mark = ","), group_id))
cat(sprintf("  Unique %s: %s | Unique pixels: %s\n",
            group_id,
            format(length(unique(pixel_map[[group_id]])), big.mark = ","),
            format(length(unique(pixel_map$pixel_id)), big.mark = ",")))

pm_cols <- c(group_id, "pixel_id", "coverage_fraction")
if (has_obs_id && group_id == "DAMAGE_AREA_ID") pm_cols <- c("OBSERVATION_ID", pm_cols)
pixel_map <- pixel_map[, intersect(pm_cols, names(pixel_map))]

pixel_map_dt <- as.data.table(pixel_map)
setkey(pixel_map_dt, pixel_id)
rm(pixel_map)

pm_size_mb <- as.numeric(object.size(pixel_map_dt)) / 1024^2
cat(sprintf("  Pixel map in memory: %.0f MB\n", pm_size_mb))
cat(sprintf("  Time: %s\n\n", format_time(as.numeric(difftime(Sys.time(), step_start, units = "secs")))))

# ==============================================================================
# Step 2/7: Scan source files
# ==============================================================================

cat("Step 2/7: Scanning source files\n")
cat(strrep("-", 60), "\n")

if (!dir.exists(source_dir)) stop("Source directory not found: ", source_dir)

step_start <- Sys.time()

# Find yearly source files and extract years from filenames
source_files <- list.files(source_dir,
                           pattern = paste0("^", source_prefix, "_\\d{4}\\.parquet$"),
                           full.names = TRUE)
if (length(source_files) == 0) stop("No source files found in: ", source_dir)

years <- sort(as.integer(regmatches(basename(source_files),
                                     regexpr("\\d{4}", basename(source_files)))))

# Build lookup: year -> source file path
source_file_map <- setNames(source_files[order(basename(source_files))],
                             as.character(years))

# Get variables from config (verified against first source file)
variables <- ds_config$variables
first_file_cols <- names(read_parquet(source_files[1], as_data_frame = FALSE)$schema)
variables <- intersect(variables, first_file_cols)

# Time columns for output (we generate these in process_one_chunk)
time_cols <- c("calendar_year", "calendar_month", "water_year", "water_year_month")
if ("day" %in% ds_config$time_cols) time_cols <- c(time_cols[1:2], "day", time_cols[3:4])

n_chunks <- length(variables) * length(years)

cat(sprintf("  Source: %s/ (%d yearly files)\n", basename(source_dir), length(source_files)))
cat(sprintf("  Variables (%d): %s\n", length(variables), paste(variables, collapse = ", ")))
cat(sprintf("  Year range: %d-%d (%d years)\n", min(years), max(years), length(years)))
cat(sprintf("  Total chunks: %d variables x %d years = %d\n",
            length(variables), length(years), n_chunks))
cat(sprintf("  Time: %s\n\n", format_time(as.numeric(difftime(Sys.time(), step_start, units = "secs")))))

# ==============================================================================
# Step 3/7: Pre-flight benchmark
# ==============================================================================

cat("Step 3/7: Pre-flight benchmark\n")
cat(strrep("-", 60), "\n")
cat(sprintf("  Running 1 test chunk: %s / %d ...\n", variables[1], years[1]))

mem_before <- get_rss_mb()

benchmark <- process_one_chunk(
  variables[1], years[1],
  source_file_map[[as.character(years[1])]], pixel_map_dt,
  time_cols, group_id
)

mem_after <- get_rss_mb()
mem_delta <- if (!is.na(mem_before) && !is.na(mem_after)) mem_after - mem_before else NA_real_

cat(sprintf("\n  Benchmark results:\n"))
cat(sprintf("    Arrow filter + collect : %6.1fs  (%s rows loaded)\n",
            benchmark$timings$arrow,
            format(benchmark$timings$arrow_rows, big.mark = ",")))
cat(sprintf("    data.table join        : %6.1fs  (%s joined rows)\n",
            benchmark$timings$join,
            format(benchmark$timings$join_rows, big.mark = ",")))
cat(sprintf("    Aggregation            : %6.1fs  (%s result rows)\n",
            benchmark$timings$agg,
            format(benchmark$rows, big.mark = ",")))
cat(sprintf("    ────────────────────────────────\n"))
cat(sprintf("    Total chunk time       : %6.1fs  (%s)\n",
            benchmark$time, format_time(benchmark$time)))
if (!is.na(mem_delta)) {
  cat(sprintf("    Memory delta           : %+.0f MB  (RSS: %.0f MB)\n", mem_delta, mem_after))
}
cat("\n")

# ==============================================================================
# Step 4/7: Feasibility analysis
# ==============================================================================

cat("Step 4/7: Feasibility analysis\n")
cat(strrep("-", 60), "\n")

available_mem_gb <- get_available_mem_gb()
n_cores <- parallel::detectCores()

# Estimate per-worker memory: pixel_map + ~2x working memory from benchmark
est_worker_mem_gb <- (pm_size_mb + max(mem_delta, 2000)) / 1024

sequential_time_hrs <- (benchmark$time * n_chunks) / 3600

cat(sprintf("  System resources:\n"))
cat(sprintf("    CPU cores: %d\n", n_cores))
if (!is.na(available_mem_gb)) {
  cat(sprintf("    Available memory: %.0f GB\n", available_mem_gb))
}
cat(sprintf("    Est. memory per worker: ~%.1f GB\n", est_worker_mem_gb))
cat(sprintf("\n  Sequential estimate: %d chunks x %.0fs = %.1f hours\n",
            n_chunks, benchmark$time, sequential_time_hrs))

# Determine worker count
if (!is.null(n_workers_override)) {
  n_workers <- n_workers_override
  cat(sprintf("\n  Worker count: %d (user override)\n", n_workers))
} else if (is.na(available_mem_gb)) {
  n_workers <- 1
  cat("\n  Cannot read system memory - defaulting to sequential.\n")
} else {
  max_by_mem <- max(1, floor(available_mem_gb * 0.7 / est_worker_mem_gb))
  max_by_cpu <- max(1, n_cores - 2)
  n_workers <- min(max_by_mem, max_by_cpu, 8)
  cat(sprintf("    Max workers by memory (70%% budget): %d\n", max_by_mem))
  cat(sprintf("    Max workers by CPU: %d\n", max_by_cpu))
  cat(sprintf("    Selected: %d workers\n", n_workers))
}

# Parallel verification test
use_parallel <- FALSE

if (n_workers > 1) {
  cat(sprintf("\n  Parallel verification test (2 chunks, 2 workers)...\n"))

  test_ok <- tryCatch({
    test_years <- years[1:min(2, length(years))]
    par_start <- Sys.time()

    par_results <- parallel::mclapply(test_years, function(y) {
      process_one_chunk(variables[1], y,
                        source_file_map[[as.character(y)]],
                        pixel_map_dt, time_cols, group_id)
    }, mc.cores = min(2, n_workers))

    par_wall_time <- as.numeric(difftime(Sys.time(), par_start, units = "secs"))

    # Check for mclapply errors (returns try-error objects on failure)
    has_errors <- any(sapply(par_results, inherits, "try-error"))
    all_ok <- !has_errors && all(sapply(par_results, function(r) r$status == "ok"))
    worker_times <- sapply(par_results, function(r) r$time)

    cat(sprintf("    Worker 1: %s/%d -> %.1fs, %s rows\n",
                variables[1], test_years[1],
                par_results[[1]]$time,
                format(par_results[[1]]$rows, big.mark = ",")))
    if (length(test_years) > 1) {
      cat(sprintf("    Worker 2: %s/%d -> %.1fs, %s rows\n",
                  variables[1], test_years[2],
                  par_results[[2]]$time,
                  format(par_results[[2]]$rows, big.mark = ",")))
    }
    cat(sprintf("    Wall clock: %.1fs (vs %.1fs sequential benchmark)\n",
                par_wall_time, benchmark$time))

    # Parallel is working if wall time < sum of worker times (i.e. they overlapped)
    sum_worker_times <- sum(worker_times)
    speedup <- sum_worker_times / par_wall_time

    if (all_ok && speedup > 1.3) {
      cat(sprintf("    PASS: Workers ran concurrently (%.1fx speedup)\n", speedup))
      TRUE
    } else if (all_ok) {
      cat(sprintf("    WARN: Workers ran but limited speedup (%.1fx)\n", speedup))
      TRUE
    } else {
      cat("    FAIL: Worker errors detected\n")
      FALSE
    }
  }, error = function(e) {
    cat(sprintf("    FAIL: %s\n", conditionMessage(e)))
    FALSE
  })

  use_parallel <- test_ok
  if (!use_parallel) {
    cat("  -> Falling back to sequential processing.\n")
    n_workers <- 1
  }
}

# Time estimates
if (use_parallel) {
  n_batches_per_var <- ceiling(length(years) / n_workers)
  parallel_time_hrs <- (length(variables) * n_batches_per_var * benchmark$time) / 3600
  speedup <- sequential_time_hrs / parallel_time_hrs

  cat(sprintf("\n  Parallel estimate (%d workers): ~%.1f hours (%.1fx speedup)\n",
              n_workers, parallel_time_hrs, speedup))
  mode_label <- sprintf("PARALLEL (%d workers)", n_workers)
} else {
  cat(sprintf("\n  Sequential estimate: ~%.1f hours\n", sequential_time_hrs))
  mode_label <- "SEQUENTIAL"
}

cat(sprintf("\n  -> Mode: %s\n\n", mode_label))

# ==============================================================================
# Step 5/7: Processing chunks
# ==============================================================================

cat(sprintf("Step 5/7: Processing %d chunks [%s]\n", n_chunks, mode_label))
cat(strrep("-", 60), "\n")

# Initialize progress log
writeLines(character(0), progress_log)
cat(sprintf("  Progress log: %s\n", progress_log))
cat(sprintf("  Monitor live: tail -f %s\n\n", progress_log))

processing_start <- Sys.time()
chunk_num <- 0
chunk_times <- numeric()
total_rows_written <- 0
vars_written <- character()

for (i in seq_along(variables)) {
  var <- variables[i]
  var_file <- file.path(output_dir, sprintf("%s.parquet", var))

  cat(sprintf("Variable %d/%d: %s\n", i, length(variables), var))

  # --- Check existing output for this variable ---
  years_to_process <- years
  existing_data <- NULL

  if (file.exists(var_file)) {
    existing_ds <- open_dataset(var_file)
    existing_col_names <- names(existing_ds$schema)
    missing_cols <- setdiff(summary_cols, existing_col_names)

    existing_years <- existing_ds %>%
      select(calendar_year) %>% distinct() %>% collect() %>% pull()
    missing_years <- setdiff(years, existing_years)

    if (length(missing_cols) == 0 && length(missing_years) == 0) {
      # Fully complete - skip
      existing_rows <- existing_ds %>%
        summarize(n = n()) %>% collect() %>% pull()
      total_rows_written <- total_rows_written + existing_rows
      vars_written <- c(vars_written, var)
      chunk_num <- chunk_num + length(years)
      cat(sprintf("  Already complete (%s rows, all columns). Skipping.\n\n",
                  format(existing_rows, big.mark = ",")))
      next
    }

    if (length(missing_cols) > 0) {
      cat(sprintf("  Missing columns: %s -> reprocessing all years\n",
                  paste(missing_cols, collapse = ", ")))
      years_to_process <- years  # Must redo all to add new columns
    } else {
      cat(sprintf("  Missing %d year(s): %s\n",
                  length(missing_years), paste(missing_years, collapse = ", ")))
      years_to_process <- missing_years
      existing_data <- collect(existing_ds)
      chunk_num <- chunk_num + length(existing_years)
    }
  }

  # --- Process years_to_process ---
  var_results <- list()
  var_start <- Sys.time()

  if (use_parallel) {
    # ---- Parallel: process years in batches ----
    year_batches <- split(years_to_process,
                          ceiling(seq_along(years_to_process) / n_workers))

    for (b in seq_along(year_batches)) {
      batch <- year_batches[[b]]
      batch_start <- Sys.time()

      cat(sprintf("  Batch %d/%d: [%s] (%d workers)...",
                  b, length(year_batches),
                  paste(batch, collapse = ", "),
                  min(length(batch), n_workers)))

      batch_results <- parallel::mclapply(batch, function(year) {
        process_one_chunk(var, year,
                          source_file_map[[as.character(year)]],
                          pixel_map_dt, time_cols, group_id,
                          progress_log)
      }, mc.cores = n_workers)

      batch_wall <- as.numeric(difftime(Sys.time(), batch_start, units = "secs"))

      for (k in seq_along(batch)) {
        r <- batch_results[[k]]
        chunk_num <- chunk_num + 1

        if (inherits(r, "try-error")) {
          cat(sprintf("\n    %d: ERROR - %s", batch[k], as.character(r)))
          next
        }

        if (!is.null(r$result)) {
          var_results[[as.character(batch[k])]] <- r$result
          chunk_times <- c(chunk_times, r$time)
        }
      }

      # Progress update
      pct <- (chunk_num / n_chunks) * 100
      elapsed <- as.numeric(difftime(Sys.time(), processing_start, units = "mins"))
      if (length(chunk_times) > 0) {
        vars_done <- i - 1 + (b / length(year_batches))
        vars_remaining <- length(variables) - vars_done
        avg_batch_wall <- elapsed / (chunk_num / length(years))
        eta <- vars_remaining * avg_batch_wall
      } else {
        eta <- NA
      }

      cat(sprintf(" %.0fs wall | %s %.1f%% | %d/%d | ETA: %s\n",
                  batch_wall, make_progress_bar(pct), pct, chunk_num, n_chunks,
                  if (is.na(eta)) "?" else sprintf("%.0fm", eta)))
    }

  } else {
    # ---- Sequential: process years one by one ----
    for (j in seq_along(years_to_process)) {
      year <- years_to_process[j]
      chunk_num <- chunk_num + 1

      r <- process_one_chunk(var, year,
                             source_file_map[[as.character(year)]],
                             pixel_map_dt, time_cols, group_id,
                             progress_log)

      if (!is.null(r$result)) {
        var_results[[as.character(year)]] <- r$result
        chunk_times <- c(chunk_times, r$time)
      }

      pct <- (chunk_num / n_chunks) * 100
      elapsed <- as.numeric(difftime(Sys.time(), processing_start, units = "mins"))
      avg_chunk <- if (length(chunk_times) > 0) mean(chunk_times) else r$time
      eta <- ((n_chunks - chunk_num) * avg_chunk) / 60

      cat(sprintf("  %d: %5.1fs | %s %.1f%% | %d/%d | Elapsed: %.0fm | ETA: %.0fm\n",
                  year, r$time, make_progress_bar(pct), pct,
                  chunk_num, n_chunks, elapsed, eta))
    }
  }

  # --- Save: combine new results with existing data if needed ---
  var_time <- as.numeric(difftime(Sys.time(), var_start, units = "secs"))

  if (length(var_results) > 0) {
    new_dt <- rbindlist(var_results)

    if (!is.null(existing_data)) {
      # Append new years to existing data
      var_dt <- rbind(as.data.table(existing_data), new_dt)
      rm(existing_data, new_dt)
    } else {
      var_dt <- new_dt
      rm(new_dt)
    }

    write_parquet(as_tibble(var_dt), var_file, compression = "snappy")
    var_rows <- nrow(var_dt)
    var_size_mb <- file.size(var_file) / 1024^2
    total_rows_written <- total_rows_written + var_rows
    vars_written <- c(vars_written, var)
    rm(var_dt)
    cat(sprintf("  Saved: %s rows (%.0f MB) -> %s  [%s]\n\n",
                format(var_rows, big.mark = ","), var_size_mb, basename(var_file),
                format_time(var_time)))
  } else {
    cat("  No new data for this variable.\n\n")
  }

  rm(var_results)
  gc(verbose = FALSE)
}

processing_time <- as.numeric(difftime(Sys.time(), processing_start, units = "mins"))
cat(strrep("-", 60), "\n")
cat(sprintf("  Processing complete: %.1f minutes (%.2f hours)\n", processing_time, processing_time / 60))
if (length(chunk_times) > 0) {
  cat(sprintf("  Avg chunk time: %.1fs | Total chunks: %d\n\n", mean(chunk_times), length(chunk_times)))
}

# ==============================================================================
# Step 6/7: Verify output
# ==============================================================================

cat("Step 6/7: Verifying output\n")
cat(strrep("-", 60), "\n")

if (length(vars_written) == 0) {
  stop("No results produced - check pixel maps and pixel values")
}

# List output files
output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
total_size_mb <- sum(file.size(output_files)) / 1024^2

cat(sprintf("  Output directory: %s\n", output_dir))
cat(sprintf("  Files written: %d\n", length(output_files)))
cat(sprintf("  Total size: %.0f MB\n", total_size_mb))
cat(sprintf("  Total rows: %s\n", format(total_rows_written, big.mark = ",")))

# Verify we can open as a dataset
summaries_ds <- open_dataset(output_dir)
cat(sprintf("  Columns: %s\n", paste(names(summaries_ds$schema), collapse = ", ")))
cat(sprintf("  Verified: open_dataset() reads all %d files\n\n", length(output_files)))

# Clean up progress log
unlink(progress_log)

# ==============================================================================
# Summary
# ==============================================================================

total_time <- as.numeric(difftime(Sys.time(), script_start_time, units = "mins"))

cat("================================================================================\n")
cat(" COMPLETE\n")
cat("================================================================================\n")
cat(sprintf("  Dataset: %s\n", toupper(dataset)))
cat(sprintf("  Mode: %s\n", mode_label))
cat(sprintf("  Output: %s/ (%d parquet files)\n", basename(output_dir), length(output_files)))
cat(sprintf("  Total rows: %s\n", format(total_rows_written, big.mark = ",")))
cat(sprintf("  Total size: %.0f MB\n", total_size_mb))
cat(sprintf("  Variables: %s\n", paste(sort(vars_written), collapse = ", ")))
cat(sprintf("  Year range: %d-%d\n", min(years), max(years)))
cat(sprintf("  Total runtime: %.1f minutes (%.2f hours)\n", total_time, total_time / 60))
cat(sprintf("\n  Read with: open_dataset('%s')\n", output_dir))
cat("================================================================================\n\n")
