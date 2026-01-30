# ==============================================================================
# 01_download_ids.R
# Download USDA Forest Service Insect and Disease Detection Survey data
# ==============================================================================

# Load config
source("scripts/utils/load_config.R")
config <- load_config()

library(fs)
library(httr)
library(glue)

# Set up directories
raw_dir <- here(config$raw$ids$local_dir)
dir_create(raw_dir)

# Get list of regions from config
regions <- config$raw$ids$files

# Download function
download_ids_region <- function(region_name, region_info, dest_dir) {
  
  dest_file <- file.path(dest_dir, region_info$filename)
  
  # Skip if already exists
  if (file_exists(dest_file)) {
    message(glue("Skipping {region_name}: {region_info$filename} already exists"))
    return(invisible(NULL))
  }
  
  message(glue("Downloading {region_name}: {region_info$description}"))
  
  # Download with progress
  response <- GET(
    region_info$url,
    write_disk(dest_file, overwrite = FALSE),
    progress()
  )
  
  # Check for errors
  if (http_error(response)) {
    warning(glue("Failed to download {region_name}: HTTP {status_code(response)}"))
    file_delete(dest_file)  # Clean up partial download
    return(invisible(NULL))
  }
  
  message(glue("  Saved: {dest_file} ({file_size(dest_file)})"))
  
  # Unzip
  gdb_dir <- file.path(dest_dir, gsub("\\.zip$", "", region_info$filename))
  if (!dir_exists(gdb_dir)) {
    message(glue("  Unzipping..."))
    unzip(dest_file, exdir = dest_dir)
  }
  
  invisible(dest_file)
}

# Download all regions
message(glue("Downloading IDS data to: {raw_dir}\n"))

for (region_name in names(regions)) {
  download_ids_region(region_name, regions[[region_name]], raw_dir)
  message("")  # blank line between regions
}

message("Done.")