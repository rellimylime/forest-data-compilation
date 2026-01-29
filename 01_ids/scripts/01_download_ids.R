# ==============================================================================
# 01_download_ids.R
# Download USDA Forest Service Insect and Disease Detection Survey data
# ==============================================================================

library(yaml)
library(here)

# --- Load config --------------------------------------------------------------
config <- read_yaml(here("config.yaml"))
ids_config <- config$raw$ids

# --- Setup output directory ---------------------------------------------------
output_dir <- here(ids_config$local_dir)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("Source:", ids_config$source, "\n")
cat("Downloading to:", output_dir, "\n\n")

# --- Download each region -----------------------------------------------------
for (region_name in names(ids_config$files)) {
  region <- ids_config$files[[region_name]]
  
  output_path <- file.path(output_dir, region$filename)
  
  # Skip if already exists
  if (file.exists(output_path)) {
    cat("[SKIP]", region_name, "- already exists\n")
    next
  }
  
  cat("[DOWNLOAD]", region_name, "-", region$description, "\n")
  
  download.file(
    url = region$url,
    destfile = output_path,
    mode = "wb"  # binary mode for zip files
  )
}

cat("\nDone.\n")