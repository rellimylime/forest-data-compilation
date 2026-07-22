# ==============================================================================
# parquet_atomic.R
# Atomic Parquet writer: write to a temp file in the destination directory, then
# rename over the target. Guarantees a reader never sees a half-written canonical
# product, and a failed/interrupted write leaves the previous file intact.
# ==============================================================================

write_parquet_atomic <- function(df, path, compression = "snappy") {
  dir_path <- dirname(path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  tmp_path <- tempfile(
    pattern = paste0(basename(path), "_tmp_"),
    tmpdir = dir_path,
    fileext = ".parquet"
  )
  ok <- FALSE
  on.exit(if (!ok && file.exists(tmp_path)) unlink(tmp_path, force = TRUE), add = TRUE)
  arrow::write_parquet(df, tmp_path, compression = compression)
  # file.rename is atomic within the same directory/filesystem. Do not fall back
  # to copy-overwrite: a copy can expose a partial canonical file to readers.
  # If this filesystem cannot atomically replace the target, leave the previous
  # target intact when possible and fail with the temporary file cleaned up.
  if (!isTRUE(file.rename(tmp_path, path))) {
    stop(sprintf(
      "Atomic Parquet replace failed for '%s'; previous output was not intentionally overwritten.",
      path
    ))
  }
  ok <- TRUE
  invisible(path)
}
