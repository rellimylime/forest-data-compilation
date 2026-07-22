# ==============================================================================
# fia_year_schema.R
# Shared data contract for FIA calendar-year fields (TRTYR*/DSTRBYR*).
#
# Background: COND.csv year slots (TRTYR2/3, DSTRBYR2/3) are entirely empty in
# states where no plot ever recorded a 2nd/3rd treatment or disturbance year.
# data.table::fread infers such all-NA columns as `logical`, which is written to
# the state Parquet as a Boolean column. When the per-state partitions are then
# combined with arrow::open_dataset (schema taken from the first fragment), a
# Boolean fragment can force the whole national column to Boolean, silently
# casting real integer years to TRUE. This module enforces a nullable-integer
# contract on both the producer (write) and reader (national union) sides so the
# result never depends on partition/file order.
#
# Do NOT convert year fields to strings; FIA years are numeric. Documented
# sentinels (e.g. 9999 = continuous/unknown) are preserved as integers here and
# excluded from time-since arithmetic downstream, not at read time.
# ==============================================================================

# Calendar-year (and sentinel-year) fields that pass through the state-partition
# and national-union code paths and must stay nullable integer.
FIA_YEAR_FIELDS <- c(
  "TRTYR1", "TRTYR2", "TRTYR3",
  "DSTRBYR1", "DSTRBYR2", "DSTRBYR3"
)

# Producer-side contract: cast any present year field to nullable integer in a
# data.table/data.frame before writing a state Parquet. Safe on all-NA logical
# columns (NA logical -> NA integer) and on numeric year columns.
cast_fia_year_fields <- function(dt, year_fields = FIA_YEAR_FIELDS) {
  present <- intersect(year_fields, names(dt))
  for (col in present) {
    v <- dt[[col]]
    if (!is.integer(v)) {
      data.table::set(dt, j = col, value = as.integer(v))
    }
  }
  invisible(dt)
}

# Reader-side contract: open a hive-partitioned cond dataset with an explicit
# schema that forces every year field to int32, so a Boolean fragment (all-NA)
# is promoted to integer NA rather than dragging real years down to Boolean.
# Independent of which partition arrow reads first.
open_cond_dataset <- function(cond_dir, partitioning = "state",
                              year_fields = FIA_YEAR_FIELDS) {
  ds0 <- arrow::open_dataset(cond_dir, partitioning = partitioning)
  sch <- ds0$schema
  needs_fix <- FALSE
  flds <- lapply(seq_len(sch$num_fields), function(i) {
    f <- sch$field(i - 1L)
    if (f$name %in% year_fields && f$type$ToString() != "int32") {
      needs_fix <<- TRUE
      arrow::field(f$name, arrow::int32())
    } else {
      f
    }
  })
  if (!needs_fix) {
    return(ds0)
  }
  new_schema <- do.call(arrow::schema, flds)
  arrow::open_dataset(cond_dir, schema = new_schema, partitioning = partitioning)
}

# Validation contract: TRUE iff every present year field in a schema/dataset is
# int32. Used by the reader and by tests to prove partition order cannot corrupt
# the national schema.
fia_year_fields_are_integer <- function(x, year_fields = FIA_YEAR_FIELDS) {
  sch <- if (inherits(x, "Schema")) x else x$schema
  present <- intersect(year_fields, sch$names)
  all(vapply(present, function(nm) {
    sch$GetFieldByName(nm)$type$ToString() == "int32"
  }, logical(1)))
}

# Convenience assert for reader entry points; stops with a clear, actionable
# message if the national year schema is not integer.
assert_fia_year_schema <- function(x, context = "cond dataset") {
  if (!fia_year_fields_are_integer(x)) {
    sch <- if (inherits(x, "Schema")) x else x$schema
    present <- intersect(FIA_YEAR_FIELDS, sch$names)
    types <- vapply(present, function(nm) sch$GetFieldByName(nm)$type$ToString(),
                    character(1))
    stop(sprintf(
      paste0("FIA year-field schema contract violated in %s: %s. ",
             "Expected all int32. Re-extract state partitions with the ",
             "producer cast (cast_fia_year_fields) or open via open_cond_dataset()."),
      context, paste(sprintf("%s=%s", present, types), collapse = ", ")
    ))
  }
  invisible(TRUE)
}
