# Shared helpers for the cumulative-mortality workflow.

fia_agent_family <- function(code) {
  data.table::fcase(
    !is.na(code) & code >= 10L & code <= 19L, "insect",
    !is.na(code) & code >= 20L & code <= 29L, "disease",
    !is.na(code) & code >= 30L & code <= 39L, "fire",
    !is.na(code) & code > 0L, "other",
    default = NA_character_
  )
}

fia_agent_completeness <- function(code) {
  data.table::fcase(
    is.na(code), "missing",
    code == 0L, "zero",
    code > 0L, "positive",
    default = "unexpected"
  )
}

fia_is_verified_interval_death <- function(component) {
  component %in% c("MORTALITY1", "MORTALITY2")
}

fia_condition_interval_eligible <- function(
    same_condid,
    t1_status,
    t2_status,
    t1_prop,
    t2_prop,
    cutoff = 0.30) {
  !is.na(same_condid) & same_condid &
    !is.na(t1_status) & t1_status == 1L &
    !is.na(t2_status) & t2_status == 1L &
    !is.na(t1_prop) & t1_prop >= cutoff &
    !is.na(t2_prop) & t2_prop >= cutoff
}

# Identify the FIA sampling element represented by a TREE row.
fia_sampling_element <- function(
    diameter,
    tpa_unadj = NA_real_,
    subptyp = NA_integer_) {
  n <- max(length(diameter), length(tpa_unadj), length(subptyp))
  diameter <- rep_len(diameter, n)
  tpa_unadj <- rep_len(tpa_unadj, n)
  subptyp <- rep_len(subptyp, n)

  data.table::fcase(
    !is.na(subptyp) & subptyp == 2L, "microplot",
    !is.na(subptyp) & subptyp == 1L, "subplot",
    !is.na(subptyp) & subptyp == 3L, "macroplot",
    !is.na(diameter) & diameter >= 1 & diameter < 5, "microplot",
    !is.na(tpa_unadj) & abs(tpa_unadj - 0.999188) < 0.02, "macroplot",
    !is.na(diameter) & diameter >= 5, "subplot",
    default = "unresolved"
  )
}

fia_mid_date <- function(lower, upper) {
  lower <- as.Date(lower)
  upper <- as.Date(upper)
  as.Date(
    ifelse(
      !is.na(lower) & !is.na(upper),
      as.numeric(lower) + (as.numeric(upper) - as.numeric(lower)) / 2,
      ifelse(!is.na(lower), as.numeric(lower), as.numeric(upper))
    ),
    origin = "1970-01-01"
  )
}
