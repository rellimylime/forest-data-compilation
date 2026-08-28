source(here::here("tests/testthat/helpers.R"))
source(here::here("scripts/utils/fia_components.R"))

suppressPackageStartupMessages({
  library(data.table)
})

# Contract sections 2-6; invariants 1-5; acceptance fixtures A, B, C.
# 09_analysis/docs/analysis_contract.md

visit <- function(plt, prev, year, plot_id = "X") {
  data.table(
    PLT_CN = plt,
    PREV_PLT_CN = prev,
    stable_plot_id = plot_id,
    INVYR = year,
    measurement_date_lower = as.IDate(sprintf("%d-07-01", year))
  )
}

# ------------------------------------------------------------------------------
# Fixture A -- continuous chain
# ------------------------------------------------------------------------------

fixture_a <- function() {
  rbindlist(list(
    visit("A", NA_character_, 2000L),
    visit("B", "A", 2005L),
    visit("C", "B", 2010L)
  ))
}

test_that("fixture A: a continuous chain is one component with A->C endpoints", {
  out <- fia_add_remeasurement_components(fixture_a())

  expect_equal(uniqueN(out$remeasurement_component_id), 1L)
  expect_equal(unique(out$n_visits_in_component), 3L)
  expect_equal(out[PLT_CN == "A", visit_link_status], "no_official_link")
  expect_equal(out[PLT_CN %in% c("B", "C"), sum(connectivity_edge_valid)], 2L)

  endpoints <- fia_component_endpoints(out)
  expect_equal(endpoints[component_endpoint == "first", PLT_CN], "A")
  expect_equal(endpoints[component_endpoint == "last", PLT_CN], "C")
  # B is retained in the component even though it is not an endpoint.
  expect_equal(unique(endpoints$n_visits_in_component), 3L)
})

# ------------------------------------------------------------------------------
# Fixture B -- broken chain
# ------------------------------------------------------------------------------

fixture_b <- function() {
  rbindlist(list(
    visit("A", NA_character_, 2000L),
    visit("B", "A", 2005L),
    visit("C", NA_character_, 2010L),
    visit("D", "C", 2015L)
  ))
}

test_that("fixture B: a null PREV_PLT_CN splits one plot into two components", {
  out <- fia_add_remeasurement_components(fixture_b())

  expect_equal(uniqueN(out$remeasurement_component_id), 2L)
  expect_equal(
    out[PLT_CN == "A", remeasurement_component_id],
    out[PLT_CN == "B", remeasurement_component_id]
  )
  expect_equal(
    out[PLT_CN == "C", remeasurement_component_id],
    out[PLT_CN == "D", remeasurement_component_id]
  )
  # The two halves must not be joined.
  expect_false(identical(
    out[PLT_CN == "B", remeasurement_component_id],
    out[PLT_CN == "C", remeasurement_component_id]
  ))
})

test_that("invariant 1: a null PREV_PLT_CN never becomes a connectivity edge", {
  out <- fia_add_remeasurement_components(fixture_b())
  expect_false(out[PLT_CN == "C", connectivity_edge_valid])
  expect_equal(out[PLT_CN == "C", visit_link_status], "no_official_link")
})

test_that("invariant 2: chronological adjacency does not create an edge", {
  # C follows B in time on the same plot, and FIA supplies no link. No amount of
  # chronological closeness may turn that into a remeasurement.
  out <- fia_add_remeasurement_components(fixture_b())
  pairs <- fia_component_endpoints(out)

  first_last <- dcast(
    pairs[component_endpoint != "only"],
    remeasurement_component_id ~ component_endpoint,
    value.var = "PLT_CN"
  )
  observed <- paste(first_last$first, first_last$last, sep = "->")
  expect_setequal(observed, c("A->B", "C->D"))
  expect_false("B->C" %in% observed)
  expect_false("A->D" %in% observed)
})

test_that("invariants 3 and 4: endpoints never span two components", {
  out <- fia_add_remeasurement_components(fixture_b())
  pairs <- fia_component_endpoints(out)
  wide <- dcast(
    pairs[component_endpoint != "only"],
    remeasurement_component_id ~ component_endpoint,
    value.var = "PLT_CN"
  )
  wide <- merge(
    wide,
    out[, .(first = PLT_CN, comp_first = remeasurement_component_id)],
    by = "first"
  )
  wide <- merge(
    wide,
    out[, .(last = PLT_CN, comp_last = remeasurement_component_id)],
    by = "last"
  )
  expect_true(all(wide$comp_first == wide$comp_last))
  expect_silent(fia_assert_within_component(wide, "comp_first", "comp_last"))
})

test_that("the cross-component guard actually fires", {
  bad <- data.table(comp_first = "c1", comp_last = "c2")
  expect_error(
    fia_assert_within_component(bad, "comp_first", "comp_last"),
    "different remeasurement components"
  )
})

# ------------------------------------------------------------------------------
# Fixture C -- explicit nonadjacent link
# ------------------------------------------------------------------------------

fixture_c <- function() {
  rbindlist(list(
    visit("A", NA_character_, 2000L),
    visit("B", NA_character_, 2005L),
    visit("C", "A", 2010L)
  ))
}

test_that("fixture C / invariant 5: an explicit nonadjacent link is kept", {
  out <- fia_add_remeasurement_components(fixture_c())

  # A and C are connected because FIA says so, even though B falls between them.
  expect_true(out[PLT_CN == "C", connectivity_edge_valid])
  expect_equal(
    out[PLT_CN == "A", remeasurement_component_id],
    out[PLT_CN == "C", remeasurement_component_id]
  )
  # B has no link of its own, so it is a separate single-visit component.
  expect_false(identical(
    out[PLT_CN == "B", remeasurement_component_id],
    out[PLT_CN == "A", remeasurement_component_id]
  ))
  expect_equal(out[PLT_CN == "B", n_visits_in_component], 1L)

  endpoints <- fia_component_endpoints(out)
  ac <- endpoints[
    remeasurement_component_id == out[PLT_CN == "A", remeasurement_component_id]
  ]
  expect_equal(ac[component_endpoint == "first", PLT_CN], "A")
  expect_equal(ac[component_endpoint == "last", PLT_CN], "C")
  # B->C must not be manufactured just because B is chronologically closer.
  expect_false("B" %in% ac$PLT_CN)
})

# ------------------------------------------------------------------------------
# Link classification and determinism
# ------------------------------------------------------------------------------

test_that("a link to another stable plot is not a connectivity edge", {
  visits <- rbindlist(list(
    visit("A", NA_character_, 2000L, plot_id = "X"),
    visit("B", "A", 2005L, plot_id = "Y")
  ))
  out <- fia_add_remeasurement_components(visits)
  expect_false(out[PLT_CN == "B", connectivity_edge_valid])
  expect_equal(
    out[PLT_CN == "B", visit_link_status],
    "official_target_other_stable_plot"
  )
  expect_equal(uniqueN(out$remeasurement_component_id), 2L)
})

test_that("a link to a visit missing from the snapshot is not an edge", {
  visits <- visit("B", "GONE", 2005L)
  out <- fia_add_remeasurement_components(visits)
  expect_false(out[PLT_CN == "B", connectivity_edge_valid])
  expect_equal(
    out[PLT_CN == "B", visit_link_status],
    "official_target_unavailable"
  )
})

test_that("component identifiers do not depend on row order", {
  visits <- fixture_b()
  forward <- fia_add_remeasurement_components(visits)
  reversed <- fia_add_remeasurement_components(visits[rev(seq_len(.N))])

  key <- function(x) {
    setorder(x[, .(PLT_CN, remeasurement_component_id)], PLT_CN)[]
  }
  expect_equal(key(forward), key(reversed))
})

test_that("component identifiers are anchored on the smallest PLT_CN", {
  out <- fia_add_remeasurement_components(fixture_a())
  expect_true(all(grepl("^fia_component_v1\\|X\\|A$", out$remeasurement_component_id)))
})

test_that("a duplicated PLT_CN is rejected before components are built", {
  visits <- rbindlist(list(
    visit("A", NA_character_, 2000L),
    visit("A", NA_character_, 2005L)
  ))
  expect_error(fia_add_remeasurement_components(visits), "unique by PLT_CN")
})

# ------------------------------------------------------------------------------
# connectivity_edge_valid means connectivity and nothing else
# ------------------------------------------------------------------------------

test_that("connectivity_edge_valid is exactly the three link conditions", {
  # Present + resolves + same stable plot. Nothing else may enter the flag.
  out <- fia_add_remeasurement_components(fixture_a())
  expect_true(all(
    out$connectivity_edge_valid ==
      (out$official_link_present &
         out$official_target_available &
         out$official_target_same_stable_plot)
  ))
})

test_that("connectivity ignores sampled status and date ordering", {
  # B was never sampled and its date is out of order. Those make it unusable as a
  # response endpoint, but FIA still asserts the link, so the plot stays connected.
  visits <- rbindlist(list(
    visit("A", NA_character_, 2010L),
    visit("B", "A", 2005L)
  ))
  visits[, `:=`(
    PLOT_STATUS_CD = c(1L, 2L),
    is_sampled_plot = c(TRUE, FALSE)
  )]
  out <- fia_add_remeasurement_components(visits)

  expect_true(out[PLT_CN == "B", connectivity_edge_valid])
  expect_equal(uniqueN(out$remeasurement_component_id), 1L)
})

test_that("connectivity ignores whether a visit carries community data", {
  # The same plot must produce the same components for seedlings, saplings, and
  # trees. If layer availability leaked into the flag, a plot could be connected
  # for one life stage and disconnected for another.
  visits <- fixture_a()
  base <- fia_add_remeasurement_components(visits)

  for (missing_visit in c("A", "B", "C")) {
    layered <- copy(visits)
    layered[, has_layer := PLT_CN != missing_visit]
    out <- fia_add_remeasurement_components(layered)
    expect_equal(
      out[order(PLT_CN), remeasurement_component_id],
      base[order(PLT_CN), remeasurement_component_id],
      info = paste("components changed when", missing_visit, "lacked layer data")
    )
  }
})

# ------------------------------------------------------------------------------
# Invariant 6 -- PREV defines visits; stable CONDID selects conditions
# ------------------------------------------------------------------------------

test_that("invariant 6: CONDID cannot define a history by itself", {
  response <- data.table(
    remeasurement_component_id = "c1", CONDID = 1L, change_mean_temp = 0.4
  )
  expect_error(
    fia_assert_condition_history_linkage(
      response,
      key_cols = "CONDID"
    ),
    "without remeasurement_component_id"
  )
})

test_that("invariant 6: different endpoint conditions are rejected", {
  response <- data.table(
    remeasurement_component_id = "c1",
    T1_CONDID = 1L, T2_CONDID = 2L,
    change_mean_temp = 0.4
  )
  expect_error(
    fia_assert_condition_history_linkage(response),
    "different T1 and T2"
  )
})

test_that("invariant 6: PREVCOND has no place in a response table", {
  response <- data.table(
    remeasurement_component_id = "c1", PREVCOND = 1L, change_mean_temp = 0.4
  )
  expect_error(fia_assert_condition_history_linkage(response), "PREVCOND")
})

test_that("invariant 6: the approved stable-condition response passes", {
  response <- data.table(
    remeasurement_component_id = "c1",
    stable_plot_id = "X",
    CONDID = 1L,
    life_stage = "trees",
    first_PLT_CN = "A", last_PLT_CN = "C",
    mean_temp_first = 10.1, mean_temp_last = 10.5,
    change_mean_temp = 0.4
  )
  expect_silent(fia_assert_condition_history_linkage(
    response,
    key_cols = c("remeasurement_component_id", "CONDID", "life_stage")
  ))
})

test_that("invariant 6: a single within-visit CONDID column is allowed", {
  condition_table <- data.table(
    PLT_CN = "A", INVYR = 2020L, CONDID = 1L, CONDPROP_UNADJ = 0.35
  )
  expect_silent(fia_assert_condition_history_linkage(condition_table))
})

test_that("a single-visit component cannot supply a change response", {
  out <- fia_add_remeasurement_components(visit("A", NA_character_, 2000L))
  endpoints <- fia_component_endpoints(out)
  expect_equal(endpoints$component_endpoint, "only")
  expect_equal(nrow(endpoints[component_endpoint != "only"]), 0L)
})

test_that("only usable visits become endpoints, but all visits stay counted", {
  # B carries no community data for this layer. The edge A->B->C is still real,
  # so the response runs A->C and B remains inside the component.
  visits <- fixture_a()
  out <- fia_add_remeasurement_components(visits)
  out[, has_layer := PLT_CN != "B"]

  endpoints <- fia_component_endpoints(out, usable_col = "has_layer")
  expect_equal(endpoints[component_endpoint == "first", PLT_CN], "A")
  expect_equal(endpoints[component_endpoint == "last", PLT_CN], "C")
  expect_equal(unique(endpoints$n_visits_in_component), 3L)
  expect_equal(unique(endpoints$n_usable_visits_in_component), 2L)
})
