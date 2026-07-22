# Thermophilization archive notes

A running record of fields, columns, and approaches that were **removed or
replaced** in the thermophilization work, and why. This exists so the live
documentation (`README.md`, the internal meeting brief) can describe only what
the products contain *now*, without carrying "we used to have X, it was removed"
notes inline. When you remove or replace something, move the explanation here
and leave the live docs describing only the current state.

Each entry: what it was, why it went away, and what to use instead.

---

## Removed derived disturbance-severity categories

**Removed:** `fire_severity_class`, `plot_disturbance_extent_class`
(previously written by `07_thermophilization/scripts/03_build_plot_disturbance_severity.R`).

**What they were:** categorical labels that bucketed continuous FIA crown-fire
area coverage into named severity/extent classes (e.g. a "high severity" label
applied above some fixed share of the plot).

**Why removed:** the class boundaries were an analyst-chosen label layered on top
of the data, not something FIA reports. That hid the decision (what counts as
"high severity") inside the build script, where it was easy to miss and hard to
change.

**Use instead:** grade high-severity fire directly from the FIA crown-fire area
coverage that the class was derived from — `prop_crown_fire` (share of the whole
plot) or `forested_prop_crown_fire` (share of forested area), both from
`COND.DSTRBCD == 32`. The single cutoff lives in
`config.yaml -> processed.thermophilization.high_severity_fire` (`column` +
`threshold`). Script 03 writes `is_high_severity_fire` by thresholding that
column; until a threshold is set, `is_high_severity_fire` is `NA` for every row.
This keeps the raw continuous coverage in the product and moves the one
judgement call (the cutoff) to a single, visible, reviewable place.

**Note — not removed:** the condition-level `is_high_severity_proxy`
(= `has_crown_fire_condition`) in `plot_disturbance_classification.parquet` is a
plain alias of the raw crown-fire code, not a threshold, and remains. It is a
different field from the plot-level, threshold-based `is_high_severity_fire`.
