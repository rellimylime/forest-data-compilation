# FIA Survey-Interval Foundation

This document defines the implemented common foundation for interval-specific MTBS and IDS linkage. It is intentionally source-neutral: fire and insect/disease products must consume the same interval identifiers and endpoint dates.

## Products and grains

### `plot_visit_context.parquet`

Path:

```text
05_fia/data/processed/summaries/plot_visit_context.parquet
```

One row is one raw FIADB `PLOT` record, identified by `PLT_CN`. All raw visits are retained, including visits outside the configured 2000–2024 analysis window and nonsampled plot records. This is necessary because `PREV_PLT_CN` can point outside the processed condition-data window.

The product carries:

- official identity and predecessor fields;
- `MEASYEAR`, `MEASMON`, and `MEASDAY`;
- bounded measurement dates and their precision/source;
- public coordinates;
- plot sampling status and nonsample reason;
- protocol fields including `MANUAL`, `DESIGNCD`, `REMPER`, `KINDCD`, `SAMP_METHOD_CD`, `CYCLE`, and `SUBCYCLE`;
- `is_sampled_plot`, `has_usable_coordinates`, and `in_configured_inventory_window`.

### `fia_visit_pairing_audit.parquet`

Path:

```text
08_disturbance_linkage/data/processed/fia_visit_pairing_audit.parquet
```

One row is one current-window FIA `PLOT` record. This table retains unpaired records and is the authority for linkage attrition. `current_PLT_CN` is unique.

The audit separates:

- whether an official link is present;
- whether its target is available;
- whether the target belongs to the expected `stable_plot_id`;
- whether the target is the chronological predecessor;
- whether a sampled chronological fallback exists;
- whether endpoint dates are ordered;
- whether the resolved pair meets the technical usability contract.

### `fia_survey_intervals.parquet`

Path:

```text
08_disturbance_linkage/data/processed/fia_survey_intervals.parquet
```

One row is one resolved previous/current pair. A row is emitted only when an available previous PLOT endpoint on the same stable plot can be selected. Nonsampled endpoints and date problems are retained with explicit flags rather than discarded.

`interval_id` is a deterministic, versioned composite of:

```text
stable_plot_id
previous_PLT_CN
current_PLT_CN
```

The current representation is:

```text
fia_interval_v1|{stable_plot_id}|{previous_PLT_CN}|{current_PLT_CN}
```

The readable representation avoids loss of precision from serializing FIADB 64-bit identifiers and makes key derivation independently reproducible.

## Pair selection

Official `PREV_PLT_CN` is authoritative when its target is available and belongs to the current record's stable plot. If that target is nonadjacent in date order, the official target is still followed and the interval is classified as `previous_link_to_nonadjacent_visit`.

When `PREV_PLT_CN` is null, the table records the most recent earlier sampled
visit with the same `stable_plot_id` as a fallback candidate. In plain language,
this means only: "this is the nearest earlier row that might be the prior visit."
FIA did not supply the link, so the fallback is not proof of a remeasurement and
must not be admitted to an analysis automatically. A fallback never overrides a
present official link.

Cross-stable targets and unavailable official targets remain in the audit but do not become intervals.

### Interpretation warning before analysis

The USDA Forest Service [FIADB Database Description, version 9.3](https://research.fs.usda.gov/sites/default/files/2024-12/wo-v9-3_dec2024_ug_fiadb_database_description_nfi.pdf) defines `PREV_PLT_CN` as the previous inventory record for the location, but explicitly notes that it does not link to a previous record classified as periodic. This matters for the current audit:

- For 6,367 visits, FIA provides no previous-visit link, but an earlier record has the same `stable_plot_id`. The earlier record is retained only as an audit candidate.
- In 26 Utah cases, FIA links a 2010 visit to 2000 even though a 2005 record has the same `stable_plot_id`. The workflow preserves FIA's link and skips the 2005 record.
- In 52 cases, FIA's previous-visit link points to a record with a different `stable_plot_id`. These links are excluded from interval products pending review.

## Pairing classes

The classes are mutually exclusive:

| Class | Meaning |
| --- | --- |
| `structural_match` | Available same-plot official target is the immediately preceding available PLOT record. |
| `previous_link_to_nonadjacent_visit` | Available same-plot official target is not the immediately preceding PLOT record; the official target is followed. |
| `previous_link_missing` | Official link is null and an earlier sampled visit supplies a chronological fallback. |
| `previous_link_target_unavailable` | Official target is not present in the raw PLOT snapshot. |
| `previous_link_conflict` | Official target is available but belongs to another stable plot identifier. |
| `first_observed_visit` | No official link and no earlier sampled visit is available. |

Date quality is stored in `date_status`, not encoded as a pairing class. This keeps structural linkage and temporal ordering as independent dimensions.

## Date bounds

Date precision is preserved as follows:

| Available fields | Lower bound | Upper bound | Precision |
| --- | --- | --- | --- |
| Valid year, month, day | exact date | exact date | `day` |
| Valid year and month | first of month | last of month | `month` |
| Valid measurement year | January 1 | December 31 | `year` |
| Only valid `INVYR` | January 1 | December 31 | `inventory_year_fallback` |
| No valid year | missing | missing | `missing` |

Invalid day or month values are demoted to the next defensible precision and recorded in `measurement_date_issue`.

Interval duration bounds use 365.2425 days per fractional year:

```text
interval_years_min =
    (current_date_lower - previous_date_upper) / 365.2425

interval_years_max =
    (current_date_upper - previous_date_lower) / 365.2425
```

## Technical usability

`pairing_usable` is a technical flag, not final scientific cohort eligibility. It is true only when:

- a previous endpoint is resolved;
- both endpoint PLOT records have `PLOT_STATUS_CD == 1`;
- endpoint date bounds are strictly ordered.

It does not require community-composition data, FIA condition eligibility, MTBS/IDS coverage, a disturbance candidate, or any thermophilization threshold. Those remain downstream decisions.

## QA outputs

```text
08_disturbance_linkage/qa/outputs/survey_interval_pairing_counts.csv
08_disturbance_linkage/qa/outputs/fia_survey_interval_validation_checks.csv
```

Pairing counts are grouped by:

```text
pairing_class
state
previous_INVYR
current_INVYR
```

Validation checks enforce primary keys, endpoint referential integrity, configured-window coverage, official/fallback rules, interval-ID determinism, and the technical usability contract.

## Run order

```bash
Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R
Rscript 08_disturbance_linkage/scripts/fia/01_build_survey_intervals.R
Rscript 08_disturbance_linkage/qa/scripts/fia/01_validate_survey_intervals.R
Rscript scripts/run_tests.R 08_disturbance_linkage
```

## Implemented baseline counts

For the current local raw FIA snapshot and configured 2000–2024 current window:

| Pairing class | Current visits | Technically usable |
| --- | ---: | ---: |
| `structural_match` | 822,792 | 326,064 |
| `previous_link_missing` | 6,367 | 6,367 |
| `previous_link_to_nonadjacent_visit` | 26 | 21 |
| `previous_link_target_unavailable` | 31,293 | 0 |
| `previous_link_conflict` | 52 | 0 |
| `first_observed_visit` | 360,743 | 0 |

The audit contains 1,221,273 current-window PLOT records. The interval table contains 829,185 resolved pairs, of which 332,452 meet the technical usability contract.

## Next implementation slice

The next spatial step should build a reusable endpoint/coordinate buffer product before linking either external source:

1. Derive a deterministic `coordinate_id` from public latitude/longitude.
2. Link every visit endpoint to that coordinate ID.
3. Build one 800 m search buffer per unique coordinate, using region-appropriate spatial operations for CONUS, Alaska, and Hawaii.
4. Intersect MTBS and IDS with coordinate buffers once.
5. Assemble previous/current interval evidence through endpoint joins.

This prevents repeated spatial work for unchanged coordinates and avoids using EPSG:5070 outside its appropriate area.
