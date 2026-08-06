# 08 Disturbance Linkage Workflow

Configuration lives under `processed.disturbance_linkage` in `config.yaml`. This workflow prepares reusable evidence; it does not choose cohorts, temporal endpoints, primary severity measures, or modeling tables.

## FIA fire preparation

`02_build_fia_forest_disturbance_measures.R` reads the forested-condition foundation and plot-visit context.

Each FIA condition has three code-year disturbance slots:

```text
DSTRBCD1 <-> DSTRBYR1
DSTRBCD2 <-> DSTRBYR2
DSTRBCD3 <-> DSTRBYR3
```

Only slots whose code is `30`, `31`, or `32` enter fire evidence. General disturbance evidence may retain every disturbance slot separately.

Fire-year status is evaluated against `MEASYEAR`, then the measurement-date upper bound, then `INVYR` as fallback:

```text
valid
continuous_or_unknown
date_unavailable
invalid_year_code
post_measurement_year
```

Outputs:

```text
fia_fire_disturbance_slot_evidence.parquet
  stable_plot_id x PLT_CN x INVYR x CONDID x disturbance_slot

fia_forest_disturbance_measures.parquet
  stable_plot_id x PLT_CN x INVYR

qa/outputs/fia_fire_years_restored_by_measurement_timing.csv
  one same-slot fire record valid under MEASYEAR but invalid under INVYR-only logic
```

The known-wrong development output that collected nonfire years is not retained as a product. The canonical filenames above contain the corrected method.

## FIA tree damage-agent preparation

The lookup is extracted from Appendix H of the official FIADB Database Description v9.4:

```bash
python 05_fia/scripts/09_build_damage_agent_lookup.py
```

The lookup retains exact codes, official names and groups, thresholds, the Appendix H region field, retirement status, and a reviewed insect flag. Its definitions are explicitly labeled `official_v9.4_definition_only`. `manual_version_applicability` is `not_established_by_source_appendix`; the current appendix is not treated as a historical code crosswalk.

`05_fia/scripts/10_audit_tree_cn.py` scanned all 26,113,544 rows in the 50 physical raw `TREE` files and found no duplicated `TREE.CN` values. Products still declare the complete tree-agent key rather than depending on an undocumented uniqueness assumption.

The preparation script reads raw `TREE` records and writes state-partitioned Parquet datasets:

```text
fia_tree_damage_agent_evidence/
  PLT_CN x INVYR x CONDID x TREE_CN x DAMAGE_AGENT_CD

fia_condition_damage_denominators/
  stable_plot_id x PLT_CN x INVYR x CONDID

fia_condition_damage_agent_candidates/
  stable_plot_id x PLT_CN x INVYR x CONDID x DAMAGE_AGENT_CD

fia_insect_severity_candidates/
  stable_plot_id x PLT_CN x INVYR x CONDID x DAMAGE_AGENT_CD
```

The general products contain all FIA condition classes. Filter `COND_STATUS_CD == 1` only when constructing a forest-specific dataset. Tree evidence also retains unmatched raw TREE records with `condition_join_status == "condition_unmatched"` and null condition fields. These records cannot enter condition fractions until matching raw COND data exist.

Each matched observation retains `MEASYEAR`, `MEASMON`, `MANUAL`, `KINDCD`, and `CYCLE`. The following fields keep definition scope separate from code lookup coverage:

```text
review_status
definition_applicability_status
region_applicability_status
```

Thus, finding an exact code in Appendix H does not by itself certify its meaning for an earlier manual or a region-specific observation.

The current candidate denominator is identified explicitly as:

```text
live_tree_dia_ge_1in_positive_tpa_unadj
```

Structured columns retain:

```text
eligible_status_codes
minimum_diameter_inches
requires_positive_tpa
denominator_definition_id
mortality_agents_included
```

Candidate fractions are:

```text
tree_record_fraction
tpa_unadj_fraction
basal_area_fraction
```

None is primary. Condition-area weighting and plot-level aggregation are not applied. The tree-agent evidence is retained so a future reviewed grouping can deduplicate trees carrying multiple exact codes.

The earlier, misleading `fia_insect_impact_measures_v1` development product and producer were removed. New builds use `03_prepare_fia_damage_agent_evidence.R`.

See [FIA_DISTURBANCE_DATA_DICTIONARY.md](FIA_DISTURBANCE_DATA_DICTIONARY.md) for exact raw-column provenance, units, formulas, and eligibility rules.

## Biological product rule

Existing condition-level tree, sapling, seedling, and community inputs remain general-purpose. Do not rebuild them solely to remove nonforest conditions.

For forest-specific work:

```text
join on PLT_CN x INVYR x CONDID
filter COND_STATUS_CD == 1
then perform the reviewed aggregation
```

`CONDID` is visit-local and must never be used to match conditions through time.

## Coordinates and external evidence

Foundational tables preserve coordinate provenance. The agreed primary-analysis rule excludes all visits of a `stable_plot_id` with multiple distinct public coordinate pairs. The exclusion is an eligibility decision, not deletion from source or preparation tables.

An external overlap with an 800 m public-coordinate buffer is an association, not proof that the disturbance affected the measured FIA condition.

`01_build_fia_visit_spatial_linkage_status.R` writes:

```text
fia_visit_spatial_linkage_status.parquet
  stable_plot_id x PLT_CN x INVYR
```

It retains each visit's reported `LAT` and `LON`. `linkage_LAT` and `linkage_LON` are populated only when the stable plot has exactly one usable coordinate across its visits. Coordinate identity is exact at the source's six-decimal precision. Every row for a multi-coordinate stable plot has `eligible_spatial_linkage = FALSE`.

The status script verifies the eligible IDs in `plot_footprints.gpkg`; the
footprint producer rebuilds the 800 m buffers from the current single-coordinate
plot set. Neither script chooses, averages, or substitutes coordinates.

`03_extract_ids_annual_agent_history.R` writes separate, resumable year partitions:

```text
ids_annual_agent_evidence/
  stable_plot_id x survey_year x dca_code

ids_annual_survey_coverage/
  stable_plot_id x survey_year
```

The detection product contains detections only and retains exact DCA codes, labels, source observation and damage-area identifiers, IDS attribute ranges/codes, unioned overlap area, overlap fraction, coverage relationship, linkage method, and source snapshot. No agent grouping or outbreak construction is applied.

The linkage uses the IDS `damage_areas` polygons. Point detections represent small
clusters and are outside the current large-extent analysis scope.

The coverage product is built from the actual IDS `surveyed_areas` layer. Its `coverage_relationship` is one of:

```text
full_surveyed_area_intersection
partial_surveyed_area_intersection
no_surveyed_area_intersection
coverage_unknown
```

Partial coverage is not labeled fully surveyed. Absence from the damage layer is not used to infer survey coverage.

`00_prepare_mtbs_fire_perimeters.R` validates the downloaded national archive and writes the configured `mtbs_fire_perimeters.gpkg`. The current source is the June 21, 2026 USGS/USDA Forest Service `S_USA.MTBS_Burn_Area_Boundary` release, DOI `10.5066/P9IED7RZ`. Its SHA-256 is recorded in `config.yaml` and checked before conversion.

`02_extract_mtbs_fire_history.R` retains every MTBS event whose burned-area boundary touches an eligible 800 m FIA footprint. It preserves exact event and mapping IDs, ignition date, event year, fire name and type, MTBS burned-area acres, and overlap area/fraction. It does not select a primary fire, filter fire types, summarize temporal endpoints, or use severity rasters.

The current `fire_type` values are `Wildfire`, `Prescribed Fire`, `Wildland Fire Use`, and `Other`. They are retained so downstream analyses can explicitly include or exclude each type.

`fire_ignition_date` is the source `IG_DATE`. The current snapshot supplies a day for every event, so artificial lower/upper date bounds are not emitted. `search_buffer_overlap_fraction` is the share of the 800 m public-coordinate search circle covered by the MTBS burned-area polygon. It is not the share of an FIA condition, and it does not establish that the measured condition burned.

Constants shared by every row—including the 800 m method, intersection rule, source layer, publication date, DOI, checksum, and snapshot—are documented here, in `config.yaml`, and in `disturbance_source_manifest.csv`; they are not repeated in every evidence row.

`04_build_disturbance_readiness.R` writes one long metrics table, one source manifest, and a short Markdown report. Stage availability counts use the existing biological products filtered to `COND_STATUS_CD == 1`; they do not select a cohort or pair endpoints.

### Spatial product contracts

| Product | One row represents | Key | Source | Filtering applied |
| --- | --- | --- | --- | --- |
| `fia_visit_spatial_linkage_status.parquet` | one condition-backed FIA plot visit | `stable_plot_id + PLT_CN + INVYR` | `plot_condition_metadata.parquet` | no visits removed; stable-plot eligibility is labeled |
| `plot_footprints.gpkg` | one eligible stable plot's 800 m search buffer | `stable_plot_id` | distinct public FIA coordinates | only stable plots with exactly one usable six-decimal coordinate pair |
| `ids_annual_agent_evidence/` | one stable plot, IDS year, and exact detected DCA code | `stable_plot_id + survey_year + dca_code` | IDS `damage_areas` | 800 m footprint intersections; multi-coordinate FIA plots absent through the footprint input |
| `ids_annual_survey_coverage/` | one eligible stable plot and IDS coverage year | `stable_plot_id + survey_year` | IDS `surveyed_areas` | every eligible footprint retained, including zero-overlap rows |
| `mtbs_fire_event_evidence.parquet` | one stable plot and associated MTBS event | `stable_plot_id + mtbs_event_id` | cited national MTBS burned-area boundaries | any 800 m footprint contact; all MTBS fire types retained; overlap is of the search buffer, not an FIA condition |
| `disturbance_analysis_readiness.csv` | one readiness metric | `scope + metric` | products above plus existing biological products | biological availability counts use forest conditions; no analysis cohort selected |

## Visit history

Visit history remains neutral. See [INTERVAL_FOUNDATION.md](INTERVAL_FOUNDATION.md) for `PREV_PLT_CN` auditing, date bounds, and labeled fallback relationships. First/last endpoints and eligible analysis cohorts are intentionally deferred.

## Run order

```bash
Rscript 08_disturbance_linkage/scripts/01_build_fia_visit_spatial_linkage_status.R
Rscript 08_disturbance_linkage/scripts/00_prepare_mtbs_fire_perimeters.R
Rscript 08_disturbance_linkage/scripts/02_extract_mtbs_fire_history.R
Rscript 08_disturbance_linkage/scripts/03_extract_ids_annual_agent_history.R
Rscript 08_disturbance_linkage/scripts/04_build_disturbance_readiness.R
Rscript scripts/run_tests.R 05_fia 08_disturbance_linkage
```

The IDS script skips complete existing year partitions. If coverage was written
before an agent calculation failed, the restart reuses that coverage and builds
only the missing agent partition. Use `--year=YYYY` for a single-year run and
`--overwrite` for an intentional rebuild.

Surveyed-area coverage uses exact geometry. A buffer wholly covered by one
survey polygon is assigned its known full buffer area without constructing a
redundant clipped polygon; buffers crossing polygon boundaries still use exact
intersection and union. A 1999 regression comparison retained identical keys,
coverage classes, source identifiers, and nonnumeric fields; maximum numerical
differences were `7.1e-06` square metres of area and `3.6e-12` in overlap
fraction. IDS legacy count, TPA, and percent summaries are stored as doubles so
large grouped sums cannot change type mid-calculation.
