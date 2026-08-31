# 08 Disturbance Linkage

This module prepares disturbance evidence without choosing an analysis model. FIA condition disturbance, FIA tree damage agents, MTBS fire events, and IDS aerial detections remain separate because they measure different things.

## Current preparation products

| Product | Grain | Purpose |
| --- | --- | --- |
| `fia_forest_disturbance_measures.parquet` | forested plot visit | FIA fire, insect, and disease condition extent |
| `fia_fire_disturbance_slot_evidence.parquet` | forest condition x fire slot | Auditable fire code-year pairs |
| `fia_tree_damage_agent_evidence/` | tree record x exact FIA agent | Finest retained tree-agent evidence |
| `fia_condition_damage_denominators/` | FIA condition | Record, TPA, and basal-area denominators |
| `fia_condition_damage_agent_candidates/` | FIA condition x exact FIA agent | All-agent candidate fractions |
| `fia_insect_severity_candidates/` | FIA condition x exact insect agent | Officially insect-filtered candidate fractions |
| `fia_visit_pairing_audit.parquet` | current FIA visit | Neutral predecessor-link audit |
| `fia_survey_intervals.parquet` | proposed visit relationship | Labeled interval evidence |
| `fia_visit_spatial_linkage_status.parquet` | stable plot x FIA visit | Coordinate provenance and stable-plot linkage eligibility |
| `ids_annual_agent_evidence/` | stable plot x IDS year x exact DCA code | Detected IDS agents; partitioned by year |
| `ids_annual_survey_coverage/` | stable plot x IDS year | Full, partial, absent, or unknown surveyed-area overlap; partitioned by year |
| `disturbance_analysis_readiness.csv` | readiness metric | Availability and overlap counts, not an analysis cohort |
| `disturbance_source_manifest.csv` | source snapshot | Paths, sizes, modification dates, and feasible checksums |

The damage-agent products include all FIA condition classes. Forest-specific work must filter `COND_STATUS_CD == 1`. No record-, TPA-, or basal-area fraction is primary, and no condition-area or plot-level weighting is applied.

The exact-code lookup supplies definitions from the current FIADB v9.4 Appendix H only. It does not assert that meanings were identical across earlier manuals, years, or regional implementations. Evidence rows retain `MANUAL`, the Appendix H `region` field, `definition_applicability_status`, and `region_applicability_status` so those cases are not mislabeled as resolved. The intended analysis does not require FIA to supply species-level identity: reviewed broad FIA groups supply tree-level impact ingredients, while exact agent identity and dates come from IDS. The official [FIA v9.4 field guide](https://research.fs.usda.gov/sites/default/files/2025-04/pnw-2025_v9-4_palau_fia_field_manual.pdf) states that specific regional damage-agent codes can be collapsed into national general categories. Inferring a bark-beetle species from tree host may be considered later, but is not part of preparation.

The canonical fire product pairs `DSTRBCD1` only with `DSTRBYR1`, and likewise for slots 2 and 3. Nonfire slots are excluded from fire evidence. Unknown or continuous years remain explicit rather than being treated as literal dates.

During correction of the development output, QA found:

- identical 494,775 visit keys and identical fire-extent values;
- 660 visits where a nonfire year was removed from fire timing;
- 506 visits where a same-slot fire year was restored because `MEASYEAR`, rather than the less precise `INVYR`, established that it was not post-measurement.

Those 506 visits contain 570 fire-coded condition-slot records. All 570 disturbance years are on or before `MEASYEAR`; none is future relative to the measurement year. Review the row-level comparison in `qa/outputs/fia_fire_years_restored_by_measurement_timing.csv`.

National condition-class QA is in `qa/outputs/fia_damage_agent_condition_status_counts.csv`. The denominator product contains all 1,476,881 conditions in the current foundation: 593,380 forest, 766,268 nonforest, 10,205 noncensus water, 47,252 census water, and 59,776 nonsampled/possible-forest conditions.

An August 2026 audit found that the local Florida, Kentucky, and Texas TREE files were newer than their PLOT and COND files. This created 6,640 agent-evidence rows from 921 conditions without local condition metadata. All 921 keys existed in the live DataMart. All eight configured tables for each affected state were then refreshed together, recorded in `05_fia/data/raw/download_manifest.csv`, and the dependent FIA products were rebuilt. Current QA reports zero unmatched tree-agent pairs. The mix arose because smaller PLOT, COND, and SEEDLING CSVs had been committed to Git in February, while git-ignored TREE and TREE_GRM files were downloaded later from the live DataMart. The downloader then equated an existing file with a current file and recorded no acquisition manifest. It now stages a complete state and supports explicit `--refresh`.

Known-wrong, unused development outputs are removed rather than retained beside their corrections. Canonical preparation filenames are stable and unversioned; Git records code history. Versioned filenames remain only for deliberately experimental analysis products that may need to coexist for method comparison.

For exact eligibility rules, raw FIA column provenance, TPA meaning, the basal-area formula, and forest condition weights, see the [FIA disturbance data dictionary](FIA_DISTURBANCE_DATA_DICTIONARY.md).

## Coordinate decision

The foundational FIA records retain every reported public coordinate. For the main spatial analysis, the agreed rule is to exclude the entire `stable_plot_id`, including all visits, when it has more than one distinct public coordinate pair. No coordinate is selected randomly and the latest coordinate does not silently replace earlier coordinates.

`fia_visit_spatial_linkage_status.parquet` repeats the stable-plot decision on every foundational FIA visit while retaining the visit's reported `LAT` and `LON`. The physical FIA coordinates are stored at six decimal places, so distinct pairs are defined by exact inequality after formatting both values to six decimals. The 800 m distance is only the external-source search buffer; it is not a coordinate-equality tolerance.

`plot_footprints.gpkg` contains exactly the 405,510 eligible single-coordinate stable plots. It was rebuilt after the FIA refresh because one Florida plot's corrected public coordinate moved its buffer center by 30.3 km; the other 41,406 eligible plots in the three refreshed states matched their existing centers within 1 cm. Public coordinates are privacy-adjusted; an 800 m buffer is a search area, not the plot's true boundary.

## External evidence status

- IDS detections and survey coverage are separate products. A missing DCA code is never used as a nondetection row. Coverage comes from the actual `surveyed_areas` source layer, and partial buffer overlap remains labeled partial.
- IDS output is resumable by survey year. Existing complete years are retained;
  if a year has coverage but no agent file after an interruption, only its agent
  calculation is resumed. `--overwrite` intentionally rebuilds both partitions.
- MTBS event evidence uses the cited June 21, 2026 national burned-area boundary release. The source archive, checksum, prepared GeoPackage, and plot-event evidence are present.
- MTBS evidence has one `fire_ignition_date`; the source has exact days for every event. `search_buffer_overlap_fraction` refers only to the 800 m search circle, never to an FIA condition.
- Older MTBS severity-raster and integrated modeling summaries are not part of the current preparation scope.

## Run the preparation and readiness round

```bash
Rscript 08_disturbance_linkage/scripts/fia/01_build_survey_intervals.R
Rscript 08_disturbance_linkage/scripts/fia/02_build_forest_disturbance_measures.R
Rscript 08_disturbance_linkage/scripts/fia/03_prepare_damage_agent_evidence.R
Rscript 08_disturbance_linkage/scripts/spatial/01_build_plot_footprints.R
Rscript 08_disturbance_linkage/scripts/spatial/02_build_visit_linkage_status.R
Rscript 08_disturbance_linkage/scripts/mtbs/01_prepare_fire_perimeters.R
Rscript 08_disturbance_linkage/scripts/mtbs/02_extract_fire_history.R
Rscript 08_disturbance_linkage/scripts/ids/01_extract_annual_agent_history.R
Rscript 08_disturbance_linkage/scripts/integration/01_build_readiness_summary.R
Rscript 08_disturbance_linkage/qa/scripts/fia/01_validate_survey_intervals.R
Rscript 08_disturbance_linkage/qa/scripts/fia/03_validate_damage_agent_preparation.R
Rscript scripts/run_tests.R 08_disturbance_linkage
```

For one IDS year, use `--year=2020`. Add `--overwrite` only when intentionally rebuilding an existing partition.

The folders under `scripts/` are independent product families, not alternative
versions of one analysis. Numbering restarts inside each family. The commands
above give the supported complete preparation order; none of these external
evidence products is required by the current `09_analysis` mortality models.

The MTBS preparation step validates the downloaded archive checksum and source schema before replacing the canonical GeoPackage.

See [WORKFLOW.md](WORKFLOW.md) for exact product contracts and [INTERVAL_FOUNDATION.md](INTERVAL_FOUNDATION.md) for visit-link QA.
