-- Build a pooled forest-community CWM for each eligible condition visit by
-- pooling live seedlings, saplings, and trees with comparable FIA individual-
-- abundance expansions. This is the current preliminary response product.
-- life-stage-specific model figures are not retained.
--
-- Sampling-element condition adjustment:
--   seedlings and saplings -> TPA_UNADJ / MICRPROP_UNADJ
--   ordinary >=5-inch trees -> TPA_UNADJ / SUBPPROP_UNADJ
--   macroplot-signature tree groups -> TPA_UNADJ / MACRPROP_UNADJ
-- The generic CONDPROP_UNADJ is used only when the applicable element-specific
-- proportion is unavailable. Tree groups are already live-only and separated
-- from saplings by the FIA summary producer. Seedling rows are tallies, so their
-- expanded seedlings_tpa—not database-row count—is used.

SET preserve_insertion_order = false;
SET threads = 4;

CREATE OR REPLACE TEMP VIEW model_histories AS
SELECT
  history_id,
  stable_plot_id,
  remeasurement_component_id,
  state,
  CONDID,
  first_PLT_CN,
  last_PLT_CN,
  first_inventory_year,
  last_inventory_year,
  n_visits,
  n_intervals,
  full_survey_period_years,
  fire_cumulative_mortality_pct,
  insect_cumulative_mortality_pct,
  disease_cumulative_mortality_pct,
  first_measurement_date,
  last_measurement_date,
  cumulative_site_CWD_mm,
  cumulative_site_CWD_complete
FROM read_parquet(
  '09_analysis/data/processed/lifestage_model_data.parquet'
)
QUALIFY row_number() OVER (PARTITION BY history_id ORDER BY layer) = 1;

CREATE OR REPLACE TEMP VIEW history_edges AS
SELECT
  e.history_id,
  e.stable_condition_interval_key,
  e.t1_visit_number,
  e.t2_visit_number,
  e.n_visits_in_component,
  CAST(e.PREV_PLT_CN AS BIGINT) AS PREV_PLT_CN,
  CAST(e.T2_PLT_CN AS BIGINT) AS T2_PLT_CN,
  e.CONDID,
  m.T1_CONDPROP_UNADJ,
  m.T1_MICRPROP_UNADJ,
  m.T1_SUBPPROP_UNADJ,
  m.T1_MACRPROP_UNADJ,
  m.T2_CONDPROP_UNADJ,
  m.T2_MICRPROP_UNADJ,
  m.T2_SUBPPROP_UNADJ,
  m.T2_MACRPROP_UNADJ
FROM (
  SELECT
    remeasurement_component_id || '|' || CAST(CONDID AS VARCHAR) AS history_id,
    *
  FROM read_parquet(
    '09_analysis/data/intermediate/complete_history_edges.parquet'
  )
) AS e
INNER JOIN read_parquet(
  '09_analysis/data/processed/stable_condition_intervals.parquet'
) AS m USING (stable_condition_interval_key);

-- Both sides of every official edge are retained, then deduplicated. A visit
-- appearing on adjacent edges must carry the same proportions in both records.
CREATE OR REPLACE TEMP VIEW endpoint_prop_rows AS
SELECT
  history_id,
  PREV_PLT_CN AS PLT_CN,
  CONDID,
  T1_CONDPROP_UNADJ AS CONDPROP_UNADJ,
  T1_MICRPROP_UNADJ AS MICRPROP_UNADJ,
  T1_SUBPPROP_UNADJ AS SUBPPROP_UNADJ,
  T1_MACRPROP_UNADJ AS MACRPROP_UNADJ
FROM history_edges
UNION ALL
SELECT
  history_id,
  T2_PLT_CN AS PLT_CN,
  CONDID,
  T2_CONDPROP_UNADJ AS CONDPROP_UNADJ,
  T2_MICRPROP_UNADJ AS MICRPROP_UNADJ,
  T2_SUBPPROP_UNADJ AS SUBPPROP_UNADJ,
  T2_MACRPROP_UNADJ AS MACRPROP_UNADJ
FROM history_edges;

CREATE OR REPLACE TEMP VIEW endpoint_props AS
SELECT
  history_id,
  PLT_CN,
  CONDID,
  min(CONDPROP_UNADJ) AS CONDPROP_UNADJ,
  min(MICRPROP_UNADJ) AS MICRPROP_UNADJ,
  min(SUBPPROP_UNADJ) AS SUBPPROP_UNADJ,
  min(MACRPROP_UNADJ) AS MACRPROP_UNADJ,
  max(CONDPROP_UNADJ) - min(CONDPROP_UNADJ) AS CONDPROP_range,
  max(MICRPROP_UNADJ) - min(MICRPROP_UNADJ) AS MICRPROP_range,
  max(SUBPPROP_UNADJ) - min(SUBPPROP_UNADJ) AS SUBPPROP_range,
  max(MACRPROP_UNADJ) - min(MACRPROP_UNADJ) AS MACRPROP_range
FROM endpoint_prop_rows
GROUP BY history_id, PLT_CN, CONDID;

CREATE OR REPLACE TEMP VIEW tree_groups AS
SELECT
  p.history_id,
  t.PLT_CN,
  t.INVYR,
  t.CONDID,
  t.SPCD,
  'trees' AS life_stage,
  CAST(t.n_trees_tpa AS DOUBLE) AS abundance_unadjusted,
  CASE
    WHEN t.n_trees_raw > 0
      AND abs(t.n_trees_tpa / t.n_trees_raw - 0.999188) < 0.02
      THEN 'macroplot'
    ELSE 'subplot'
  END AS sampling_element,
  CASE
    WHEN t.n_trees_raw > 0
      AND abs(t.n_trees_tpa / t.n_trees_raw - 0.999188) < 0.02
      THEN t.n_trees_tpa /
        coalesce(nullif(p.MACRPROP_UNADJ, 0), p.CONDPROP_UNADJ)
    ELSE t.n_trees_tpa /
      coalesce(nullif(p.SUBPPROP_UNADJ, 0), p.CONDPROP_UNADJ)
  END AS abundance_adjusted,
  CASE
    WHEN t.n_trees_raw > 0
      AND abs(t.n_trees_tpa / t.n_trees_raw - 0.999188) < 0.02
      THEN p.MACRPROP_UNADJ IS NULL OR p.MACRPROP_UNADJ <= 0
    ELSE p.SUBPPROP_UNADJ IS NULL OR p.SUBPPROP_UNADJ <= 0
  END AS used_generic_prop
FROM read_parquet(
  '05_fia/data/processed/summaries/plot_tree_species.parquet'
) AS t
INNER JOIN endpoint_props AS p USING (PLT_CN, CONDID)
WHERE t.n_trees_tpa > 0;

CREATE OR REPLACE TEMP VIEW sapling_groups AS
SELECT
  p.history_id,
  s.PLT_CN,
  s.INVYR,
  s.CONDID,
  s.SPCD,
  'saplings' AS life_stage,
  CAST(s.n_trees_tpa AS DOUBLE) AS abundance_unadjusted,
  'microplot' AS sampling_element,
  s.n_trees_tpa /
    coalesce(nullif(p.MICRPROP_UNADJ, 0), p.CONDPROP_UNADJ)
      AS abundance_adjusted,
  p.MICRPROP_UNADJ IS NULL OR p.MICRPROP_UNADJ <= 0 AS used_generic_prop
FROM read_parquet(
  '05_fia/data/processed/summaries/plot_sapling_species.parquet'
) AS s
INNER JOIN endpoint_props AS p USING (PLT_CN, CONDID)
WHERE s.n_trees_tpa > 0;

CREATE OR REPLACE TEMP VIEW seedling_groups AS
SELECT
  p.history_id,
  s.PLT_CN,
  s.INVYR,
  s.CONDID,
  s.SPCD,
  'seedlings' AS life_stage,
  CAST(s.seedlings_tpa AS DOUBLE) AS abundance_unadjusted,
  'microplot' AS sampling_element,
  s.seedlings_tpa /
    coalesce(nullif(p.MICRPROP_UNADJ, 0), p.CONDPROP_UNADJ)
      AS abundance_adjusted,
  p.MICRPROP_UNADJ IS NULL OR p.MICRPROP_UNADJ <= 0 AS used_generic_prop
FROM read_parquet(
  '05_fia/data/processed/summaries/plot_seedling_species.parquet'
) AS s
INNER JOIN endpoint_props AS p USING (PLT_CN, CONDID)
WHERE s.seedlings_tpa > 0;

CREATE OR REPLACE TEMP VIEW community_rows AS
SELECT * FROM tree_groups
UNION ALL BY NAME
SELECT * FROM sapling_groups
UNION ALL BY NAME
SELECT * FROM seedling_groups;

CREATE OR REPLACE TEMP VIEW community_species AS
SELECT
  history_id,
  PLT_CN,
  any_value(INVYR) AS INVYR,
  CONDID,
  SPCD,
  sum(abundance_unadjusted) AS abundance_unadjusted,
  sum(abundance_adjusted) AS abundance_adjusted,
  string_agg(DISTINCT life_stage, ',' ORDER BY life_stage) AS life_stages,
  bool_or(used_generic_prop) AS any_generic_prop
FROM community_rows
GROUP BY history_id, PLT_CN, CONDID, SPCD;

CREATE OR REPLACE TEMP VIEW niches AS
SELECT
  species_key,
  tmean_annual_mean,
  pr_annual_sum,
  cwd_annual_sum,
  niche_scope_used
FROM (
  SELECT
    species_key,
    tmean_annual_mean,
    pr_annual_sum,
    cwd_annual_sum,
    'us_study_area' AS niche_scope_used,
    1 AS priority
  FROM read_parquet(
    '06_species_niches/data/processed/species_climate_niches_us_study_area.parquet'
  )
  UNION ALL
  SELECT
    species_key,
    tmean_annual_mean,
    pr_annual_sum,
    cwd_annual_sum,
    'global_fallback' AS niche_scope_used,
    2 AS priority
  FROM read_parquet(
    '06_species_niches/data/processed/species_climate_niches.parquet'
  )
)
QUALIFY row_number() OVER (PARTITION BY species_key ORDER BY priority) = 1;

CREATE OR REPLACE TEMP VIEW community_joined AS
SELECT
  s.*,
  n.tmean_annual_mean,
  n.pr_annual_sum,
  n.cwd_annual_sum,
  n.niche_scope_used
FROM community_species AS s
LEFT JOIN niches AS n
  ON n.species_key = 'fia_spcd:' || CAST(s.SPCD AS VARCHAR);

CREATE OR REPLACE TEMP VIEW stage_totals AS
SELECT
  history_id,
  PLT_CN,
  CONDID,
  sum(abundance_adjusted) FILTER (WHERE life_stage = 'seedlings')
    AS seedling_abundance,
  sum(abundance_adjusted) FILTER (WHERE life_stage = 'saplings')
    AS sapling_abundance,
  sum(abundance_adjusted) FILTER (WHERE life_stage = 'trees')
    AS tree_abundance,
  count(DISTINCT life_stage) AS n_life_stages_present,
  count(*) FILTER (WHERE sampling_element = 'macroplot') AS macroplot_groups,
  count(*) FILTER (WHERE used_generic_prop) AS generic_prop_groups
FROM community_rows
GROUP BY history_id, PLT_CN, CONDID;

CREATE OR REPLACE TEMP VIEW community_cwm AS
SELECT
  j.history_id,
  j.PLT_CN,
  any_value(j.INVYR) AS INVYR,
  j.CONDID,
  sum(j.abundance_adjusted) AS total_individual_abundance,
  sum(j.abundance_adjusted) FILTER (WHERE j.tmean_annual_mean IS NOT NULL)
    AS abundance_with_temperature_niche,
  sum(j.abundance_adjusted) FILTER (WHERE j.pr_annual_sum IS NOT NULL)
    AS abundance_with_precipitation_niche,
  sum(j.abundance_adjusted) FILTER (WHERE j.cwd_annual_sum IS NOT NULL)
    AS abundance_with_CWD_niche,
  sum(j.tmean_annual_mean * j.abundance_adjusted)
    FILTER (WHERE j.tmean_annual_mean IS NOT NULL) /
    nullif(sum(j.abundance_adjusted)
      FILTER (WHERE j.tmean_annual_mean IS NOT NULL), 0)
      AS temperature,
  sum(j.pr_annual_sum * j.abundance_adjusted)
    FILTER (WHERE j.pr_annual_sum IS NOT NULL) /
    nullif(sum(j.abundance_adjusted)
      FILTER (WHERE j.pr_annual_sum IS NOT NULL), 0)
      AS precipitation,
  sum(j.cwd_annual_sum * j.abundance_adjusted)
    FILTER (WHERE j.cwd_annual_sum IS NOT NULL) /
    nullif(sum(j.abundance_adjusted)
      FILTER (WHERE j.cwd_annual_sum IS NOT NULL), 0)
      AS CWD,
  count(*) AS n_species,
  count(*) FILTER (WHERE j.tmean_annual_mean IS NOT NULL)
    AS n_species_with_niche,
  count(*) FILTER (WHERE j.niche_scope_used = 'global_fallback')
    AS n_species_global_fallback,
  bool_or(j.any_generic_prop) AS any_generic_prop
FROM community_joined AS j
GROUP BY j.history_id, j.PLT_CN, j.CONDID;

CREATE OR REPLACE TEMP VIEW community_condition_visits AS
SELECT
  c.*,
  coalesce(s.seedling_abundance, 0) AS seedling_abundance,
  coalesce(s.sapling_abundance, 0) AS sapling_abundance,
  coalesce(s.tree_abundance, 0) AS tree_abundance,
  coalesce(s.seedling_abundance, 0) / nullif(c.total_individual_abundance, 0)
    AS seedling_abundance_share,
  coalesce(s.sapling_abundance, 0) / nullif(c.total_individual_abundance, 0)
    AS sapling_abundance_share,
  coalesce(s.tree_abundance, 0) / nullif(c.total_individual_abundance, 0)
    AS tree_abundance_share,
  s.n_life_stages_present,
  s.macroplot_groups,
  s.generic_prop_groups,
  c.abundance_with_temperature_niche / nullif(c.total_individual_abundance, 0)
    AS temperature_niche_weight_coverage,
  c.abundance_with_precipitation_niche / nullif(c.total_individual_abundance, 0)
    AS precipitation_niche_weight_coverage,
  c.abundance_with_CWD_niche / nullif(c.total_individual_abundance, 0)
    AS CWD_niche_weight_coverage
FROM community_cwm AS c
INNER JOIN stage_totals AS s USING (history_id, PLT_CN, CONDID);

COPY (
  SELECT *
  FROM community_condition_visits
  ORDER BY history_id, PLT_CN, CONDID
) TO '09_analysis/data/processed/pooled_condition_visit_cwm.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

CREATE OR REPLACE TEMP VIEW community_history_response AS
SELECT
  h.*,
  f.temperature AS first_temperature,
  l.temperature AS last_temperature,
  l.temperature - f.temperature AS delta_temperature,
  f.precipitation AS first_precipitation,
  l.precipitation AS last_precipitation,
  l.precipitation - f.precipitation AS delta_precipitation,
  f.CWD AS first_CWD,
  l.CWD AS last_CWD,
  l.CWD - f.CWD AS delta_CWD,
  f.total_individual_abundance AS first_total_individual_abundance,
  l.total_individual_abundance AS last_total_individual_abundance,
  f.seedling_abundance_share AS first_seedling_abundance_share,
  l.seedling_abundance_share AS last_seedling_abundance_share,
  f.sapling_abundance_share AS first_sapling_abundance_share,
  l.sapling_abundance_share AS last_sapling_abundance_share,
  f.tree_abundance_share AS first_tree_abundance_share,
  l.tree_abundance_share AS last_tree_abundance_share,
  f.n_life_stages_present AS first_life_stages_present,
  l.n_life_stages_present AS last_life_stages_present,
  f.temperature_niche_weight_coverage AS first_temperature_niche_coverage,
  l.temperature_niche_weight_coverage AS last_temperature_niche_coverage,
  f.precipitation_niche_weight_coverage AS first_precipitation_niche_coverage,
  l.precipitation_niche_weight_coverage AS last_precipitation_niche_coverage,
  f.CWD_niche_weight_coverage AS first_CWD_niche_coverage,
  l.CWD_niche_weight_coverage AS last_CWD_niche_coverage
FROM model_histories AS h
INNER JOIN community_condition_visits AS f
  ON f.history_id = h.history_id
 AND f.PLT_CN = h.first_PLT_CN
 AND f.CONDID = h.CONDID
INNER JOIN community_condition_visits AS l
  ON l.history_id = h.history_id
 AND l.PLT_CN = h.last_PLT_CN
 AND l.CONDID = h.CONDID;

COPY (
  SELECT *
  FROM community_history_response
  ORDER BY state, stable_plot_id, remeasurement_component_id, CONDID
) TO '09_analysis/data/processed/pooled_model_data.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

COPY (
  SELECT
    (SELECT count(*) FROM model_histories) AS candidate_histories,
    count(*) AS pooled_community_histories,
    count(*) FILTER (WHERE delta_temperature IS NOT NULL)
      AS usable_temperature_histories,
    count(*) FILTER (WHERE delta_precipitation IS NOT NULL)
      AS usable_precipitation_histories,
    count(*) FILTER (WHERE delta_CWD IS NOT NULL) AS usable_CWD_histories,
    count(*) FILTER (WHERE cumulative_site_CWD_complete)
      AS histories_with_complete_site_CWD,
    count(*) FILTER (
      WHERE first_life_stages_present = 3 AND last_life_stages_present = 3
    ) AS histories_all_three_stages_at_both_endpoints,
    avg(first_seedling_abundance_share) AS mean_first_seedling_share,
    median(first_seedling_abundance_share) AS median_first_seedling_share,
    avg(last_seedling_abundance_share) AS mean_last_seedling_share,
    median(last_seedling_abundance_share) AS median_last_seedling_share,
    min(first_temperature_niche_coverage) AS min_first_niche_weight_coverage,
    median(first_temperature_niche_coverage) AS median_first_niche_weight_coverage,
    min(last_temperature_niche_coverage) AS min_last_niche_weight_coverage,
    median(last_temperature_niche_coverage) AS median_last_niche_weight_coverage
  FROM community_history_response
) TO '09_analysis/qa/outputs/07_pooled_community_cwm/pooled_cwm_coverage.csv'
  (HEADER, DELIMITER ',');

COPY (
  SELECT
    life_stage,
    sampling_element,
    count(*) AS source_groups,
    count(*) FILTER (WHERE used_generic_prop) AS generic_prop_groups,
    sum(abundance_unadjusted) AS total_unadjusted_abundance,
    sum(abundance_adjusted) AS total_adjusted_abundance
  FROM community_rows
  GROUP BY life_stage, sampling_element
  ORDER BY life_stage, sampling_element
) TO '09_analysis/qa/outputs/07_pooled_community_cwm/pooled_cwm_sampling.csv'
  (HEADER, DELIMITER ',');

COPY (
  SELECT
    CASE
      WHEN greatest(
        coalesce(CONDPROP_range, 0), coalesce(MICRPROP_range, 0),
        coalesce(SUBPPROP_range, 0), coalesce(MACRPROP_range, 0)
      ) > 1e-10 THEN 'inconsistent_across_adjacent_edges'
      ELSE 'consistent'
    END AS endpoint_prop_status,
    count(*) AS condition_visits
  FROM endpoint_props
  GROUP BY endpoint_prop_status
) TO '09_analysis/qa/outputs/07_pooled_community_cwm/pooled_cwm_endpoint_props.csv'
  (HEADER, DELIMITER ',');
