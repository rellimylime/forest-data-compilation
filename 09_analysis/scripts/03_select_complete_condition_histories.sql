-- Select complete stable-condition histories and their first/last CWM response.
--
-- PREV_PLT_CN defines visit order. The same numeric CONDID is then required at
-- every linked visit; CONDID never creates the visit link.

SET preserve_insertion_order = false;
SET threads = 4;

CREATE OR REPLACE TEMP VIEW eligible_edges AS
SELECT *
FROM read_parquet('09_analysis/data/processed/interval_agent_mortality.parquet')
WHERE analysis_ready;

CREATE OR REPLACE TEMP VIEW complete_histories AS
SELECT
  stable_plot_id,
  remeasurement_component_id,
  state,
  CONDID,
  n_visits_in_component AS n_visits,
  count(*) AS n_intervals,
  sum(interval_years) AS full_survey_period_years,
  min(T1_INVYR) AS first_inventory_year,
  max(T2_INVYR) AS last_inventory_year,
  max(CASE WHEN t2_visit_number = 2 THEN PREV_PLT_CN END) AS first_PLT_CN,
  max(CASE WHEN t2_visit_number = n_visits_in_component THEN T2_PLT_CN END)
    AS last_PLT_CN
FROM eligible_edges
GROUP BY
  stable_plot_id, remeasurement_component_id, state, CONDID,
  n_visits_in_component
HAVING min(t2_visit_number) = 2
   AND max(t2_visit_number) = n_visits_in_component
   AND count(*) = n_visits_in_component - 1
   AND count(DISTINCT t2_visit_number) = n_visits_in_component - 1;

CREATE OR REPLACE TEMP VIEW cwm_edges AS
SELECT
  e.stable_plot_id,
  e.remeasurement_component_id,
  e.state,
  e.CONDID,
  e.t2_visit_number,
  e.n_visits_in_component,
  c.layer,
  c.T1_temperature,
  c.T2_temperature,
  c.T1_precipitation,
  c.T2_precipitation,
  c.T1_CWD,
  c.T2_CWD
FROM eligible_edges AS e
INNER JOIN read_parquet(
  '09_analysis/data/processed/stable_condition_cwm_change.parquet'
) AS c USING (stable_condition_interval_key);

CREATE OR REPLACE TEMP VIEW first_last_cwm AS
SELECT
  stable_plot_id,
  remeasurement_component_id,
  state,
  CONDID,
  layer,
  max(CASE WHEN t2_visit_number = 2 THEN T1_temperature END)
    AS first_temperature,
  max(CASE WHEN t2_visit_number = n_visits_in_component THEN T2_temperature END)
    AS last_temperature,
  max(CASE WHEN t2_visit_number = 2 THEN T1_precipitation END)
    AS first_precipitation,
  max(CASE WHEN t2_visit_number = n_visits_in_component THEN T2_precipitation END)
    AS last_precipitation,
  max(CASE WHEN t2_visit_number = 2 THEN T1_CWD END) AS first_CWD,
  max(CASE WHEN t2_visit_number = n_visits_in_component THEN T2_CWD END)
    AS last_CWD,
  count(*) FILTER (WHERE t2_visit_number = 2) AS has_first_edge,
  count(*) FILTER (WHERE t2_visit_number = n_visits_in_component)
    AS has_last_edge
FROM cwm_edges
GROUP BY stable_plot_id, remeasurement_component_id, state, CONDID, layer
HAVING has_first_edge = 1 AND has_last_edge = 1;

COPY (
  SELECT
    h.stable_plot_id,
    h.remeasurement_component_id,
    h.state,
    h.CONDID,
    c.layer,
    h.first_PLT_CN,
    h.last_PLT_CN,
    h.first_inventory_year,
    h.last_inventory_year,
    h.n_visits,
    h.n_intervals,
    h.full_survey_period_years,
    c.first_temperature,
    c.last_temperature,
    c.last_temperature - c.first_temperature AS delta_temperature,
    c.first_precipitation,
    c.last_precipitation,
    c.last_precipitation - c.first_precipitation AS delta_precipitation,
    c.first_CWD,
    c.last_CWD,
    c.last_CWD - c.first_CWD AS delta_CWD
  FROM complete_histories AS h
  INNER JOIN first_last_cwm AS c
    USING (stable_plot_id, remeasurement_component_id, state, CONDID)
  ORDER BY state, stable_plot_id, remeasurement_component_id, CONDID, layer
) TO '09_analysis/data/intermediate/first_last_cwm_response.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

COPY (
  SELECT
    e.stable_condition_interval_key,
    e.stable_plot_id,
    e.remeasurement_component_id,
    e.state,
    e.CONDID,
    e.t1_visit_number,
    e.t2_visit_number,
    e.n_visits_in_component,
    e.PREV_PLT_CN,
    e.T2_PLT_CN,
    e.T1_CONDPROP_UNADJ,
    e.T1_MICRPROP_UNADJ,
    e.T1_SUBPPROP_UNADJ,
    e.T1_MACRPROP_UNADJ
  FROM eligible_edges AS e
  INNER JOIN complete_histories AS h
    USING (stable_plot_id, remeasurement_component_id, state, CONDID)
  ORDER BY state, stable_plot_id, remeasurement_component_id, CONDID,
           t1_visit_number
) TO '09_analysis/data/intermediate/complete_history_edges.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);
