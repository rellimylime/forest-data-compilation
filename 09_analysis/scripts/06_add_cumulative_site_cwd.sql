-- Sum TerraClimate monthly climatic water deficit (def) over each existing
-- first-to-last condition history. The TerraClimate timestamp is the first day
-- of the represented month, so the inclusion rule is:
--   first_measurement_date <= month_start <= last_measurement_date.
-- Mortality, history, and CWM response fields are passed through unchanged.

SET preserve_insertion_order = false;
SET threads = 4;

CREATE OR REPLACE TEMP VIEW history_dates AS
SELECT
  *,
  CASE
    WHEN first_measurement_date = date_trunc('month', first_measurement_date)
      THEN CAST(date_trunc('month', first_measurement_date) AS DATE)
    ELSE CAST(date_trunc('month', first_measurement_date) + INTERVAL 1 MONTH AS DATE)
  END AS first_included_month,
  CAST(date_trunc('month', last_measurement_date) AS DATE) AS last_included_month
FROM read_parquet(
  '09_analysis/data/intermediate/history_measurement_dates.parquet'
);

CREATE OR REPLACE TEMP VIEW monthly_def AS
SELECT
  CAST(site_id AS VARCHAR) AS stable_plot_id,
  make_date(CAST(year AS INTEGER), CAST(month AS INTEGER), 1) AS climate_month,
  CAST(value AS DOUBLE) AS site_CWD_mm
FROM read_parquet(
  '09_analysis/data/cache/terraclimate_site_cwd/site_climate.parquet'
)
WHERE variable = 'def';

CREATE OR REPLACE TEMP VIEW history_cwd_sums AS
SELECT
  h.history_id,
  COUNT(c.climate_month) AS observed_CWD_months,
  SUM(c.site_CWD_mm) AS cumulative_site_CWD_mm,
  AVG(c.site_CWD_mm) AS mean_monthly_site_CWD_mm,
  MIN(c.site_CWD_mm) AS min_monthly_site_CWD_mm,
  MAX(c.site_CWD_mm) AS max_monthly_site_CWD_mm
FROM history_dates AS h
LEFT JOIN monthly_def AS c
  ON c.stable_plot_id = h.stable_plot_id
 AND c.climate_month >= h.first_included_month
 AND c.climate_month <= h.last_included_month
GROUP BY h.history_id;

CREATE OR REPLACE TEMP VIEW history_cwd AS
SELECT
  h.*,
  date_diff('month', h.first_included_month, h.last_included_month) + 1
    AS expected_CWD_months,
  s.observed_CWD_months,
  s.cumulative_site_CWD_mm,
  s.mean_monthly_site_CWD_mm,
  s.min_monthly_site_CWD_mm,
  s.max_monthly_site_CWD_mm,
  s.observed_CWD_months =
    date_diff('month', h.first_included_month, h.last_included_month) + 1
      AS cumulative_site_CWD_complete,
  CASE
    WHEN s.observed_CWD_months = 0 THEN 'no_site_climate'
    WHEN s.observed_CWD_months <
      date_diff('month', h.first_included_month, h.last_included_month) + 1
      THEN 'incomplete_months'
    ELSE 'complete'
  END AS cumulative_site_CWD_status
FROM history_dates AS h
INNER JOIN history_cwd_sums AS s USING (history_id);

COPY (
  SELECT *
  FROM history_cwd
  ORDER BY state, stable_plot_id, remeasurement_component_id, CONDID
) TO '09_analysis/data/processed/history_site_cwd.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

COPY (
  SELECT
    m.history_id,
    m.stable_plot_id,
    m.remeasurement_component_id,
    m.state,
    m.CONDID,
    m.layer,
    m.first_PLT_CN,
    m.last_PLT_CN,
    m.first_inventory_year,
    m.last_inventory_year,
    m.n_visits,
    m.n_intervals,
    m.full_survey_period_years,
    m.delta_temperature,
    m.delta_precipitation,
    m.delta_CWD,
    m.fire_cumulative_mortality_pct,
    m.insect_cumulative_mortality_pct,
    m.disease_cumulative_mortality_pct,
    c.first_measurement_date,
    c.last_measurement_date,
    c.cumulative_site_CWD_mm,
    c.cumulative_site_CWD_complete
  FROM read_parquet(
    '09_analysis/data/processed/lifestage_model_base.parquet'
  ) AS m
  LEFT JOIN history_cwd AS c USING (history_id)
  ORDER BY m.state, m.stable_plot_id, m.remeasurement_component_id,
           m.CONDID, m.layer
) TO '09_analysis/data/processed/lifestage_model_data.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

COPY (
  WITH duplicate_months AS (
    SELECT stable_plot_id, climate_month, COUNT(*) AS n
    FROM monthly_def
    GROUP BY ALL
    HAVING COUNT(*) > 1
  )
  SELECT
    COUNT(*) AS site_month_rows,
    COUNT(DISTINCT stable_plot_id) AS sites_with_CWD,
    MIN(climate_month) AS first_CWD_month,
    MAX(climate_month) AS last_CWD_month,
    MIN(site_CWD_mm) AS min_monthly_CWD_mm,
    MAX(site_CWD_mm) AS max_monthly_CWD_mm,
    (SELECT COUNT(*) FROM duplicate_months) AS duplicate_site_month_keys,
    'TerraClimate def' AS source_variable,
    'mm per month' AS source_units,
    0.1 AS netcdf_scale_factor,
    'month timestamp (first day) within exact FIA measurement dates'
      AS history_inclusion_rule
  FROM monthly_def
) TO '09_analysis/qa/predictors/site_cwd_source.csv'
  (HEADER, DELIMITER ',');
