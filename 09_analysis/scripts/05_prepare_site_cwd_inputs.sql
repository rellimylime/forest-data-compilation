-- Prepare the FIA history dates and site locations needed for TerraClimate CWD.
-- Run the extraction command documented in 09_analysis/README.md after this.

SET preserve_insertion_order = false;
SET threads = 4;

CREATE OR REPLACE TEMP VIEW model_histories AS
SELECT DISTINCT
  history_id,
  stable_plot_id,
  remeasurement_component_id,
  state,
  CONDID,
  CAST(first_PLT_CN AS BIGINT) AS first_PLT_CN,
  CAST(last_PLT_CN AS BIGINT) AS last_PLT_CN
FROM read_parquet(
  '09_analysis/data/processed/lifestage_model_base.parquet'
);

CREATE OR REPLACE TEMP VIEW component_visits AS
SELECT
  PLT_CN,
  measurement_date_lower,
  measurement_date_upper,
  measurement_date_lower
    + CAST(FLOOR(date_diff('day', measurement_date_lower,
                          measurement_date_upper) / 2.0) AS INTEGER)
      AS measurement_date_midpoint
FROM read_parquet(
  '09_analysis/data/processed/fia_remeasurement_components.parquet'
);

CREATE OR REPLACE TEMP VIEW history_dates AS
SELECT
  h.*,
  f.measurement_date_lower AS first_measurement_date_lower,
  f.measurement_date_upper AS first_measurement_date_upper,
  f.measurement_date_midpoint AS first_measurement_date,
  l.measurement_date_lower AS last_measurement_date_lower,
  l.measurement_date_upper AS last_measurement_date_upper,
  l.measurement_date_midpoint AS last_measurement_date,
  CASE
    WHEN f.measurement_date_lower = f.measurement_date_upper
      AND l.measurement_date_lower = l.measurement_date_upper
    THEN 'exact_day_both_endpoints'
    ELSE 'measurement_date_midpoint'
  END AS history_date_source
FROM model_histories AS h
LEFT JOIN component_visits AS f ON h.first_PLT_CN = f.PLT_CN
LEFT JOIN component_visits AS l ON h.last_PLT_CN = l.PLT_CN;

COPY (
  SELECT *
  FROM history_dates
  ORDER BY state, stable_plot_id, remeasurement_component_id, CONDID
) TO '09_analysis/data/intermediate/history_measurement_dates.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE true);

COPY (
  SELECT
    CAST(s.site_id AS VARCHAR) AS site_id,
    s.latitude,
    s.longitude,
    s.source
  FROM read_csv_auto(
    '05_fia/data/processed/site_climate/all_site_locations.csv',
    all_varchar = true
  ) AS s
  INNER JOIN (SELECT DISTINCT stable_plot_id FROM model_histories) AS h
    ON CAST(s.site_id AS VARCHAR) = h.stable_plot_id
  ORDER BY site_id
) TO '09_analysis/data/intermediate/model_site_locations.csv'
  (HEADER, DELIMITER ',');
