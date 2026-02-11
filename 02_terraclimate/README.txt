================================================================================
TerraClimate Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://www.climatologylab.org/terraclimate.html
Description: High-resolution (~4km) global climate and water balance dataset
derived from WorldClim, CRU, and JRA-55 reanalysis. Monthly data from 1958 to
present. Pixel-level values extracted at IDS observation locations via GEE.

Citation: Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018).
TerraClimate, a high-resolution global dataset of monthly climate and climatic
water balance from 1958-2015. Scientific Data 5:170191.

GEE Asset: IDAHO_EPSCOR/TERRACLIMATE
Native resolution: ~4km (1/24th degree, approximately 4638m at equator)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1958-present (IDS extraction: 1997-2024)

================================================================================
RUNNING THE WORKFLOW
================================================================================
Run scripts in order. Steps 1-2 are TerraClimate-specific (in this directory).
Steps 3-4 use shared scripts (in the top-level scripts/ directory) that work
identically for all climate datasets (PRISM, WorldClim, ERA5).

  Step 1: Rscript 02_terraclimate/scripts/01_build_pixel_maps.R
          Maps each IDS observation to the ~4km TerraClimate pixels it overlaps.

  Step 2: Rscript 02_terraclimate/scripts/02_extract_terraclimate.R
          Extracts monthly climate values for all unique pixels from GEE.

  Step 3: Rscript scripts/03_reshape_pixel_values.R terraclimate
          Reshapes wide-format yearly files into a single long-format parquet.

  Step 4: Rscript scripts/04_build_climate_summaries.R terraclimate
          Computes area-weighted climate means per observation per month.

Prerequisite: 00_explore_terraclimate.R is an optional exploratory script.

================================================================================
USING THE OUTPUTS
================================================================================
The workflow produces two key output types:

  1. PIXEL MAPS  -- link observations to raster pixels
     Location: 02_terraclimate/data/processed/pixel_maps/
     Files: damage_areas_pixel_map.parquet
            damage_points_pixel_map.parquet
            surveyed_areas_pixel_map.parquet
     Columns: OBSERVATION_ID, DAMAGE_AREA_ID, pixel_id, x, y, coverage_fraction

  2. PIXEL VALUES -- monthly climate data per unique pixel
     Location: 02_terraclimate/data/processed/pixel_values/
     Files: terraclimate_{year}.parquet (one per year, 1997-2024)
     Columns: pixel_id, x, y, year, month, [14 climate variables]

After reshape (step 3):
     Location: processed/climate/terraclimate/pixel_values.parquet
     Columns: pixel_id, calendar_year, calendar_month, water_year,
              water_year_month, variable, value

After summaries (step 4):
     Location: processed/climate/terraclimate/damage_areas_summaries_long.parquet
     Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year,
              water_year_month, variable, weighted_mean, n_pixels,
              n_pixels_with_data, sum_coverage_fraction

================================================================================
HOW TO: Get Climate Data for a Specific Species
================================================================================
Goal: Select IDS observations for a particular host species and retrieve the
TerraClimate values at the corresponding pixels.

  library(sf)
  library(dplyr)
  library(arrow)

  # --- 1. Load IDS data and species lookup ---
  damage_areas <- st_read(
    "01_ids/data/processed/ids_layers_cleaned.gpkg",
    layer = "damage_areas"
  )
  species_lookup <- read.csv("01_ids/lookups/host_code_lookup.csv")

  # --- 2. Find the HOST_CODE for your species ---
  # Browse available species:
  print(species_lookup)
  # Example codes: 122 = ponderosa pine, 202 = Douglas-fir,
  #                746 = quaking aspen, 108 = lodgepole pine

  # --- 3. Filter IDS observations ---
  my_obs <- damage_areas %>%
    filter(HOST_CODE == 122)               # ponderosa pine
  # Add more filters if needed:
  #   filter(HOST_CODE == 122,
  #          SURVEY_YEAR >= 2010,
  #          REGION_ID == 1)               # Northern Region only

  # --- 4. Get the TerraClimate pixels for those observations ---
  pixel_map <- read_parquet(
    "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet"
  )
  my_pixels <- pixel_map %>%
    filter(OBSERVATION_ID %in% my_obs$OBSERVATION_ID)

  # --- 5. Load climate data and join ---
  # OPTION A: Use pre-built summaries (one weighted mean per observation)
  summaries <- read_parquet(
    "processed/climate/terraclimate/damage_areas_summaries_long.parquet"
  )
  my_climate <- summaries %>%
    filter(DAMAGE_AREA_ID %in% unique(my_obs$DAMAGE_AREA_ID))

  # OPTION B: Keep individual pixel values (for within-polygon variation)
  pixel_values <- read_parquet(
    "processed/climate/terraclimate/pixel_values.parquet"
  )
  my_pixel_climate <- my_pixels %>%
    inner_join(pixel_values, by = "pixel_id")

  # --- 6. Merge climate back to IDS attributes ---
  result <- my_obs %>%
    st_drop_geometry() %>%
    inner_join(
      my_climate %>% filter(variable == "tmmx"),
      by = "DAMAGE_AREA_ID"
    )

NOTE ON PANCAKE FEATURES: Multiple OBSERVATION_IDs can share the same
DAMAGE_AREA_ID (same geometry, different damage agents). When computing
total affected area, group by DAMAGE_AREA_ID first to avoid double-counting.
Pancake features are flagged by OBSERVATION_COUNT = "MULTIPLE".

================================================================================
HOW TO: Get Climate Data for a Survey Area
================================================================================
Goal: Retrieve TerraClimate values for surveyed areas (the polygons describing
where aerial surveys actually flew), rather than for individual damage
observations.

  library(sf)
  library(dplyr)
  library(arrow)

  # --- 1. Load surveyed areas ---
  survey_areas <- st_read(
    "01_ids/data/processed/ids_layers_cleaned.gpkg",
    layer = "surveyed_areas"
  )
  # Filter if needed:
  #   survey_areas %>% filter(SURVEY_YEAR == 2020, REGION_ID == 5)

  # --- 2. Get the TerraClimate pixels for those survey polygons ---
  pixel_map <- read_parquet(
    "02_terraclimate/data/processed/pixel_maps/surveyed_areas_pixel_map.parquet"
  )
  my_pixels <- pixel_map %>%
    filter(SURVEY_FEATURE_ID %in% survey_areas$SURVEY_FEATURE_ID)

  # --- 3. Load pixel values and join ---
  pixel_values <- read_parquet(
    "processed/climate/terraclimate/pixel_values.parquet"
  )
  survey_climate <- my_pixels %>%
    inner_join(pixel_values, by = "pixel_id")

  # --- 4. Compute area-weighted mean per survey polygon ---
  survey_summaries <- survey_climate %>%
    group_by(SURVEY_FEATURE_ID, calendar_year, calendar_month,
             water_year, water_year_month, variable) %>%
    summarize(
      weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                      sum(coverage_fraction[!is.na(value)]),
      n_pixels = n(),
      .groups = "drop"
    )

NOTE: Surveyed areas use SURVEY_FEATURE_ID (not OBSERVATION_ID) as their
primary key. The pre-built summaries (step 4) are generated for damage_areas
by default. For surveyed_areas, compute summaries from pixel values directly
as shown above, or run script 04 with the surveyed_areas layer.

================================================================================
SURVEY_YEAR vs WATER YEAR
================================================================================
IDS observations have SURVEY_YEAR (integer, no month). TerraClimate pixel
values store monthly data with BOTH calendar_year/calendar_month AND
water_year/water_year_month on every row. All years of climate data are
extracted for every pixel -- not just the observation year.

The water year runs Oct-Sep. When joining IDS observations to climate:

  - calendar_year == SURVEY_YEAR gives Jan-Dec of the observation year
  - water_year == SURVEY_YEAR gives Oct(prior year)-Sep(observation year)

These share 9 months (Jan-Sep) but differ on 3 (Oct-Dec). Because IDS
surveys are typically flown in summer/fall, the damage being observed was
often driven by climate from the preceding winter/spring -- which is better
captured by the water year. However, without month-of-survey there is
inherent ambiguity.

The pixel values and summaries include both time systems on every row, so
you choose the time window at analysis time:

  # Calendar year match: Jan-Dec of SURVEY_YEAR
  my_climate %>% filter(calendar_year == 2020)

  # Water year match: Oct 2019 - Sep 2020
  my_climate %>% filter(water_year == 2020)

  # Custom: prior water year (lagged climate)
  my_climate %>% filter(water_year == 2020 - 1)

  # Growing season only (Apr-Sep = water year months 7-12)
  my_climate %>% filter(water_year == 2020, water_year_month >= 7)

This is a downstream analysis decision. The extraction pipeline is agnostic
-- it stores all months for all years and lets you filter at join time.

================================================================================
VARIABLES EXTRACTED (14 total)
================================================================================
See data_dictionary.csv for complete field definitions and scale factors.

Temperature:          tmmx (max), tmmn (min) -- degrees C
Precipitation/Water:  pr (precip), aet (actual ET), pet (reference ET),
                      def (water deficit), soil (moisture), swe (snow),
                      ro (runoff) -- mm
Atmospheric:          vap (vapor pressure), vpd (vapor pressure deficit),
                      srad (solar radiation), vs (wind speed)
Drought Index:        pdsi (Palmer Drought Severity Index) -- unitless

================================================================================
OTHER DOCUMENTATION
================================================================================
  WORKFLOW.md        Technical architecture, script details, design decisions
  cleaning_log.md    Data quality issues and resolutions
  data_dictionary.csv  Field definitions and scale factors for all output tables
  docs/terraclim_ref.pdf  TerraClimate reference publication
