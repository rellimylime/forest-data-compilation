# ERA5 Monthly Variables for Forest Insect/Disease Analysis

## Purpose
This note is intended for decision-making, not just documentation.

It summarizes:

- what is currently in the ERA5 monthly download pipeline
- what is ready to use now for monthly analysis
- what additional variables are easy to add
- what variables would require a separate sub-daily pipeline to make monthly
- what can be derived from existing variables without new downloads
- what `analysis (an)` vs `forecast (fc)` means in ERA5 tables

## Executive Summary (Current Status + Decisions)

### Current status (where we are now)
- We currently have a **44-variable ERA5 monthly configuration** for forest-relevant climate analysis.
- These 44 variables are all in the **direct ERA5 monthly product** used by the current downloader.
- We intentionally removed a small set that caused problems in this monthly workflow:
  - `snowc` (snow cover fraction) due CDS/MARS ambiguity issues in this pipeline
  - `mn2t`, `mx2t`, `i10fg` (the "since previous post-processing" versions) because they do not have direct monthly means in ERA5 monthly means

### Forest-health candidate universe (what we are choosing from)
- The boss-review CSV is the broader forest-health long-list, not just the current config.
- Current forest-focused review universe: **281 variables** (after removing strict ocean/wave-only variables).
- Of these 281 variables:
  - **214** are documented as directly available in ERA5 monthly means
  - **5** are documented as **not** direct monthly means (`no mean`) but can be aggregated to monthly from sub-daily ERA5
  - **62** are not covered by the Table 8 monthly-exception list (mostly upper-air/column/advanced fields); they are treated as potentially aggregatable from sub-daily ERA5 and should be evaluated case-by-case
- The current 44-variable config is therefore a **base subset**, not the full set of potentially relevant forest-health variables.

### Main decision options (for variable expansion)
1. **Stay with the current base set (lowest risk, fastest)**
- Good for broad monthly forest-climate analyses.

2. **Add a small “easy monthly” extension (recommended next step)**
- Direct monthly variables that are ecologically relevant and low-friction to add.

3. **Add a “derived monthly” extension (no new downloads, more feature engineering)**
- Build additional interpretable variables from the current base set.

4. **Create a separate sub-daily pipeline for event/extreme variables (higher effort)**
- Needed for biologically important monthly event metrics (e.g., monthly max gust, freeze/heat counts).

5. **Run a larger independent ERA5 feature expansion (highest effort)**
- Broad exploration of the remaining forest-health candidate universe beyond the current base set (including advanced column, vertical-integral, radiation, and land-surface diagnostics).

## Key Concept: Not All ERA5 Variables Are the Same Kind of Data

For monthly analysis, variables should be separated into three groups:

1. **Direct monthly ERA5 variables**
- ERA5 already provides monthly values.
- Lowest processing burden.

2. **Sub-daily ERA5 variables that can be converted to monthly**
- Require an hourly/sub-daily download + aggregation rule.
- More flexible, but more work and more assumptions.

3. **Derived variables**
- Computed from variables in groups 1 or 2.
- Useful and often ecologically interpretable, but require formula provenance.

This grouping is what makes the analysis scientifically interpretable.

## What `an` and `fc` Mean (Plain Language)

In the ERA5 parameter tables:

- `an` = **analysis field available**
- `fc` = **forecast field available**

How to interpret this conceptually:

- **Analysis fields** are state estimates (for example many temperature/pressure/cloud variables).
- **Forecast fields** are often variables that depend on forecast steps or processing periods (for example many accumulations and fluxes).

What this does **not** mean:

- `fc` does **not** mean low quality or unusable.
- Many ecologically important variables are forecast-type fields and are valid for analysis.

Why this matters:

- It helps explain why some variables are straightforward monthly means and others require care.
- It affects how we aggregate and interpret variables if we move to sub-daily processing.

## What “Monthly-Ready” Means in This Project

A variable is considered **monthly-ready** for the current project if:

- it is in the project ERA5 config
- it is available in the ERA5 monthly product used by the downloader
- its unit conversion/scaling is defined in the pipeline
- its monthly interpretation is clear enough for comparative analysis

This is a practical definition, not a scientific judgment that other variables are unimportant.

## Important Scope Clarification (44 vs. 281 Variables)

- The **44 variables** are the current production-ready monthly download set.
- The **281 variables** in `05_era5/data/metadata/era5_variable_metadata_review.csv` are the broader forest-health candidate universe for decision-making.
- This guide now treats the 44 as **Option 0 (base set)** and uses the remaining variables as candidate add-ons in higher-effort options.
- Not every one of the 281 should be included immediately; the point is to make the tradeoffs explicit and choose expansion paths intentionally.

## Important Caveat: Monthly Does Not Mean “Use the Same Aggregation for Everything”

### 1. State / instantaneous variables
Examples:

- temperature
- pressure
- cloud cover
- soil moisture
- boundary layer height

Typical monthly use:

- monthly mean is usually appropriate

Main caveat:

- monthly means can hide biologically important extremes (heat/freeze/wind events)

### 2. Accumulations, fluxes, and rates
Examples:

- precipitation / snowfall
- runoff
- radiative accumulations
- sensible / latent heat flux variables

Typical monthly use:

- direct monthly values are often usable, but unit interpretation matters
- if aggregated from hourly data later, aggregation method must match the physical meaning

Main caveat:

- these are the easiest variables to misinterpret if units/sign conventions are not handled consistently

### 3. Event/extreme variables (“since previous post-processing”)
Examples:

- max/min 2m temperature since previous post-processing
- some gust/extreme rate variables

Typical monthly use:

- usually not direct monthly means in ERA5 monthly means
- if needed, build monthly event metrics from sub-daily data

Main caveat:

- a “monthly mean of an extreme variable” is often less useful than event counts or monthly maxima/minima

## Decision Framework (How To Choose What To Add)

Use this order of questions:

1. Is the variable ecologically plausible for insect/disease impacts on forests?
2. Is it directly available in ERA5 monthly means?
3. If not, can it be meaningfully made monthly from sub-daily ERA5?
4. Is it redundant with something we already have or can derive?
5. Can we explain it clearly to a non-ERA5 audience?

## Option Sets (Status + Add-On Paths)

## Option 0: Current Base Set (Recommended Baseline)
This is the current **44-variable monthly-ready set** in `config.yaml`.

Strengths:

- already configured
- directly usable in monthly ERA5 workflow
- broad coverage of temperature, moisture, wind, radiation, clouds, snow, runoff, and land surface conditions

Best use:

- first-pass forest insect/disease monthly analyses
- comparative/climatological summaries
- baseline modeling and screening

What this is **not**:

- It is not the full list of forest-health-relevant ERA5 candidates.
- It is the smallest stable set that is already configured and downloading successfully in the current monthly pipeline.

## Option 1: Easy Monthly Add-On (Recommended Next Step)
These are direct-monthly variables that are forest-relevant and low-friction to add.

Recommended add-on list:

- `instantaneous_10m_wind_gust` (direct monthly; use instead of removed no-mean gust variable)
- `convective_inhibition` (`cin`)
- `cloud_base_height` (`cbh`) (optional but useful)
- `surface_solar_radiation_downward_clear_sky` (`ssrdc`)
- `surface_net_solar_radiation_clear_sky` (`ssrc`)
- `surface_thermal_radiation_downward_clear_sky` (`strdc`)
- `surface_net_thermal_radiation_clear_sky` (`strc`)

Why this option is attractive:

- direct monthly availability
- minimal pipeline redesign
- improves interpretation of storms/cloudiness/radiation potential

## Option 2: Derived Monthly Add-On (No New Downloads)
These variables can be derived from the current base set.

Examples:

- `large_scale_precipitation = total_precipitation - convective_precipitation`
- `sub_surface_runoff = runoff - surface_runoff`
- `10m wind speed = sqrt(u10^2 + v10^2)` (if desired)
- `100m wind speed = sqrt(u100^2 + v100^2)` (if desired)
- `snow_cover` (diagnostic estimate from `sd` and `rsn`, if you choose to derive it rather than download it)

Strengths:

- no additional ERA5 download burden
- improves interpretability
- easy to document as formulas

Main caveat:

- derived variables should be clearly labeled as derived (not raw ERA5 parameters)

## Option 3: Sub-Daily Event/Extreme Add-On (Separate Pipeline)
This option is for biologically important event metrics that are not good direct monthly means.

Examples of what this would enable:

- monthly maximum wind gust
- freeze event counts
- heat stress hour counts
- days above/below thresholds
- monthly maxima/minima derived from hourly variables

Examples of variables that motivate this option:

- the removed `mn2t`, `mx2t`, `i10fg` (no direct monthly means in current monthly product)

Strengths:

- more biologically aligned features
- captures events and thresholds that monthly means miss

Downsides:

- larger downloads
- more compute/storage
- more QC and methodological choices
- needs a separate, clearly documented aggregation policy

## Option 4: Full ERA5 Expansion (Independent Analysis Track)
This would explore the broader forest-health candidate universe beyond the base set (including specialized flux, radiation, land/cryosphere diagnostics, column variables, and vertical-integral fields).

Two practical versions of this option:

- **4A. Direct-monthly expansion track**
  - Add from the remaining **170 direct-monthly variables** in the forest-health candidate universe that are not currently in config.
  - Best for expanding coverage without building a sub-daily pipeline first.
- **4B. Full forest-health expansion track**
  - Consider all **281 forest-relevant variables** in the boss-review CSV.
  - Includes variables that require sub-daily aggregation or independent methodological choices.

Best use:

- later-stage feature discovery
- focused mechanistic analysis
- separate exploratory track from the main production climate merge

Downsides:

- high dimensionality
- high redundancy
- much harder interpretation
- significantly more analyst time

## Recommended Practical Strategy (Short-Term)

### Phase 1 (now)
- Use the current **44-variable base set**
- Finalize variable choices with a clear rationale

### Phase 2 (low effort upgrade)
- Add the **easy monthly add-on set** (Option 1)
- Optionally add **derived monthly add-on variables** (Option 2)

### Phase 2b (broader monthly expansion, still no sub-daily pipeline)
- Review the remaining **direct-monthly** forest-health candidates in the boss-review CSV
- Add only those with clear ecological rationale and low redundancy

### Phase 3 (if needed for mechanism-focused questions)
- Build a separate **sub-daily event metrics** workflow (Option 3)

This keeps the main monthly workflow stable while leaving room for stronger ecological features later.

## Base Set and Add-On Variable Bundles (Decision Appendix)

## A. Forest-Health Candidate Universe (Full Decision Space)

This appendix is organized around the full forest-health candidate universe represented in:

- `05_era5/data/metadata/era5_variable_metadata_review.csv` (**281 rows**)

Use this as the long-list, and use the option bundles below as selection paths.

### A1. Candidate universe summary (current snapshot)
- **281** forest-health candidate variables in the boss-review CSV
- **44** currently configured and downloaded by the monthly ERA5 script (base set)
- **237** not currently in the production monthly download config
- **214** direct ERA5 monthly mean available from docs
- **170** direct ERA5 monthly mean candidates not currently in config
- **5** explicitly `no mean` in docs but can be made monthly from sub-daily ERA5
- **62** additional advanced variables (mostly upper-air/column/vertical-integral rows not covered by Table 8 monthly exception table), which should be treated as an exploratory/advanced pool

### A2. Candidate universe by variable family (counts)
- surface flux/evaporation: **50**
- radiation: **39**
- temperature: **28**
- precipitation/snow: **23**
- land surface: **22**
- wind: **19**
- dynamics/convection: **16**
- clouds: **10**
- atmosphere composition / column: **9**
- snow/cryosphere + cryosphere: **13** combined
- pressure: **5**
- other / specialized diagnostics: **45**

This is why the guide uses option bundles rather than one long recommended list: the long-list is broad and includes both immediately useful near-surface variables and advanced diagnostics.
### A3. How to read the boss-review CSV for decisions
Key columns to use in the review CSV:

- `currently_downloaded_by_project_monthly_script`
- `direct_era5_monthly_mean_available_from_docs`
- `can_make_monthly_from_subdaily`
- `variable_type_family`
- `value_origin`
- `monthly_availability_summary`
- `why_not_downloaded_now`

Practical interpretation:

- Start with `currently_downloaded_by_project_monthly_script = TRUE` (base set)
- Next review rows with `direct_era5_monthly_mean_available_from_docs = TRUE` (easy expansion pool)
- Then review rows with `direct_era5_monthly_mean_available_from_docs = FALSE` and `can_make_monthly_from_subdaily = TRUE` (sub-daily aggregation pool)
- Treat highly specialized vertical-integral/upper-air rows as a separate advanced option unless there is a strong ecological reason
## B. Current Base Set (44 variables; direct monthly; currently downloaded)

This section lists all **44** currently configured and downloaded base-set variables.

### B1. State / instantaneous / structural variables (mostly Table 2 + one Table 6)
- `t2m` (`2m_temperature`)
- `d2m` (`2m_dewpoint_temperature`)
- `sp` (`surface_pressure`)
- `msl` (`mean_sea_level_pressure`)
- `u10` (`10m_u_component_of_wind`)
- `v10` (`10m_v_component_of_wind`)
- `u100` (`100m_u_component_of_wind`)
- `v100` (`100m_v_component_of_wind`)
- `tcc` (`total_cloud_cover`)
- `lcc` (`low_cloud_cover`)
- `mcc` (`medium_cloud_cover`)
- `hcc` (`high_cloud_cover`)
- `blh` (`boundary_layer_height`)
- `cape` (`convective_available_potential_energy`)
- `fal` (`forecast_albedo`)
- `lai_hv` (`leaf_area_index_high_vegetation`)
- `lai_lv` (`leaf_area_index_low_vegetation`)
- `skt` (`skin_temperature`)
- `sd` (`snow_depth`)
- `rsn` (`snow_density`)
- `stl1` (`soil_temperature_level_1`)
- `stl2` (`soil_temperature_level_2`)
- `stl3` (`soil_temperature_level_3`)
- `stl4` (`soil_temperature_level_4`)
- `swvl1` (`volumetric_soil_water_layer_1`)
- `swvl2` (`volumetric_soil_water_layer_2`)
- `swvl3` (`volumetric_soil_water_layer_3`)
- `swvl4` (`volumetric_soil_water_layer_4`)
- `tcwv` (`total_column_water_vapour`)

### B2. Accumulations / fluxes / rates (direct monthly, but interpretation requires more care)
- `tp` (`total_precipitation`)
- `cp` (`convective_precipitation`)
- `sf` (`snowfall`)
- `e` (`total_evaporation`)
- `pev` (`potential_evaporation`)
- `ro` (`runoff`)
- `sro` (`surface_runoff`)
- `smlt` (`snowmelt`)
- `ssrd` (`surface_solar_radiation_downwards`)
- `ssr` (`surface_net_solar_radiation`)
- `str` (`surface_net_thermal_radiation`)
- `strd` (`surface_thermal_radiation_downwards`)
- `sshf` (`surface_sensible_heat_flux`)
- `slhf` (`surface_latent_heat_flux`)
- `fdir` (`total_sky_direct_solar_radiation_at_surface`)

### B3. Base set notes
- These are all currently configured and downloaded by the monthly ERA5 script.
- The variables in **B2** are monthly-ready, but should be treated as a separate “flux/accumulation” group in analysis documentation because unit/sign handling matters more.

## C. Easy Monthly Add-On Set (Direct monthly; low-friction additions)

### C1. Recommended additions
- `instantaneous_10m_wind_gust`
- `convective_inhibition` (`cin`)
- `cloud_base_height` (`cbh`) (optional)
- `surface_solar_radiation_downward_clear_sky` (`ssrdc`)
- `surface_net_solar_radiation_clear_sky` (`ssrc`)
- `surface_thermal_radiation_downward_clear_sky` (`strdc`)
- `surface_net_thermal_radiation_clear_sky` (`strc`)

### C2. Why this set
- Adds wind/storm context and cloud/radiation diagnostics without requiring a new data pipeline.
- Supports stronger ecological interpretation (stress, storm environment, cloud effects).

### C3. Broader direct-monthly expansion pool (beyond the base set)
In addition to the recommended easy add-ons above, the forest-health candidate universe currently contains **170 direct-monthly variables not in the config**.

This broader pool includes (examples by theme):

- land surface and vegetation descriptors (land cover fractions, vegetation types, soil type, land-sea mask)
- snow/cryosphere diagnostics (snow albedo, snow evaporation, snow temperature)
- additional precipitation and snowfall components (large-scale fractions/rates, convective rain/snow rates)
- clear-sky and top-of-atmosphere radiation diagnostics
- turbulence/stress and boundary-layer diagnostics
- total-column water/cloud species and ozone
- selected vertical-integral moisture/energy transport diagnostics (advanced)

Recommendation:

- Treat this as a **screened expansion pool**, not a single bundle to add all at once.

## D. Derived Monthly Add-On Set (No new downloads)

### D1. High-value derived variables from current base set
- `large_scale_precipitation = tp - cp`
- `sub_surface_runoff = ro - sro`
- `wind_speed_10m = sqrt(u10^2 + v10^2)`
- `wind_speed_100m = sqrt(u100^2 + v100^2)`

### D2. Optional derived snow diagnostic
- `snow_cover` (estimated from `sd` and `rsn`) if desired for interpretation
- This should be explicitly labeled as a **derived** variable (formula-based), not a raw downloaded parameter

### D3. Why this set
- Adds ecological interpretability and convenience without expanding ERA5 download time

## E. Sub-Daily Event/Extreme Add-On Set (Separate pipeline required)

### E1. Candidate features (examples)
- monthly maximum wind gust
- monthly minimum/maximum temperature (from hourly ERA5)
- freeze event counts
- heat stress counts (hours/days above threshold)
- drought/warm spell event counts or durations

### E2. Previously removed variables that motivate this path
- `mn2t` (min 2m temperature since previous post-processing)
- `mx2t` (max 2m temperature since previous post-processing)
- `i10fg` (10m wind gust since previous post-processing version)

Additional examples in the forest-health candidate universe that fall into this logic:

- variables marked in the CSV as `No direct ERA5 monthly mean (docs 'no mean'), but can be aggregated from sub-daily ERA5`
- event/extreme metrics that are better computed from hourly thresholds than from pre-packaged extrema fields
### E3. Why this set matters
- These features may be more biologically meaningful than monthly means alone

### E4. Main tradeoff
- Much more work and more methodological choices than Options 0–2

## F. Full Expansion Track (Independent / exploratory)

### F1. What this means
- Review the full **281-variable forest-health candidate universe**
- Select additional variables in staged bundles (direct monthly first, sub-daily/advanced second)
- Includes specialized flux/rate families, vertical integrals, column diagnostics, and advanced diagnostics

### F2. When to use it
- targeted mechanistic studies
- separate exploratory modeling track
- later project phase after the base workflow is stable

### F3. Why not first
- high redundancy and complexity
- harder to explain to non-ERA5 audiences
- larger risk of overfitting / feature sprawl

### F4. Suggested staged structure for a full expansion (easier to manage)
1. Add a curated set of remaining direct-monthly variables (theme-based)
2. Add derived variables with clear formulas and provenance
3. Add sub-daily event metrics for specific biological hypotheses
4. Evaluate advanced column/vertical-integral variables only if they improve interpretation or model performance

## Communication Guidance (How To Present This)

If presenting verbally, a simple framing is:

- “We have a strong monthly-ready base set now.”
- “We can add a small number of useful variables immediately with low effort.”
- “For event/extreme ecology questions, we need a separate sub-daily feature pipeline.”
- “We are separating direct monthly variables from derived and event-based features so the analysis stays interpretable.”
- “The 44 variables are our current operational base set, but the decision universe includes 281 forest-relevant ERA5 candidates documented in the review CSV.”

## Related Files
- Forest-focused variable review CSV: `05_era5/data/metadata/era5_variable_metadata_review.csv`
- Full ERA5 metadata CSV: `05_era5/data/metadata/era5_variable_metadata.csv`
- Export script: `05_era5/scripts/00_export_era5_variable_metadata.R`
