# Thermophilization Analysis: Step-By-Step Walkthrough

**Audience:** you (so you can hold the whole pipeline in your head), your boss (so you can answer "why did you do it that way?"), and future-you in six months when this is cold.
**Format:** notes, bullets, decision trees, worked toy examples. Not prose. Skim it, find the part you need, dig in.
**Companion docs:** [thermophilization_plan.md](thermophilization_plan.md) is the working tracker (status + open questions). This file is the explainer.

---

## The question, in one paragraph

We want to know whether forests that get clobbered by an extreme disturbance (fire, beetle outbreak, disease, wind) regenerate with **different species** than the species coming up at undisturbed plots nearby — specifically, with species whose climate "comfort zone" is **warmer or more drought-tolerant**. If yes, that is a fingerprint of climate change reshaping forest composition through disturbance. The boss's hypothesis: this should be **stronger in the West** than the East.

---

## Big-picture decision tree

```
Is the data product about "what's at the plot"?  -> Pre-thermophilization (Part A)
   |- raw FIA tables                              -> A1
   |- stable plot identity across visits          -> A2
   |- which plots are disturbed vs control        -> A3
   |- baseline climate at each plot               -> A4
   |- each species' climate "comfort zone"        -> A5
   |- each plot's seedling community climate avg  -> A6

Is the data product about "did disturbance shift things"? -> Analysis (Part B)
   |- matched disturbed-vs-control pairs          -> B1
   |- per-pair difference (delta)                 -> B2
   |- averaged by disturbance class x region      -> B3
   |- broken out over time since disturbance      -> B4
   |- drilled into a single suspicious cell       -> B5
   |- species-level explanation of that cell      -> B6
   |- single-species artifact check               -> B7
```

---

# PART A — Pre-thermophilization (the foundation)

Everything in Part A is just **building the inputs**. The actual ecological question isn't tested until Part B. But the credibility of Part B rests entirely on whether Part A is right.

---

## A1. FIA data ingestion

**What FIA is:** the U.S. Forest Service's plot-based inventory. ~410K plots nationwide, revisited on a rolling 5–10 year cycle. Each plot has nested sampling: subplots → trees, microplots → seedlings, condition records → forest type and disturbance.

**Tables we use:**

| Table  | Grain | What we get |
|---|---|---|
| `PLOT`     | one row per visit | inventory year, lat/lon, state/county/plot ids |
| `COND`     | one row per condition within a plot | forest type, disturbance codes, treatment codes |
| `TREE`     | one row per tree (>= 1 inch diameter) | species, status (live/dead), used for species range |
| `SEEDLING` | one row per microplot x species | seedling counts — **the recruitment signal** |

**Key choice — why FIA as the backbone, not IDS (insect/disease survey polygons):**

- FIA disturbance codes are **field-recorded by the crew at the plot**. IDS polygons are aerial sketches over millions of acres.
- FIA gives us plot-level seedling counts (the response we care about). IDS doesn't.
- Cost: FIA disturbance codes are coarse — a "fire" code doesn't tell you crown vs surface severity. We accept this and use FIA's `crown fire` code (`32`) as our high-severity proxy.
- IDS is on the backlog as **enrichment** for severity/agent attribution, not as the first-pass classifier.

**Scripts:** [05_fia/scripts/01_download_fia.R](../05_fia/scripts/01_download_fia.R) → 04 extract trees and seedlings → 05 build summaries.

---

## A2. Stable plot IDs

**Problem:** FIA's `PLT_CN` is a *visit* id, not a *plot* id. The same physical plot gets a new `PLT_CN` every time a crew comes back, so you can't join a 2010 visit to a 2020 visit on `PLT_CN` alone.

**What `PLT_CN` looks like in the data:** an opaque numeric string, e.g. `230456789010661`. No structure you can read; same patch of forest visited 10 years later might be `285194317010661`. Two unrelated identifiers.

**What does carry across visits:** the plot's location codes — `STATECD`, `UNITCD` (FIA survey unit within state), `COUNTYCD`, `PLOT` (the per-county plot number). Those four together uniquely identify a physical plot.

**What we do:** concatenate them.

```
stable_plot_id = STATECD _ UNITCD _ COUNTYCD _ PLOT
```

e.g. `8_2_109_63473`. Reads as: state 8 (Colorado), survey unit 2, county 109 (Park County), plot number 63473.

**What it looks like in the data:**

| PLT_CN          | INVYR | STATECD | UNITCD | COUNTYCD | PLOT  | stable_plot_id  |
|-----------------|-------|---------|--------|----------|-------|-----------------|
| 230456789010661 | 2008  | 8       | 2      | 109      | 63473 | `8_2_109_63473` |
| 257812345010661 | 2013  | 8       | 2      | 109      | 63473 | `8_2_109_63473` |
| 285194317010661 | 2018  | 8       | 2      | 109      | 63473 | `8_2_109_63473` |

Three rows, three different `PLT_CN`s, **same physical plot** — because `stable_plot_id` is the same string in all three rows. That's what lets us join climate (one row per `stable_plot_id`) to seedling records (many rows per `PLT_CN`) without losing visits.

We also use `stable_plot_id` as the `site_id` in the climate extraction (A4), so the climate file joins directly without a coordinate lookup.

**Why this and not `PREV_PLT_CN` (FIA's official cross-cycle pointer):**

- `PREV_PLT_CN` is more rigorous (handles edge cases where FIA re-keys a plot), but more code.
- The concatenated id is enough for matching plots to other plots. A `PREV_PLT_CN`-based panel is on the backlog if we ever do true within-plot before/after.

---

## A3. Disturbance classification

**Goal:** turn raw FIA codes into a small set of disturbance **classes** plus a clean **control** group.

### How FIA records disturbance

- Each `COND` record has up to 3 disturbance code slots (`DSTRBCD1`, `DSTRBCD2`, `DSTRBCD3`) and 3 treatment slots (`TRTCD1-3`).
- Codes go in groups of 10: 10s = insect, 20s = disease, 30s = fire, 50s = weather/wind, 80 = human.
- The "year" field can hold `9999` meaning "ongoing/unknown" — important for time-since analyses (see Part B caveat).

### Classes we use

| Class    | FIA codes | Notes |
|---|---|---|
| fire     | 30, 31, 32 | 32 = crown fire (high-severity proxy) |
| insect   | 10, 11, 12 | bark beetle, defoliators, etc. |
| disease  | 20, 21, 22 | pathogens, including BBD, EAB |
| weather  | 50, 52, 54 | wind/hurricane (52), drought (54) |
| other    | 60–79 plus rare codes | catch-all |
| **control** | no disturbance codes, no harvest, forested | the reference group |

### Decision tree: is this plot a candidate?

The forested gate is applied **first** — both disturbed and control candidates have to pass it. Then human/treatment gets filtered out, then we ask whether a natural disturbance code is present.

```
Is the condition forested with pct_forested >= 0.5?
   NO  -> EXCLUDE (not a forest, or too partial to be interpretable)
   YES -> Continue
       Any harvest or treatment record (DSTRBCD == 80, OR any TRTCD non-zero)?
           YES -> EXCLUDE (human-managed; tagged "human_or_harvest" or
                  "other_treatment" in the table but never enters
                  the matching pool)
           NO  -> Continue
               Does it have any natural-disturbance code
               (DSTRBCD in 10s/20s/30s/50s/etc.)?
                   YES -> "disturbed" candidate (is_natural_disturbance_candidate)
                   NO  -> "control" candidate     (is_control_candidate)
```

**What "disturbed candidate" means:** the condition is *eligible* to enter the disturbed pool of `plot_matches.parquet`. The candidate flag is just the upstream gate — the matching itself (forest type group + region must match, climate within caliper) happens later in script 03. A "disturbed candidate" can still end up unmatched if no acceptable control exists in its stratum.

**Same for "control candidate":** it's eligible to be drawn as a control. Whether it actually gets used depends on whether some disturbed plot picks it as one of its K=5 nearest matches.

### Why "no disturbance AND no treatment" for controls

A control isn't just "no human disturbance" — it's "nothing happened to it at all." If a control plot had a recent fire, it's not a clean baseline against which to measure the recruitment shift on a *different* fire plot. Same for treatment: a thinned-but-not-disturbed plot has its own recruitment story we don't want pretending to be the unperturbed reference.

### Why exclude harvest from the disturbed side

If we mixed harvested plots into "disturbed", we'd be measuring management decisions, not climate-driven recruitment. Harvest also has its own well-studied response we don't want to confound with the natural-disturbance signal.

**Output:** `plot_disturbance_classification.parquet`. One row per `(PLT_CN, INVYR, CONDID)` with `disturbance_class`, `is_control_candidate`, `is_natural_disturbance_candidate`, and `disturbed_vs_control` (one of `"control"`, `"disturbed"`, `"exclude_or_other"`).

---

## A4. Climate extraction (the slow expensive step)

**Goal:** get a 30-year baseline (1981–2010) of temperature, precipitation, and climate water deficit (CWD) at every FIA plot location.

### Why these three variables

- **Temperature (annual mean of monthly tmean)** — the headline thermophilization axis. Are post-disturbance recruits adapted to warmer temperatures?
- **Precipitation (annual sum)** — the obvious "drought" axis, but it's **incomplete**. A wet, hot place and a wet, cool place have very different water stress because evaporation depends on temperature.
- **CWD (climate water deficit)** — the difference between potential evapotranspiration (PET, "what the atmosphere wants") and actual evapotranspiration (AET, "what actually evaporates"). High CWD = chronic moisture stress regardless of total rainfall. **This is our preferred drought metric** because it captures both supply and demand.

### Why TerraClimate

- 1/24-degree resolution (~4 km), monthly, 1958–present.
- Already has CWD computed (`def`).
- Free, on Google Earth Engine.
- Trade-off: 4 km is coarse for mountains. Acceptable because FIA coordinates are themselves fuzzed by ~1 km for privacy.

### Why 1981–2010 baseline

- Standard climate-normal window in ecology and climatology.
- Pre-dates most of the strongest recent warming, so it represents the climate species were "established under."
- Trade-off: by using a fixed baseline (rather than the actual climate of each plot's inventory year), we are measuring **realized historical climate envelope**, not current conditions. That's intentional — we want each species' "where it was found" climate, not a moving target.

### Workflow

1. [05_fia/scripts/06a_build_fia_site_list.R](../05_fia/scripts/06a_build_fia_site_list.R) builds the unique site list from `cond` extract: 408,040 distinct plots with valid lat/lon. `site_id = stable_plot_id`.
2. [05_fia/scripts/06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) hits Google Earth Engine, snaps each plot to its TerraClimate pixel, pulls monthly `tmmx`, `tmmn`, `pr`, `def`, `pet`, `aet` for 1958–2024.
3. Output: `site_climate.parquet`, ~1.95B rows, 5.85 GB. 408K sites collapse to 338K unique pixels (multiple plots share a pixel in dense areas).

### Result we use downstream

For each `site_id` (= stable plot), three numbers:

```
site_temp_mean    = mean of monthly tmean across 1981-2010
site_precip_mean  = mean of annual precip sums across 1981-2010
site_cwd_mean     = mean of annual CWD sums across 1981-2010
```

---

## A5. Species climate-affinity traits

**Goal:** for each tree species, summarize the **climate of every place it actually grows**. This becomes the species' "climate fingerprint."

### Intuition

If we want to know whether species coming up after a fire are warm-adapted, we need a measure of "how warm does this species like it." Two ways to measure that:

1. **Physiological** — measure heat tolerance in a lab. Available for ~50 species. Gold standard but tiny coverage.
2. **Realized climate envelope** — look at every place the species is actually found, average the climate. Available for **every species in FIA**. Less mechanistic but high coverage.

We use option 2 because we need traits for ~600 species.

### Choices made — script [01_build_species_climate_affinity.R](../06_traits/scripts/01_build_species_climate_affinity.R)

| Choice | What we did | Why |
|---|---|---|
| Live trees only (`STATUSCD == 1`) | filter `TREE` table | Dead trees don't tell us where the species is currently established |
| Trees, not seedlings | use `TREE` table | Seedlings are noisy (lots of plots have a few outlier rare species). Adult trees are a stable signal of "this species can live here." |
| Dedupe to (stable_plot_id, SPCD) | `unique()` after join | A plot revisited 3 times shouldn't count 3× toward a species' envelope. Equal weight per location. |
| Mean + p10 + p90 + min + max | quantiles | Mean is the easy interpretation. p10/p90 are robust to outliers. Min/max kept for diagnostics but use cautiously. |

### Result

`species_climate_affinity.parquet` — 603 species, 925,947 unique (plot, species) occurrences. One row per species with:

- `temp_mean`, `temp_p10`, `temp_p90`, `temp_min`, `temp_max`
- same for `precip_*` and `cwd_*`
- `n_occurrences` (used downstream as a quality filter)

### Plain-language sanity check

Examples to reassure us this is working:

- **American beech**: `temp_mean = 10.23` deg C — northern hardwoods.
- **Live oak**: warmer (Gulf Coast).
- **Subalpine fir**: cold (high western mountains).
- **Loblolly pine**: warm (Southeast).

If you ever see a wildly off value (e.g. balsam fir at 18 deg C), the trait calc broke.

### Why we don't filter rare species *here*

We keep all 603 species in the trait table. The minimum-occurrence filter (`n_occurrences >= 30`) is applied **downstream in script 02**, so we can change the threshold without rebuilding traits.

---

## A6. Per-plot recruitment CWM (community-weighted mean)

**Goal:** turn each plot's seedling community into **one number** that summarizes its climate affinity.

### What CWM means

A community-weighted mean is just the **abundance-weighted average** of a trait across the species present.

```
CWM_temp(plot) = sum( seedling_count(species) * temp_mean(species) ) / sum( seedling_count(species) )
```

### Toy example

A plot has:

| Species | Seedling count | Species temp_mean |
|---|---|---|
| Red maple   | 8 | 11.2 deg C |
| Sugar maple | 2 | 8.4 deg C  |

```
CWM_temp = (8 * 11.2 + 2 * 8.4) / (8 + 2) = (89.6 + 16.8) / 10 = 10.64 deg C
```

So this plot's seedling community has a **community-weighted average climate affinity of 10.64 deg C**. If next year all the red maple died and only sugar maple was left, CWM would drop to 8.4. That's the signal we're tracking.

### Choices made — script [02_build_plot_recruitment_cwm.R](../06_traits/scripts/02_build_plot_recruitment_cwm.R)

| Choice | What we did | Why |
|---|---|---|
| Grain = condition (`PLT_CN x INVYR x CONDID`) | one CWM per forested condition | Different conditions on the same plot can have different forest types — keep them separate |
| Weight by raw `treecount_total` | not by `TPA_UNADJ` (per-acre expansion) | Simpler. Per-acre expansion is on the sensitivity-check backlog. |
| Sum across `SUBP` first | aggregate microplots within condition | A plot has 4 microplots; we want plot-level community, not per-microplot |
| Drop species with `n_occurrences < 30` | trait quality filter | Species with < 30 occurrences have noisy `temp_mean` estimates. 30 is a defensible compromise. |
| Keep diagnostics: `frac_seedlings_with_traits` | report what fraction of seedlings had usable traits | If a plot has 90% rare-species seedlings, its CWM is unreliable |

### Result

`plot_recruitment_cwm.parquet` — **475,055 conditions**. One row per condition with:

- `cwm_temp`, `cwm_precip`, `cwm_cwd` — the three community climate signals
- `n_species_total`, `n_seedlings_total` — what we summarized over
- `frac_seedlings_with_traits` — quality flag (typically 1.0; species we drop are very rare)

### What this *isn't*

- Not a per-tree trait — every species in the community contributes weighted by abundance.
- Not a measure of biodiversity — high CWM doesn't mean rich, it means warm-affinity.
- Not a measure of climate at the plot — it's the climate the community is **adapted to**, not the climate the plot **has**.

---

# PART B — The actual thermophilization analysis

Everything above produced inputs. Now we ask: **does CWM differ between disturbed plots and undisturbed plots that are otherwise as similar as possible?**

---

## B1. Matched controls (the heart of the design)

**Goal:** for every disturbed plot, find ~5 undisturbed plots that are **as climatically and ecologically similar as possible**, so that any difference in CWM can be attributed to the disturbance and not to baseline differences.

### Why matching, not regression

- Regression on "disturbed vs not" with covariates (climate, forest type) assumes you got the functional form right. Matching makes minimal assumptions: you're directly comparing similar units.
- Matching produces a clean, intuitive output: "these N disturbed plots had +X delta on average vs their controls."
- Easy to explain to a non-statistician: "we paired each disturbed plot with similar undisturbed plots."

### Decision tree: how to choose a control for a disturbed plot

```
Same forest type group? (e.g. maple-beech-birch)
   NO -> reject (different species pool entirely)
   YES -> Continue
       Same region (East/West)?
           NO -> reject (different climate/species regime)
           YES -> Continue
               Inventoried within +/- 5 years?
                   NO -> reject (large temporal sampling gap)
                   YES -> Continue
                       Climate similarity (Euclidean distance in standardized
                       temperature + precip space) within 2 SD ("caliper")?
                           NO -> reject (too different climatically)
                           YES -> ACCEPT as candidate
                               -> rank by climate distance, take 5 nearest
```

### Choices made — script [03_match_disturbed_to_controls.R](../06_traits/scripts/03_match_disturbed_to_controls.R)

| Choice | Value | Why |
|---|---|---|
| Hard match: forest type group | required exact match | Forest type drives which species *can* recruit. Comparing oak forest to spruce forest is meaningless. Forest type group (rather than raw `FORTYPCD`) is broad enough to give us match candidates. |
| Hard match: region (East/West) | required exact match | Western and eastern forests have entirely different species pools |
| Soft match: inventory year within +/- 5 yr | window | Avoids comparing a 1985 inventory to a 2020 one. 5 yr is one FIA cycle. |
| Soft match: climate similarity | Euclidean in (temp_z, prec_z) | Standardized so 1 SD in temperature equals 1 SD in precipitation in distance terms |
| Caliper: 2 SD | hard limit on climate distance | Reject obviously bad matches. 2 SD is loose — almost every disturbed plot finds enough candidates. **Tightening to 0.5 SD is on the active to-do list as a sensitivity check.** |
| K = 5 controls per disturbed plot | matching ratio | More controls = lower noise per pair, fewer = closer matches. K=5 is a common default. **K=3 and K=10 sensitivity check is on the to-do list.** |
| With replacement | same control can match multiple disturbed plots | Better matches at the cost of independence. The bootstrap CIs handle the resulting dependency. |

### Why use baseline climate (not current climate) to match

- We're trying to match plots that started from similar conditions. Current climate is itself shifting differently in different places.
- Baseline climate is a **fixed** characteristic of the location, not a moving target.

### Result

`plot_matches.parquet` — one row per (disturbed condition, matched control) pair.

- **57,055 of 57,062 disturbed conditions** had at least one usable match (the 7 unmatched are likely odd forest-type / region combinations).
- Drawn from a control pool of **330,437 conditions**.
- **Median match distance: 0.013 SD** — very close. So close that the 2 SD caliper is rarely binding. (This is exactly why "tighten to 0.5 SD" is a sensible sensitivity check, not a result-changing one.)

### What's in the output

Each row carries **both** sides' CWMs and the per-pair difference:

```
delta_cwm_temp   = dist_cwm_temp   - ctrl_cwm_temp
delta_cwm_precip = dist_cwm_precip - ctrl_cwm_precip
delta_cwm_cwd    = dist_cwm_cwd    - ctrl_cwm_cwd
```

Positive `delta_cwm_temp` = the disturbed plot's seedlings favor a warmer climate than the matched controls' seedlings. **That's the thermophilization signal.**

---

## B2. From per-pair deltas to per-plot deltas

For each disturbed plot, we now have ~5 deltas (one per matched control). To get one delta per disturbed plot:

```
plot_delta = mean( delta_cwm_temp across the 5 matches )
```

Equivalent formulation that's slightly more interpretable:

```
plot_delta = dist_cwm_temp - mean(ctrl_cwm_temp across 5 matches)
```

So each disturbed plot becomes one number: "how much warmer/cooler-affinity is my seedling community compared to the average of my 5 controls?"

---

## B3. Stratified summaries with bootstrap CIs

**Goal:** average those per-plot deltas within each `disturbance_class x region` cell, with honest uncertainty bounds.

### Why bootstrap CIs

- Standard parametric CIs assume things (normality, independence) that don't hold here — controls are reused, plots cluster spatially.
- Bootstrap is a **resampling**: we resample disturbed plots with replacement 1000 times, recompute the mean delta each time, and take the 2.5th and 97.5th percentiles.
- More honest uncertainty bounds, no assumptions about the distribution.

### Choices — script [04_stratified_thermophilization.R](../06_traits/scripts/04_stratified_thermophilization.R)

| Choice | Value | Why |
|---|---|---|
| 1000 bootstrap resamples | balance between precision and runtime | Standard; **5000 is on the sensitivity backlog** if we ever see CI bounds shifting between runs |
| `seed = 42` | reproducibility | Same input -> same CI every time |
| Resample disturbed plots, not pairs | preserves the matched structure | Resampling pairs would over-count the dependence on reused controls |

### Decision tree: how to read a result

```
Is the 95% CI [lo, hi] entirely above zero?
   YES -> reliable warming/drying signal in this cell
   NO  -> Continue
       Is it entirely below zero?
           YES -> reliable cooling/wetting signal
           NO  -> CI crosses zero -> inconclusive (could be zero)
```

### Sample-size warning

A 95% CI on a cell of n = 13 plots can be technically "below zero" but is almost certainly noise. Always check `n` alongside the CI. **As a rule of thumb: ignore cells with n < 100 unless the effect is enormous.**

### Result

`thermophilization_by_class_region.parquet` — one row per cell with `n`, `mean_delta_temp`, `lo`, `hi`, same for precip and cwd.

See [thermophilization_plan.md](thermophilization_plan.md#findings-so-far) for the actual numbers. Headlines:

- Fire: warmer recruits in **both** regions (East +0.18, West +0.13 deg C). Fits hypothesis on direction, not on East vs West.
- Insect: **cooler** recruits in both regions (-0.10 to -0.14). Opposite direction; needs explanation.
- Disease, East: **+0.42 deg C** — biggest signal anywhere. Required drilldown (see B5).

---

## B4. Time-since-disturbance — does the signal fade?

**Goal:** is the recruitment shift a one-time pulse, or does it persist over years?

### Decision tree: which time-bin signals can we trust?

```
What's the year-coverage for this disturbance class?
   > 75% known year (fire, weather East) -> trust all time bins
   30-75% (insect)                       -> trust 0-5 yr and 5-10 yr only
   < 30% (disease, other)                -> treat all time-bin results as suggestive
```

### Why this matters

FIA crews enter `9999` ("continuous/ongoing") when a disturbance has no clean start year. That's biologically reasonable (chronic disease, multi-year beetle outbreaks) but means a large fraction of disease/other plots **have no usable disturbance year**. Time-binned results for those classes are based on a small unrepresentative subset — the disease plots that *did* have a sharp onset.

### Why we split by class

The first run of script 04 pooled all classes per time bin, which made fire (warming) and insect (cooling) cancel out. Script 05 fixes this by adding `disturbance_class` to the grouping. **Lesson: always split by mechanism before averaging across time.**

### What we found

- **Fire effect lasts at least 10 years** in both regions.
- **Insect cooling holds steady** in the West.
- **Disease East signal grows** at 5–10 years (but on a small subset).
- **10–20 year bins are noise** (n = 2–79 per cell).

---

## B5. Drilldown by forest type — script [06_disease_east_drilldown.R](../06_traits/scripts/06_disease_east_drilldown.R)

**Why drill in:** the Disease-East +0.42 deg C signal is the largest in the whole table. Before reporting it, we need to know **whether it's a real broad pattern or a single forest type carrying everything**.

### What we did

For Disease-East matches only, broke results down by `forest_type_group`. Result:

- 53% of Disease-East plots are **maple-beech-birch (MBB) forests**.
- MBB delta_temp = **+0.74 deg C** (n=3115). The whole signal sits in this one forest type.

This immediately raised a flag: MBB forests are precisely where **beech bark disease (BBD)** is most active. So the next question is which species inside MBB are doing the work.

---

## B6. Species-shift drilldown — script [07_disease_mbb_species_shift.R](../06_traits/scripts/07_disease_mbb_species_shift.R)

**Goal:** for MBB disease plots, identify which species are over-represented vs matched controls.

### Method (re-used in script 09 for fire)

For each plot, compute each species' **share** of all seedlings on that plot:

```
plot_share(species) = seedling_count(species) / total_seedlings_on_plot
```

Then average each species' share across all plots in the disturbed pool, and across all plots in the control pool. Difference = `delta_share`.

```
delta_share(species) = mean_share_disturbed - mean_share_control
```

### Why per-plot share, not total counts

- A plot with 1000 seedlings shouldn't drown out 100 plots with 10 seedlings each.
- Per-plot share makes every plot contribute equally.
- The community-shift question is fundamentally compositional (proportions), not absolute.

### What we found in MBB

| Species | delta_share |
|---|---|
| **American beech** | **+0.328** (54% of community vs 21% in controls) |
| Striped maple | +0.013 |
| (everything else) | tiny |

One species accounts for essentially the whole shift.

### Why beech increasing causes a "warming" signal

- American beech's `temp_mean` is 10.23 deg C — the warmest of the species commonly recruiting in northern hardwood forests.
- The species **losing** share are cooler-affinity (balsam fir 4.84, sugar maple 8.4, white pine 7.9, hemlock 7.8).
- So mathematically: more beech + less of the cool-conifer mix = higher CWM_temp. But ecologically this isn't climate-driven thermophilization — it's beech bark disease producing dense beech root sprouts.

---

## B7. Single-species artifact check — script [08_disease_mbb_excluding_beech.R](../06_traits/scripts/08_disease_mbb_excluding_beech.R)

**Goal:** test whether the +0.74 deg C signal survives if we remove the dominant species. If it does, there's a real climate signal underneath. If not, the signal *is* the species response.

### Method

Re-run the CWM calculation for MBB disease plots and their controls, but **exclude American beech (SPCD 531)** from both sides before computing CWM.

### Result

| Metric | With beech | Beech excluded |
|---|---|---|
| **delta_temp** | **+0.74 deg C** [0.69, 0.79] | **-0.17 deg C** [-0.23, -0.11] |
| **delta_cwd** | +2.0 mm | **-6.1 mm** |
| n plots | 3115 | 2755 |

The signal **flips** from warming to cooling. The +0.74 deg C "thermophilization" signal **was entirely beech**. 360 of the 3115 plots had to be dropped because they were 100% beech in the seedling layer (themselves diagnostic of BBD severity).

### Conclusion

The MBB Disease signal is a **beech bark disease pathology finding**, not a climate-driven thermophilization finding. It belongs in the BBD section of the writeup, not the climate section.

### The methodological lesson

> A single dominant species' response to its own pathogen can produce a large, statistically clean CWM signal that **looks like thermophilization but isn't**. Every positive cell needs the same single-species check before reporting.

That's the motivation for **script [09_fire_species_shift.R](../06_traits/scripts/09_fire_species_shift.R)** (next to run): apply the same species-shift analysis to fire-East and fire-West to see whether those signals are also dominated by one species (post-fire oak resprouting? aspen flushing? lodgepole serotinous reseeding?) or are broad community shifts.

### Decision tree: how to interpret a positive cell

```
Is the per-plot delta in this cell statistically reliable (CI excludes zero)?
   NO  -> not a finding
   YES -> Continue
       Run species-shift drilldown.
       Does one species account for > 50% of the positive shift mass?
           YES -> single-species artifact suspected
               -> Re-run CWM with that species excluded.
                   Does the signal survive (still positive, not flipped)?
                       YES -> real climate-driven shift on top of the species response
                       NO  -> the cell's signal IS the species response
                              (rebrand as e.g. a disease-pathology finding,
                               not as climate thermophilization)
           NO  -> broad community shift -> credibly climate-driven
```

---

# Cheat sheet

## File map

| Phase | Script | Output |
|---|---|---|
| A4 | [05_fia/scripts/06a_build_fia_site_list.R](../05_fia/scripts/06a_build_fia_site_list.R) | `all_site_locations.csv` (408K sites) |
| A4 | [05_fia/scripts/06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) | `site_climate.parquet` (1.95B rows) |
| A5 | [06_traits/scripts/01_build_species_climate_affinity.R](../06_traits/scripts/01_build_species_climate_affinity.R) | `species_climate_affinity.parquet` (603 species) |
| A6 | [06_traits/scripts/02_build_plot_recruitment_cwm.R](../06_traits/scripts/02_build_plot_recruitment_cwm.R) | `plot_recruitment_cwm.parquet` (475K conditions) |
| B1 | [06_traits/scripts/03_match_disturbed_to_controls.R](../06_traits/scripts/03_match_disturbed_to_controls.R) | `plot_matches.parquet` (~285K pairs) |
| B3 | [06_traits/scripts/04_stratified_thermophilization.R](../06_traits/scripts/04_stratified_thermophilization.R) | `thermophilization_by_class_region.parquet` |
| B4 | [06_traits/scripts/05_thermophilization_by_class_time.R](../06_traits/scripts/05_thermophilization_by_class_time.R) | `thermophilization_by_class_time_region.parquet` |
| B5 | [06_traits/scripts/06_disease_east_drilldown.R](../06_traits/scripts/06_disease_east_drilldown.R) | `disease_east_by_forest_type.parquet` |
| B6 | [06_traits/scripts/07_disease_mbb_species_shift.R](../06_traits/scripts/07_disease_mbb_species_shift.R) | `disease_mbb_species_comparison.parquet` |
| B7 | [06_traits/scripts/08_disease_mbb_excluding_beech.R](../06_traits/scripts/08_disease_mbb_excluding_beech.R) | `disease_mbb_excluding_beech_summary.parquet` |
| B7 | [06_traits/scripts/09_fire_species_shift.R](../06_traits/scripts/09_fire_species_shift.R) | `fire_species_shift_east.parquet`, `..._west.parquet` |

## Key numeric defaults

| Knob | Value | Where set |
|---|---|---|
| Climate baseline window | 1981–2010 | scripts 01 and 03 |
| Min species occurrences for CWM | 30 | script 02 (`MIN_OCCURRENCES`) |
| Match: K controls per disturbed | 5 | script 03 (`K_CONTROLS`) |
| Match: inventory year window | +/- 5 yr | script 03 (`INVYR_WINDOW`) |
| Match: climate caliper | 2.0 SD | script 03 (`CALIPER_SD`) |
| Population filter | `pct_forested >= 0.5` | classification module |
| Bootstrap resamples | 1000 | script 04 |

## Glossary (plain-language)

| Term | What it means |
|---|---|
| **Thermophilization** | Communities shifting toward warmer-climate species. The thing we're testing for. |
| **CWM** (community-weighted mean) | Average trait of a community, weighted by each species' abundance. |
| **Climate envelope** | The range of climate conditions a species is found in. Our proxy for its "climate preference." |
| **CWD** (climate water deficit) | A drought metric that combines water supply (precip) and atmospheric demand (PET). Higher = drier conditions for plants. |
| **Caliper** | The maximum allowed distance between a treated unit and its match. A "no match worse than this is acceptable" rule. |
| **delta_temp** | (disturbed CWM_temp) minus (matched control CWM_temp). Positive = warmer-affinity recruits on disturbed plot. |
| **Single-species artifact** | A pattern in the data that *mathematically* exists but is driven entirely by one species' response, so the ecological interpretation is wrong. The MBB / beech finding is the canonical example. |
| **Stable plot id** | `STATECD_UNITCD_COUNTYCD_PLOT` — the same id for the same physical plot across visits, even when FIA assigns a new visit-level `PLT_CN`. |

## Conceptual walkthrough: what happens to the data, in order

The join graph above shows **what tables exist and how they connect**. This walkthrough tells the same story as a **process** — what we start with, what each step does to the data, and the intuition for why. Each step opens with a one-line **Gist**; equations, classifications, decision rules, and worked examples are in their own labelled blocks so you can point at one when discussing with your boss.

---

### Step 1 — Start with raw FIA

> **Gist:** Begin with FIA's national plot inventory (~410K plots, revisited every 5–10 yr).

**Identifier formula:**

```
stable_plot_id = STATECD _ UNITCD _ COUNTYCD _ PLOT
```

- Each visit has a fresh `PLT_CN` (visit-level). The four-field concatenation above identifies a *physical* plot across visits.
- Tables in play: `PLOT`, `COND`, `TREE`, `SEEDLING`. Hundreds of millions of seedling records, billions of tree records.
- **Where we are:** raw tables. No climate, no ecology, no disturbance interpretation yet.

---

### Step 2 — Climate baseline at every plot

> **Gist:** Pull TerraClimate 1981–2010 and reduce it to one climate baseline number per plot.

**Variables (TerraClimate via Google Earth Engine):**

| Source | Used for |
|---|---|
| `tmmx`, `tmmn` | annual mean temperature |
| `pr`           | annual precipitation |
| `def`          | annual climate water deficit (CWD) |

**Aggregation formulas (per `stable_plot_id`):**

```
tmean_month      = (tmmx + tmmn) / 2
temp_annual      = mean( tmean_month, over 12 months )
precip_annual    = sum( pr,  over 12 months )
cwd_annual       = sum( def, over 12 months )

site_temp_mean   = mean( temp_annual,   1981-2010 )   # 30 yrs
site_precip_mean = mean( precip_annual, 1981-2010 )
site_cwd_mean    = mean( cwd_annual,    1981-2010 )
```

- 408K plot locations covered.
- **Why a fixed historical window:** the baseline represents "what climate the existing vegetation got established under." A moving baseline would conflate climate change with disturbance effect.

---

### Step 3 — Species climate fingerprint

> **Gist:** For each species, average the climate of every plot it lives at. That's its realized climate envelope.

**Method:**

1. Filter `TREE` to live trees (`STATUSCD == 1`).
2. Dedupe to one row per `(stable_plot_id, SPCD)` so revisits don't double-count.
3. Per species, take statistics across all plots it occurs at:

```
temp_mean(species)   = mean( site_temp_mean   over all plots where species occurs )
temp_p10(species)    = 10th percentile (...)
temp_p90(species)    = 90th percentile (...)
precip_mean(species) = mean( site_precip_mean ... )
cwd_mean(species)    = mean( site_cwd_mean    ... )
n_occurrences        = number of unique plots
```

- 603 species; 925,947 unique (plot, species) occurrences.
- **Intuition:** balsam fir lives at cold wet plots → low `temp_mean` and low `cwd_mean`. Live oak lives at hot dry plots → opposite. We use *where it grows* as a proxy for *what it tolerates*.

---

### Step 4 — Per-plot recruitment CWM

> **Gist:** Reduce each plot's seedling community to one number — the abundance-weighted climate they're adapted to.

**Formula (per condition, per climate variable):**

```
CWM_temp(plot)   = sum( seedling_count_i * temp_mean_i   ) / sum( seedling_count_i )
CWM_precip(plot) = sum( seedling_count_i * precip_mean_i ) / sum( seedling_count_i )
CWM_cwd(plot)    = sum( seedling_count_i * cwd_mean_i    ) / sum( seedling_count_i )
```

**Toy example:**

| Species     | Count | temp_mean |
|-------------|-------|-----------|
| Red maple   | 8     | 11.2      |
| Sugar maple | 2     | 8.4       |

```
CWM_temp = (8 * 11.2 + 2 * 8.4) / 10 = 10.64 deg C
```

**Trait quality filter:** drop species with `n_occurrences < 30` from the CWM (their fingerprints are too noisy). Median plot still keeps 100% of seedling abundance after filtering.

- 475K conditions get a CWM. One row per `(PLT_CN, INVYR, CONDID)` with `cwm_temp`, `cwm_precip`, `cwm_cwd`.

---

### Step 5 — Tag every condition

> **Gist:** Apply the A3 decision tree to label each condition `disturbed_candidate`, `control_candidate`, or excluded.

**Disturbance class codebook:**

| `DSTRBCD` value | Class | Notes |
|---|---|---|
| 30, 31     | fire    | surface/ground fire |
| 32         | fire    | crown fire — high-severity proxy |
| 10, 11, 12 | insect  | bark beetle, defoliators |
| 20, 21, 22 | disease | BBD, EAB, etc. |
| 50, 52, 54 | weather | wind/hurricane (52), drought (54) |
| 60–79      | other   | catch-all |
| 80         | excluded — human-induced |

**Gate (must pass all three):**

```
1.  is_forested_analysis_condition  := pct_forested >= 0.5
2.  no human/treatment              := DSTRBCD != 80  AND  all TRTCD == 0
3a. control candidate               := no DSTRBCD recorded (any natural class)
3b. disturbed candidate             := has natural DSTRBCD recorded
```

**Counts:**

- Disturbed candidates: ~57K conditions
- Control candidates: ~330K conditions

---

### Step 6 — Match each disturbed plot to controls

> **Gist:** For each disturbed plot, find K=5 controls with same forest type group + region + similar climate.

**Matching specification:**

| Setting | Value |
|---|---|
| Hard match (must equal) | `forest_type_group`, `region_east_west` |
| Time window             | `\|INVYR_dist − INVYR_ctrl\| <= 5 yr` |
| Climate distance metric | Euclidean in standardized (temp_z, prec_z) |
| Caliper (rejection)     | distance > 2 SD → match rejected |
| K (controls per disturbed) | 5 |
| Replacement             | yes (same control may serve multiple disturbed) |

**Distance formula:**

```
d(dist, ctrl)^2 = (temp_z_dist - temp_z_ctrl)^2 + (prec_z_dist - prec_z_ctrl)^2
                  where *_z are globally standardized site baselines
```

**Result:** ~285K (disturbed, control) pairs in `plot_matches.parquet`. Median match distance = **0.013 SD** (so the 2 SD caliper rarely fires).

- **Intuition:** because matched controls share forest type, region, and baseline climate, any leftover difference in seedling CWMs *can't* be explained by where the plot is. The leftover is what we attribute to the disturbance.

---

### Step 7 — Per-plot delta

> **Gist:** Subtract the average of a disturbed plot's 5 controls' CWMs from its own.

**Formula (per disturbed plot, per variable):**

```
plot_delta_temp   = dist_cwm_temp   - mean( ctrl_cwm_temp   over 5 matches )
plot_delta_precip = dist_cwm_precip - mean( ctrl_cwm_precip over 5 matches )
plot_delta_cwd    = dist_cwm_cwd    - mean( ctrl_cwm_cwd    over 5 matches )
```

**Sign convention:**

- `plot_delta_temp > 0` → disturbed plot's seedlings favor warmer-climate species than its climate-matched controls would. **The thermophilization signal at the plot level.**
- `plot_delta_cwd > 0` → favors more drought-tolerant species.

(In the data: `delta_cwm_*` is pre-computed per pair in `plot_matches.parquet`. The plot-level delta is the mean of those 5 per-pair deltas.)

---

### Step 8 — Aggregate with bootstrap CIs

> **Gist:** Average per-plot deltas inside `disturbance_class × region` cells; get 95% CIs by bootstrap.

**Aggregation:**

```
cell_mean_delta = mean( plot_delta over all disturbed plots in the cell )
```

**Bootstrap procedure (per cell):**

```
for b in 1..1000:
    sample disturbed plots in cell with replacement (size = original n)
    bootstrap_mean[b] = mean( plot_delta on the resample )

CI_lo = quantile(bootstrap_mean, 0.025)
CI_hi = quantile(bootstrap_mean, 0.975)
seed  = 42  (reproducibility)
```

**Decision rule:**

| CI bounds   | Reading |
|-------------|---|
| Both > 0    | Reliable warming/drying signal in this cell |
| Both < 0    | Reliable cooling/wetting signal |
| Crosses 0   | Inconclusive — could be zero |

**Sample-size warning:** ignore cells with `n < 100` unless effect is huge; CIs on tiny cells flip easily.

- **Why bootstrap, not a t-test:** controls are reused across pairs, plots cluster spatially. Bootstrap doesn't need independence assumptions.

---

### Step 9 — Drill into a suspicious cell by forest type

> **Gist:** When a cell shows a strong signal, re-aggregate inside it by `forest_type_group` to see if one type is doing all the work.

**Method:** filter `plot_matches` to the suspicious cell, then group `plot_delta_temp` by `forest_type_group` and re-bootstrap the cell mean for each subgroup.

**Decision rule:**

| Pattern in subgroups | Reading |
|---|---|
| Every forest type shows positive shift | **Broad** — credibly climate-driven |
| One forest type carries the cell's signal | **Narrow** — trigger Step 10 |

**Worked example (disease-East):** cell mean = +0.42 deg C. Inside the cell, MBB forests (53% of plots) showed +0.74 deg C; everything else was small. Triggered Step 10.

---

### Step 10 — Species shift inside the suspect forest type

> **Gist:** Compare per-plot species shares between disturbed and matched controls. Find the species with the biggest `delta_share`.

**Formulas:**

```
plot_share(species, plot)  = seedling_count(species, plot) / total_seedlings(plot)

mean_share_disturbed(spp)  = mean( plot_share(spp) over all disturbed plots in cell )
mean_share_control(spp)    = mean( plot_share(spp) over all matched control plots )

delta_share(spp)           = mean_share_disturbed(spp) - mean_share_control(spp)
```

**Why per-plot share, not absolute count:** every plot contributes equally regardless of total seedling load.

**Decision rule:**

| Largest delta_share | Reading |
|---|---|
| 1 species >> all others (e.g. > 5×) | Single-species artifact suspected → trigger Step 11 |
| Spread across many species          | Broad community shift → credibly climate-driven |

**Worked example (MBB disease):**

| Species | delta_share |
|---|---|
| American beech | **+0.328** (54% disturbed vs 21% control) |
| Striped maple  | +0.013 |
| (everything else) | tiny |

Beech was **25× larger** than the next biggest shift. Single-species artifact strongly suspected.

---

### Step 11 — Single-species sensitivity check

> **Gist:** Re-compute CWM for the same plots with the dominant species removed from both sides. Does the signal survive?

**Method:**

1. For each disturbed plot AND each of its controls, drop the dominant species (e.g. `SPCD == 531` for beech) before computing CWM.
2. Re-run Step 7 (per-plot delta) and Step 8 (cell mean + bootstrap CI) on the resulting deltas.

**Decision rule:**

| Result with dominant species excluded | Reading |
|---|---|
| Signal still positive (CI excludes 0) | **Real climate-driven shift on top of the species response.** Both findings count. |
| Signal close to 0                     | The cell's signal *was* the species. Not climate. Re-label as a species/pathology finding. |
| Signal flips sign                     | The cell's signal *was* the species, AND the rest of the community is moving the *opposite* direction. Definitely not climate. |

**Worked example (MBB beech):**

| Metric | With beech | Beech excluded |
|--------|------------|----------------|
| delta_temp | **+0.74** [+0.69, +0.79] | **−0.17** [−0.23, −0.11] |
| delta_cwd  | +2.0 mm                  | **−6.1 mm**              |
| n plots    | 3115                     | 2755 (360 dropped, 100% beech) |

Signal **flipped** → the +0.74 deg C disease-East signal was 100% beech bark disease, not thermophilization. Re-labelled as a BBD pathology finding.

---

### One-paragraph recap

We tag each species with a climate fingerprint based on where it lives (Steps 2–3). We score each plot's seedling community by averaging its species' fingerprints, weighted by abundance (Step 4). We classify which plots got hit by what disturbance and which are clean references (Step 5). For each disturbed plot we find five climate-matched control plots in the same forest type and region (Step 6) and compute the difference in their community climate scores (Step 7). We average those differences inside `disturbance × region` cells with bootstrap uncertainty bounds (Step 8). When a cell looks suspicious, we drill into it forest type by forest type (Step 9), then species by species (Step 10), and finally re-run the CWM excluding the dominant species to see whether the signal was climate or just one species responding to its own pathogen (Step 11). The whole pipeline is one long machine for asking: **"after we account for everything else, do disturbed plots recruit warmer-affinity seedlings than the otherwise-equivalent undisturbed plots — and is that signal real or driven by one species?"**

---

## Schema reference: data products and how they combine

Each downstream script reads from one or more of the products below. This section is a one-stop reference for **what each table contains, what its primary key is, what foreign keys it carries, and how it gets made**. Use it when you need to debug a join, write a new analysis script, or explain to someone why a particular column exists.

## Join graph

```
                ┌────────────────────────┐
                │ raw FIA extracts        │
                │ (cond / trees /         │
                │  seedlings, by state)   │
                │                         │
                │ supplies: PLT_CN,       │
                │   stable_plot_id, SPCD, │
                │   STATECD/UNITCD/       │
                │   COUNTYCD/PLOT,        │
                │   DSTRBCD*, FORTYPCD    │
                └─────┬────────┬──────────┘
                      │        │
                      │        │ stable_plot_id (= site_id)
                      │        ▼
                      │  ┌─────────────────────────────┐
                      │  │ site_climate.parquet         │
                      │  │ PK: (site_id, year, month,   │
                      │  │      variable)               │
                      │  │ Long monthly TerraClimate    │
                      │  └─────┬────────────────┬───────┘
                      │        │                │
                      │        │ baseline       │ baseline climate at
                      │        │ climate at     │ each occurrence plot
                      │        │ matching       │
                      │        │ stratum        │
                      │        │                ▼
                      │        │      ┌─────────────────────────────────┐
                      │        │      │ species_climate_affinity         │
                      │        │      │ PK: SPCD                         │
                      │        │      │ temp_mean, precip_mean,          │
                      │        │      │ cwd_mean, n_occurrences          │
                      │        │      └────────────────┬─────────────────┘
                      │        │                       │ joined to seedlings on SPCD
                      │        │                       ▼
                      │        │      ┌─────────────────────────────────┐
                      │        │      │ plot_recruitment_cwm.parquet     │
                      │        │      │ PK: (PLT_CN, INVYR, CONDID)      │
                      │        │      │ cwm_temp / cwm_precip / cwm_cwd  │
                      │        │      └────────────────┬─────────────────┘
                      │        │                       │
                      ▼        │                       │
   ┌──────────────────────────────────┐                │
   │ plot_disturbance_classification   │                │
   │ PK: (PLT_CN, INVYR, CONDID)       │                │
   │ disturbance_class,                │                │
   │ disturbed_vs_control,             │                │
   │ forest_type_group,                │                │
   │ region_east_west, stable_plot_id  │                │
   └──────────────┬───────────────────┘                │
                  │                                    │
                  │  inner join on (PLT_CN,            │
                  │  INVYR, CONDID); stratum keys      │
                  │  + climate caliper drive matching  │
                  │                                    │
                  ▼                                    ▼
              ┌──────────────────────────────────────────┐
              │ plot_matches.parquet                      │
              │ PK: (disturbed_id, control_id,            │
              │      match_rank)                          │
              │ Carries dist_* AND ctrl_* CWMs +          │
              │ pre-computed delta_cwm_temp / precip / cwd│
              └──────────────────┬───────────────────────┘
                                 │
                                 ▼
              read by every script 04-09 for stratified
              summaries, drilldowns, and species-shift checks
```

## Source FIA extracts (the raw inputs)

Not "final" products, but they're the source of every identifier downstream. Partitioned by state under `05_fia/data/processed/`.

| Extract       | Grain                                   | Key columns                                                                                                           | Supplies to downstream                           |
|---------------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `cond`        | `PLT_CN x CONDID`                       | PLT_CN, CONDID, stable_plot_id, FORTYPCD, DSTRBCD1-3, DSTRBYR1-3, TRTCD1-3, LAT, LON                                  | `stable_plot_id`, disturbance codes, forest type |
| `trees`       | `TRE_CN`                                | PLT_CN, SPCD, STATUSCD, DIA                                                                                           | live-tree occurrences for species range          |
| `seedlings`   | `PLT_CN x INVYR x CONDID x SUBP x SPCD` | PLT_CN, INVYR, CONDID, SUBP, SPCD, treecount_total                                                                    | seedling counts for CWM                          |
| `ref_species` | `SPCD`                                  | SPCD, COMMON_NAME, SCIENTIFIC_NAME, GENUS, SFTWD_HRDWD                                                                | species labels for output                        |

`stable_plot_id` is generated in the cond extract by concatenating `STATECD_UNITCD_COUNTYCD_PLOT` (see A2). Every downstream join to climate uses `stable_plot_id`.

---

## site_climate.parquet

- **Path:** `05_fia/data/processed/site_climate/site_climate.parquet`
- **Grain:** one row per (site, year, month, variable). Long format.
- **Primary key:** `(site_id, year, month, variable)`
- **Foreign key:** `site_id` = `stable_plot_id` (joins to cond / classification / any plot table)
- **Key columns:** `site_id`, `year`, `month`, `variable` (one of `tmmx`, `tmmn`, `pr`, `def`, `pet`, `aet`), `value`
- **Generated by:** [05_fia/scripts/06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) using Google Earth Engine + TerraClimate. Site list comes from [06a_build_fia_site_list.R](../05_fia/scripts/06a_build_fia_site_list.R) — 408,040 distinct `stable_plot_id`s with valid lat/lon.
- **Years:** 1958–2024 (no 2025–2026 yet from TerraClimate).
- **Size:** ~1.95B rows / 5.85 GB. Always read with `open_dataset()` + `filter()`, never `read_parquet()` whole.
- **Used by:** scripts 01 (species traits) and 03 (matching) — both aggregate to the 1981–2010 annual baseline before joining.

---

## species_climate_affinity.parquet

- **Path:** `06_traits/data/processed/species_climate_affinity.parquet`
- **Grain:** one row per species
- **Primary key:** `SPCD`
- **Foreign key:** `SPCD` joins to seedlings, trees, ref_species
- **Key columns:** `SPCD`, `COMMON_NAME`, `temp_mean`, `temp_p10`, `temp_p90`, `temp_min`, `temp_max`, same for `precip_*` and `cwd_*`, `n_occurrences`, `n_states`
- **Generated by:** [01_build_species_climate_affinity.R](../06_traits/scripts/01_build_species_climate_affinity.R). Joins live-tree records (`trees` filtered to `STATUSCD == 1`) to per-site baseline climate via `stable_plot_id`, dedupes to one `(stable_plot_id, SPCD)` per species, computes mean/p10/p90/min/max per species.
- **Rows:** 603 species; 925,947 unique (plot, species) occurrences.
- **Used by:** script 02 (CWM); also joined directly into the species-shift scripts (07, 09) for warm/cool tagging.

---

## plot_recruitment_cwm.parquet

- **Path:** `06_traits/data/processed/plot_recruitment_cwm.parquet`
- **Grain:** one row per forested condition visit
- **Primary key:** `(PLT_CN, INVYR, CONDID)`
- **Foreign key:** joins to `cond` and `plot_disturbance_classification` on the same triple
- **Key columns:** `cwm_temp`, `cwm_precip`, `cwm_cwd`, `n_species_total`, `n_species_with_traits`, `n_seedlings_total`, `n_seedlings_with_traits`, `frac_seedlings_with_traits`, `min_n_occurrences_used`
- **Generated by:** [02_build_plot_recruitment_cwm.R](../06_traits/scripts/02_build_plot_recruitment_cwm.R). Joins seedling counts to `species_climate_affinity` on `SPCD`, drops species with `n_occurrences < 30`, computes per-condition abundance-weighted means.
- **Rows:** 475,055 conditions.
- **Used by:** script 03 (matching) — both disturbed and control sides bring their `cwm_*` here, which become `dist_cwm_*` and `ctrl_cwm_*` in the matched output.

---

## plot_disturbance_classification.parquet

- **Path:** `05_fia/data/processed/summaries/plot_disturbance_classification.parquet`
- **Grain:** one row per condition visit
- **Primary key:** `(PLT_CN, INVYR, CONDID)`
- **Foreign keys:** joins to `cond` and `plot_recruitment_cwm` on the triple; carries `stable_plot_id` for the climate join
- **Key columns:** `stable_plot_id`, `forest_type_group`, `region_east_west`, `disturbance_class` (fire/insect/disease/weather/other), `disturbance_class_primary`, `is_high_severity_proxy`, `disturbed_vs_control` (one of `disturbed`/`control`/excluded), `is_control_candidate`, `is_natural_disturbance_candidate`, `disturbance_year_latest`, `time_since_disturbance`, `has_continuous_disturbance_year`
- **Generated by:** disturbance classifier modules under `05_fia/scripts/summaries/`. Rules: harvest/treatment excluded; natural-disturbance codes mapped to classes; controls = forested + no disturbance + no harvest.
- **Used by:** script 03 (matching) — defines the disturbed pool, the control pool, and the matching strata.

---

## plot_matches.parquet (the analysis-ready table)

- **Path:** `06_traits/data/processed/plot_matches.parquet`
- **Grain:** one row per matched (disturbed condition, control condition) pair. Each disturbed condition has up to 5 rows.
- **Primary key:** `(disturbed_id, control_id, match_rank)` where `disturbed_id = PLT_CN_INVYR_CONDID` of the disturbed side and `control_id` is the same string for the control side.
- **Foreign keys:**
  - `dist_PLT_CN`, `dist_INVYR`, `dist_CONDID`, `dist_stable_plot_id` -> disturbed-side condition
  - `ctrl_PLT_CN`, `ctrl_INVYR`, `ctrl_CONDID`, `ctrl_stable_plot_id` -> control-side condition
- **Key columns:**
  - **Match metadata:** `match_rank` (1–5), `match_distance` (climate distance in SD units), `forest_type_group`, `region_east_west`
  - **Disturbed-side context:** `disturbance_class`, `disturbance_class_primary`, `is_high_severity_proxy`, `disturbance_year_latest`, `time_since_disturbance`
  - **CWMs (both sides):** `dist_cwm_temp`, `ctrl_cwm_temp`, and same for `precip` and `cwd`
  - **Pre-computed deltas:** `delta_cwm_temp = dist_cwm_temp - ctrl_cwm_temp`, same for `precip` and `cwd`
  - **Quality:** `dist_n_seedlings`, `ctrl_n_seedlings`, `dist_frac_with_traits`, baseline climate z-scores `dist_temp_z`, `dist_prec_z`, `ctrl_temp_z`, `ctrl_prec_z`
- **Generated by:** [03_match_disturbed_to_controls.R](../06_traits/scripts/03_match_disturbed_to_controls.R). Inner-joins `plot_disturbance_classification` to `plot_recruitment_cwm` on `(PLT_CN, INVYR, CONDID)`, attaches baseline climate, then within each `(forest_type_group x region_east_west)` stratum finds the K=5 nearest controls in standardized climate space subject to the inventory-year window and 2 SD caliper.
- **Rows:** ~285,000 pairs (57,055 disturbed conditions × up to 5 matches each).
- **Used by:** every analysis script downstream (04 through 09). Scripts 04 and 05 average `delta_cwm_*` within strata; scripts 06–09 filter to specific cells (e.g. `disturbance_class == "disease" & region_east_west == "East" & forest_type_group == 800`) and either re-aggregate or jump back into seedlings to ask species-shift questions.

---

## How everything combines (one-paragraph summary)

Start with the FIA `cond` extract: it carries `stable_plot_id` (built from state + unit + county + plot fields, the same string across visits) and disturbance codes. **Climate** is keyed to the same `stable_plot_id` via TerraClimate, then aggregated two ways: per-species (using live trees from the `trees` extract) into **species_climate_affinity** keyed on `SPCD`, and per-plot baseline (using site means) into the matching step. **Seedlings** are joined to `species_climate_affinity` on `SPCD` and reduced to one **CWM** number per condition, keyed on `(PLT_CN, INVYR, CONDID)`. The **disturbance classifier** writes a parallel table on the same condition triple with class labels and control eligibility. Those two condition-grain tables are inner-joined to produce the matching pool, which is partitioned into disturbed and control halves and matched within `(forest_type_group x region_east_west)` strata using climate similarity. The output, **plot_matches**, carries both sides' CWMs and the pre-computed delta in a single row per matched pair — that's the table every downstream analysis reads.
