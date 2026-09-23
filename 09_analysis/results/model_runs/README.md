# Model run index

The repository keeps exactly one authoritative model-result directory. A verified rebuild overwrites that directory; Git preserves prior versions and the table below explains material historical states.

| Authoritative directory | Models | Responses | Groups | Predictors | Status |
|---|---:|---|---|---|---|
| `20260905_cumulative_mortality_site_cwd_no_seedlings_v01` | 9 | temperature, precipitation, CWD CWM change | saplings, adults, combined sapling-plus-adult community | fire, insect, disease cumulative mortality; cumulative site CWD; survey period | verified 1997-2025 authoritative rebuild |

Open `preliminary_results.html` for the model tables, raw relationships, and marginal effects. Open `robustness/robustness_results.html` for the focused stability and life-stage checks. Use `coefficients.csv` for model numbers and `input_manifest.csv` to compare exact inputs and software across machines.

## Version history

| Date | Commit | Role | Notes |
|---|---|---|---|
| 2026-09-22 | `f7a7f3f` | producing source | Rebuilt the single authoritative directory with deterministic reductions, the tracked 1997-2025 analysis window, and the verified local-NCSS TerraClimate cache. Exact input hashes and runtime are in the run manifests. |
| 2026-09-08 | `defd171` | authoritative baseline | Replaced the earlier seedling run with sapling, adult, and pooled-community results. |
| 2026-09-12 | `32c85e5` | audit branch only | Captured server outputs built from a TerraClimate cache ending in 2024. Diagnostic evidence, never an authoritative replacement. |

Do not add another dated result directory for a routine rebuild. Run the tracked orchestrator, verify the QA and sample counts, then commit the updated contents of the existing authoritative directory. Use `git show <commit>:<path>` or a temporary worktree when an older version is needed for comparison.
