# Forest Data Explorer Design

**Status:** Design recommendation for review  
**Target:** Smallest defensible release by September 15, 2026  
**Decision:** Extend the existing Streamlit dashboard with a guided extraction workflow backed by PyArrow scans of reviewed Parquet products. Do not add a database or a separate frontend.

## How evidence is labeled

This record distinguishes four kinds of statements:

- **Repository finding** — established by inspecting code, documentation, configuration, locally available products, schemas, or read-only checks in this repository.
- **Project deployment fact** — supplied by the project owner about the intended UCSB environment and treated as an implementation constraint that the environment spike must verify where practical.
- **Official FIA support** — supported by a cited USDA Forest Service FIA guide or tool specification.
- **Platform support** — supported by official documentation for Streamlit or the selected hosting environment.
- **Domain review** — an interpretation or methodological choice that still requires the researcher or an FIA subject-matter expert.

These labels describe the evidence, not the severity of a concern. Confirmed defects, contract drift, suspected linkage problems, methodological questions, and inherent data limitations are classified separately in [Risks and concerns](#risks-and-concerns).

## Executive recommendation

Build a researcher-facing **Explore and extract FIA conditions** workflow inside the existing Streamlit application. The first screen should ask what one output row should represent, then guide the user through plain-language concepts, filters, a preview, and downloads. The first release should support only a narrow, semantically reviewed condition-grain slice.

Use PyArrow to project columns and apply source-compatible filters before converting a bounded result to Pandas. Use Pandas only for small joins, reviewed derivations, preview, and export. Do not introduce DuckDB, PostgreSQL, a web API, a frontend build system, or a deployment service unless later measurements demonstrate a need.

Run the shared application backend in the environment that can read the products—preferably the UCSB server or a UCSB-managed host with the project storage mounted. Git carries application code, registries, tests, and documentation; it does not need to carry large raw or processed products. The browser receives previews and requested exports from the Streamlit backend. Streamlit Community Cloud is not the target because the ignored server products are not present in its GitHub-derived runtime.

The UCSB server is a **serving environment**, not the authoritative raw-data build environment. The project owner reports that it does not contain raw FIA tables. The application therefore reads only a versioned, validated **explorer release** published from a controlled build environment that has the archived raw snapshot. Raw FIA is neither required nor permitted as a silent runtime fallback. The release contains the narrow condition-grain Parquet product, required lookups, QA result, checksums, schema/grain contract, upstream acquisition receipt, and build provenance. A missing release makes the workflow unavailable; it does not trigger a live API query or download.

The raw snapshot used for an official explorer release must exist in durable project-controlled storage outside an individual workstation, even though it need not be mounted on the application server. Until that archive location and retention owner are identified, a locally built release can support development but should not be described as fully reproducible or operationally durable.

Keep the extraction engine independent of Streamlit and drive it with the same portable JSON request used by the interface. This provides a fallback request-builder mode when interactive hosting is unavailable: a disconnected interface may produce a validated request and command, but only an executor beside the data may claim current availability or create the export.

The first release is an extraction tool for repository observations and reviewed derived fields. It is **not** a replacement for FIA population estimation. Population estimates require evaluations, strata, expansion factors, and sampling-error procedures described by FIA.

## Intended user and success criteria

The primary user is a forest researcher who understands their research questions but should not need to understand repository directories, producer scripts, join keys, or FIADB storage conventions.

The September release succeeds when the researcher can:

1. Choose the condition-record workflow without selecting files or tables.
2. See an unambiguous statement that one row represents one FIA condition within one plot visit.
3. Filter a reviewed set of condition concepts using plain-language controls.
4. See the exact accessible-forest default and other enforced constraints.
5. Select scientific variables while required row identity and protocol context are included automatically.
6. Preview row counts, columns, caveats, and a bounded sample.
7. Download CSV or Parquet plus a JSON provenance manifest sufficient to reproduce the request.
8. Be prevented from requesting unsupported joins, aggregations, or population estimates.
9. See whether the required server products are available, partial, stale, incompatible, or missing before submitting an extraction.
10. Use the same portable request through the interface or a documented command-line executor without embedding machine-specific paths.
11. See the explorer release date, FIA source-snapshot date, geographic/year coverage, and validation status without needing access to raw FIA files.

## Repository findings

### Existing application and environment

- **Repository finding:** [`docs/dashboard/app.py`](dashboard/app.py) is an existing multi-page Streamlit application named "Forest Data Explorer." It already centralizes repository navigation and product status, but its product inventory is hard-coded and is not a safe extraction registry.
- **Repository finding:** `requirements.txt` already includes Streamlit, Pandas, and PyArrow. The recommended design adds no foundational runtime dependency.
- **Repository finding:** active workflows primarily publish Parquet summaries and state-partitioned Parquet datasets. This is a good fit for Arrow projection and predicate pushdown.
- **Repository finding:** several canonical climate and linkage outputs are absent or incomplete locally. They cannot be committed September workflows merely because their paths are documented.
- **Repository finding:** the dashboard currently resolves `REPO_ROOT` from the application location, checks a hard-coded `PIPELINE` list with file-existence calls, and reads Parquet row counts and schemas from metadata. This demonstrates basic runtime discovery but does not distinguish semantic registration from physical availability, completeness, schema compatibility, QA status, or freshness.
- **Repository finding:** repository documentation intentionally classifies many large products as local/scripted or server-mirror artifacts rather than Git-tracked files. GitHub therefore cannot be treated as the data distribution layer.
- **Project deployment fact:** the intended UCSB server does not contain the raw FIA tables used by the repository's producer scripts. Runtime extraction must therefore depend on explicitly published products rather than raw-table presence.
- **Platform support:** Streamlit Community Cloud creates a runtime from the GitHub repository and states that files needed locally by the app must be made available to that runtime. Files generated during a Community Cloud session are not guaranteed to persist. These constraints do not apply when Streamlit is run directly on a UCSB host with mounted project storage.

### Important locally observed FIA products

**This table is superseded.** Row counts, sizes, and grain verification for every product now live in [Master Product Inventory](MASTER_PRODUCT_INVENTORY.md), which is regenerated from the data by `python forest_explorer/catalog/build_inventory.py`. Consult that rather than the figures below, which were hand-recorded and have already drifted — the FIA rebuild of 2026-07-21 changed several of them (`plot_seedling_species` went from 2,772,452 to 2,859,609 rows, `plot_tree_metrics` from 491,182 to 492,318, `plot_seedling_metrics` from 415,019 to 428,391). The design dispositions in the last column remain the record of intent.

| Product | Local shape when this was written | Logical grain | Design disposition |
|---|---:|---|---|
| `plot_condition_metadata.parquet` | 1,472,276 rows; 60 MB | `PLT_CN × INVYR × CONDID` | Base candidate for the first slice, after its contract and protocol context are reconciled |
| `fia_condition_disturbance_flags.parquet` | 62 MB | condition within plot visit | Selected presence fields only; treatment timing blocked |
| `plot_tree_metrics.parquet` | 491,182 rows; 44 MB | plot visit | Future workflow |
| `plot_seedling_metrics.parquet` | 415,019 rows; 6 MB | plot visit | Future workflow |
| `plot_tree_species.parquet` | 3,789,663 rows; 118 MB | documented as plot-condition-subplot-species | Conditional on producer/product and aggregation review |
| `plot_sapling_species.parquet` | 1,323,449 rows; 46 MB | plot-condition-subplot-species | Conditional on aggregation review |
| `plot_seedling_species.parquet` | 2,772,452 rows; 60 MB | plot-condition-subplot-species | Conditional on protocol and aggregation review |
| `plot_disturbance_severity.parquet` | 1,217,271 plot visits | plot visit | Future; derived semantics need review |
| IDS cleaned GeoPackage | about 5.4 GB | damage area, damage point, or surveyed area | Not an interactive first-slice source |

**Repository finding (2026-07-22):** the declared grain of every product has now been checked against the data rather than assumed. All FIA products above are unique on their declared keys. Two IDS layers are not; see [Confirmed data or pipeline defect](#confirmed-data-or-pipeline-defect).

**Repository finding:** a projected and filtered PyArrow scan of the largest species product returned a recent California subset in under 0.2 seconds during a read-only feasibility check; the smaller products were faster. This supports direct Parquet access, but it does not justify loading national products into Pandas.

### FIA protocol context is missing from the current condition summary

- **Repository finding:** the raw 2000–2024 `PLOT` files contain 1,217,271 visits spanning `PLOT.MANUAL` 0.0 through 9.4.
- **Repository finding:** [`03_extract_trees.R`](../05_fia/scripts/core/03_extract_trees.R) currently selects `PREV_PLT_CN`, location, and `INVYR` from `PLOT`, but not `MANUAL`, `MEASYEAR`, `DESIGNCD`, or `REMPER`.
- **Repository finding:** the current `plot_condition_metadata.parquet` therefore cannot by itself enforce or explain all protocol-dependent restrictions.
- **Official FIA support:** `PLOT.MANUAL` identifies the field-guide version used to collect a plot. FIADB also distinguishes inventory year, measurement date, design, and remeasurement period. See *FIADB User Guides — Volume: Database Description, version 9.4, revision August 2025*.

The future implementation must provide a reviewed explorer input view that carries required protocol context from `PLOT` to the condition grain. This design task does not create that view or rebuild any product. No concept requiring missing context may be exposed before the context is available and validated.

## FIA semantic authority and certification

### Demand-driven scope

Semantic certification is an admission gate for proposed interface variables, not an exhaustive FIADB audit. Initial work covers only the narrow condition-grain slice. Additional variables are reviewed when a future workflow proposes to expose them.

The authority order is:

1. The FIADB Database Description matching the source schema.
2. The national field guide matching each record's `PLOT.MANUAL`.
3. An applicable regional guide when collection or code detail varies regionally.
4. Older official guides when historical records require them.
5. The Population Estimation User Guide, EVALIDator guide, and FIADB-API documentation for estimates, change, growth, removals, and mortality.

The current guide is useful context but is not applied retroactively to older records.

### Certification states

| State | Meaning | Interface behavior |
|---|---|---|
| `certified` | Definition, applicability, transformation, and allowed behavior are supported | Available |
| `certified_with_constraints` | Usable when an enforceable restriction is applied | Available only with the restriction displayed and written to provenance |
| `domain_review_required` | Official meaning is known, but the intended project interpretation or method is unresolved | Unavailable |
| `blocked_by_repository_defect` | A confirmed data or pipeline defect affects the value | Unavailable until repaired, rebuilt, and validated |
| `not_reviewed` | Not yet evaluated for an approved workflow | Unavailable |

`certified_with_constraints` is a usable state. A constraint may be a manual version, year, state, region, status, source availability, output grain, or required companion field. The request validator, not user memory, must enforce it.

### Initial condition-grain certification set

This is the proposed review set, not a claim that every row is already certified. Final status is assigned during the first implementation phase after source-to-output checks.

| Plain-language concept | Source fields | Expected status | Required interpretation or constraint |
|---|---|---|---|
| Plot visit and condition identity | `PLOT.CN → COND.PLT_CN`, `INVYR`, `CONDID` | `certified` | `CONDID` is unique within a visit, not persistent across visits |
| Administrative location | `STATECD`, `UNITCD`, `COUNTYCD`, `PLOT` | `certified_with_constraints` | Identifiers, not exact spatial coordinates; Alaska county summaries need documented care |
| Measurement and protocol context | `MEASYEAR`, `MEASMON`, `REMPER`, `MANUAL`, `DESIGNCD`, `PREV_PLT_CN` | `certified_with_constraints` | Must be carried from `PLOT`; applicability depends on source coverage |
| Approximate plot location | `LAT`, `LON`, `ELEV` | `certified_with_constraints` | Public coordinates are perturbed and may be swapped; display and record warning |
| Accessible forest land status | `COND_STATUS_CD` | `certified_with_constraints` | Default `COND_STATUS_CD = 1`; definition wording changed beginning with `MANUAL >= 6.0` |
| Share of plot in the condition | `CONDPROP_UNADJ` and its basis fields | `certified_with_constraints` | Within-plot unadjusted proportion, not population area; do not use as an expansion factor without a reviewed method |
| Calculated forest type | `FORTYPCD` plus lookup | `certified_with_constraints` | Label as calculated forest type and retain the raw code |
| Condition disturbance observations | `DSTRBCD1-3`, optionally `DSTRBYR1-3` | `certified_with_constraints` | Condition-level observation with documented area, prevalence, and lookback rules; years require manual-specific review |
| Condition treatment presence | `TRTCD1-3` | `certified_with_constraints` | Prescribed silvicultural treatment, normally at least one acre; retain raw slots and reviewed labels |
| Treatment timing | `TRTYR1-3` and derived timing fields | `certified_with_constraints` | Defect repaired 2026-07-21; `treatment_year_status` must accompany any year so missing, continuous, and post-inventory values are not read as calendar years |
| Forested share of a plot visit | repository `pct_forested` | `certified_with_constraints` | Derivation must equal the sum of `CONDPROP_UNADJ` where `COND_STATUS_CD = 1`; record formula |

### Related FIA concepts that must remain distinct

- **Official FIA support:** `COND.DSTRBCD1-3` are condition-level disturbance observations with minimum area and damage/effect rules.
- **Official FIA support:** `TREE.AGENTCD` is primarily a cause-of-death code for qualifying tree records and has important pre-national-protocol differences.
- **Official FIA support:** `TREE.DAMAGE_AGENT_CD1-3` describes observed live-tree damage agents using a regionally extended PTIPS/FHAAST system with agent-specific thresholds.
- **Repository finding:** the repository also contains external IDS detections and a scaffold for MTBS/IDS association.

The interface must not collapse these into one generic disturbance measurement. Shared labels such as insect, disease, fire, or weather may be used only in an explicitly documented broad-category crosswalk that retains source concept and grain. Such a crosswalk does not establish measurement equivalence or causality.

## High-priority workflows

### September: explore and extract FIA conditions

1. **Researcher question:** Which reviewed FIA conditions match my geographic, temporal, forest-context, disturbance, or treatment-presence criteria?
2. **Inputs:** state/county, inventory or measurement period, accessible-forest default, calculated forest type, reviewed condition-disturbance categories/codes, and treatment presence.
3. **Output grain:** one condition within one plot visit: `PLT_CN × INVYR × CONDID`.
4. **Sources:** a validated condition-grain explorer view built from `PLOT`, `COND`, reviewed lookups, and approved condition-level derivations.
5. **Joins:** structural `PLOT.CN = COND.PLT_CN`; lookup joins must be many-to-one.
6. **Transformations:** reviewed labels, forested-share derivation, presence flags, and enforced certification constraints.
7. **Result:** analysis-ready condition records with automatic identity/protocol columns, selected scientific variables, raw codes where useful, and a manifest.
8. **Caveats:** approximate coordinates, manual-dependent definitions, within-plot proportions, and no population-estimate interpretation.
9. **Performance:** expected to be interactive with Arrow pushdown against the current approximately 60 MB condition summary or its reviewed successor.
10. **September status:** committed vertical slice.

When a reviewed plain-language concept is selected, its official raw code or slot should be preserved in the export when it aids auditability. The primary controls and preview use reviewed labels and definitions; the user is not required to know raw field names.

### Useful but conditional: plot-condition-species composition

This workflow would return species abundance for a condition after combining subplot observations. It is valuable, but not committed until the producer/product contract, expansion-factor interpretation, subplot aggregation, and condition-proportion method are certified. If those gates are not closed, it is deferred without weakening the condition slice.

### Future workflows

- Plot-visit tree and seedling structural summaries.
- Reviewed species-composition and climate-affinity extracts.
- Repeated-visit descriptive change using validated remeasurement links.
- Mortality and removals with correct GRM temporal and estimation semantics.
- Population estimates through an explicitly designed EVALIDator/FIADB-API workflow.
- Approximate external disturbance association with a reviewed spatial methodology.
- Loading normalized requests as saved presets.

## Unsupported or unsafe workflows for September

- Population totals, rates, or sampling errors from local plot summaries.
- Any IDS acreage total, until the duplicated damage records are rebuilt out.
- Joining conditions across visits on `CONDID`.
- Pairing visits solely by composite `stable_plot_id` and chronological lag.
- Combining condition disturbance, `AGENTCD`, tree damage agents, IDS, or MTBS as equivalent observations.
- Inferring that a condition disturbance or external detection caused a particular tree's damage or death.
- Calling `DSTRBCD = 32` “high-severity fire” without an approved project definition.
- Exact point-in-polygon exposure claims using public FIA coordinates.
- Interactive scans of the 5.4 GB IDS GeoPackage.
- Arbitrary file selection, free-form SQL, or a universal join builder.

## Interface structure

### 1. Start screen

Keep repository navigation and the existing explanatory dashboard separate from a prominent **Create a data extract** action. The first extraction decision is the reviewed workflow/output grain, not a file or data family. September offers one production option: **FIA conditions — one row per condition in a plot visit**. Unavailable workflows may be listed as “not yet reviewed” only when that helps set expectations.

### 2. Grain and meaning

Before filters, show:

> One output row represents one mapped FIA condition within one plot visit. Conditions can occupy different shares of a plot and their identifiers do not persist reliably across visits.

Also show source products, coverage, certification status, and the observation-versus-population-estimate boundary.

In connected mode, also show the promoted explorer release ID, publication date, raw FIA snapshot/acquisition date, state/year coverage, and validation status. Do not show private server or archive paths.

### 3. Filters

Use reviewed plain-language controls grouped as location, time/protocol, forest context, condition disturbance, and treatment presence. Keep an expandable “Official FIA details” panel for raw fields, definitions, thresholds, guide citations, and code lists.

The default forest control must visibly state:

> Accessible forest land conditions only: `COND_STATUS_CD = 1`

Users may change a non-mandatory default only if the workflow certifies the alternative. Every applied default is part of the normalized request and manifest.

### 4. Variables

Separate three groups:

- **Scientific variables** — user-selected reviewed concepts.
- **Row identity** — automatic grain-specific identifiers; visible but not removable.
- **Protocol context** — automatic fields required to interpret selected concepts.

Selecting a constrained variable displays the restriction inline. The application enforces it rather than merely warning after export.

### 5. Review and preview

Show the normalized request, exact output grain, pinned release ID, source-snapshot date, estimated/actual row count, applied defaults, enforced constraints, selected and automatic columns, warnings, and a bounded preview. Empty results should explain which filters produced the empty intersection and allow the user to return to selections.

### 6. Download

Provide:

- CSV for general analysis use.
- Parquet for typed, efficient reuse.
- JSON manifest for provenance and request replay.

Preset loading and generated R/Python/SQL are deferred. The normalized request is intentionally preset-ready.

## Architecture decision

| Option | Strengths | Costs and risks | Decision |
|---|---|---|---|
| Existing Streamlit + PyArrow/Pandas, run beside the data | Reuses the application and dependencies; browser users need no data copy; direct read-only access to server Parquet; strong projection/filtering | Requires a UCSB-approved launch, access, and export path | **Recommended connected mode** |
| Portable request + command-line executor | Works when a web process cannot be hosted; same validation and manifest semantics; easy to test | Not immediate for the researcher; availability snapshots can become stale | **Required fallback, using the same extraction engine** |
| Streamlit Community Cloud with GitHub-only files | Easy public deployment | Ignored server products are absent; generated files are ephemeral; moving large data to Git/LFS does not address governance or local derived products | Reject for the September data-bearing workflow |
| Hosted Streamlit plus object storage | Decouples compute from a specific server and can scale | Requires data publication, credentials, storage lifecycle, transfer cost, and governance decisions | Defer unless UCSB hosting is unavailable and approved storage exists |
| Streamlit + embedded DuckDB | Convenient SQL and multi-Parquet joins; no server | New query layer and dependency; does not solve semantic safety; unnecessary for first-slice access patterns | Defer unless measured joins justify it |
| Separate frontend and API | Maximum UI flexibility and clearer service boundary | Largest implementation, packaging, testing, and handoff burden; poor September fit | Reject for this project phase |
| Live FIADB-API queries for row extraction | Avoids holding a local FIA snapshot | FIADB-API is designed primarily for EVALIDator-style population estimates; it does not reproduce repository transformations or non-FIA products and makes availability/reproducibility depend on a changing external service | Reject as the condition-row data source; retain for a future population-estimation workflow |

“Run beside the data” is the controlling rule. During development that may mean `streamlit run docs/dashboard/app.py` in a populated local checkout. For shared use it should mean the same command, or a supervised equivalent, on the UCSB server or a UCSB-managed application host with read-only access to the product tree. No server database or separate application API is required.

The extraction engine must be a normal Python module callable by both Streamlit and a thin command-line entry point. Streamlit owns interaction and presentation; the engine owns request normalization, registry validation, availability checks, Arrow scans, limits, export, and manifest creation. There must not be two implementations of extraction semantics.

### Deployment topology

```text
Offline build and publication

Durably archived raw FIA snapshot in the controlled build environment
        |
        v
Pinned repository producers + semantic checks + product QA
        |
        v
Immutable explorer release in staging
  condition Parquet + lookups + manifest + QA + checksums + _SUCCESS
        |
        v
Verified transfer to UCSB staging ----> atomic promotion of release pointer


Connected mode

Researcher's browser
        |
        v
Authenticated/protected Streamlit process on UCSB infrastructure
        |
        +----> Git-tracked semantic registry and workflow definitions
        |
        +----> runtime product inventory
        |
        v
Read-only PyArrow scans of UCSB-hosted Parquet products
        |
        v
request-scoped export staging ----> browser download + JSON manifest


Fallback request-builder mode

Disconnected UI/docs ----> portable request.json + exact executor command
                                      |
                                      v
                    command-line executor beside UCSB data
                                      |
                                      v
                         export + execution manifest
```

Connected mode is the target experience for the boss. The server reads the promoted explorer release and never needs the release's raw FIA files. Request-builder mode is operational resilience, not a second scientific workflow. A disconnected interface may show the last published availability snapshot with its generation time and environment label, but it must not describe that snapshot as live.

### Data flow

```text
Reviewed workflow + semantic registry
          |
          +----> runtime inventory: path, schema, partitions, fingerprint, QA
          |
          v
Normalize and validate request ----> reject incompatible/unreviewed selections
          |
          v
PyArrow scan: project columns + push source predicates
          |
          v
Bounded Pandas joins/derivations, only when required
          |
          +----> preview and diagnostics
          |
          v
CSV / Parquet + JSON manifest
```

### Data-root and path resolution

Code and data locations must be configurable without changing registry entries:

1. Registry paths are logical paths relative to a named data root; they never contain a developer home directory, server hostname, drive letter, or secret.
2. The default data root is the repository root, preserving current populated-checkout behavior.
3. A deployment setting such as `FOREST_DATA_ROOT` may point to a UCSB project tree or mounted storage when code and products are separated.
4. The resolved path must remain within its configured root after normalization; user input never contributes path components.
5. Startup diagnostics show the non-sensitive environment label and whether roots are readable/writable, but browser-visible manifests do not expose private absolute server paths.
6. Products may be read through approved symlinks or mounts only when the resolved target is inside an administrator-approved root.

The first server spike must choose between a populated Git working tree, a separate versioned code checkout plus data root, or a mounted product tree. The second arrangement is operationally safer because code updates and data lifecycle are separate, but the design supports the first to minimize September setup.

### Build, release, and promotion contract

The application product is not an arbitrary copy of whichever local Parquet happens to exist. It is an immutable release created and admitted through a controlled publication boundary.

#### Environment responsibilities

| Environment | Required inputs and permissions | Responsibilities | Prohibited responsibilities |
|---|---|---|---|
| Build environment | Read access to one dated raw FIA snapshot; pinned code and dependencies; write access to build staging | Build the reviewed condition-grain product, run semantic/structural QA, create provenance and checksums, and package a release | Serving unvalidated working files as production products |
| Durable raw archive | Project-controlled storage with documented retention and restricted access | Preserve the exact raw snapshot or source archives and acquisition receipt needed to investigate or reproduce a release | Interactive extraction and public download |
| Transfer/promotion process | Read access to completed build release; write access to UCSB staging and promotion metadata | Copy, verify, and atomically promote a complete release; retain or identify a rollback release | Modifying Parquet contents during transfer or promoting partial releases |
| Streamlit serving environment | Read-only access to promoted releases and registry; limited write access to export/cache roots | Inventory and validate the promoted release, execute bounded requests, and create request-scoped exports | Downloading raw FIA, rebuilding products, repairing data, or changing the promoted release |

The build environment may initially be the populated local repository, but the raw snapshot must not remain solely on one laptop. Before a release is considered operationally reproducible, the project must identify a durable UCSB-managed archive, project storage allocation, or other approved preservation location. The serving host may remain raw-free.

#### Explorer release contents

Use a release directory or equivalent immutable prefix such as:

```text
fia-condition-explorer/
  releases/
    2026-08-15.1/
      data/conditions/                 # Parquet file or state-partitioned dataset
      lookups/                         # only required reviewed code/label tables
      product_manifest.json
      validation_report.json
      checksums.sha256
      _SUCCESS
  current                              # administrator-controlled pointer or release ID
```

The physical path and naming convention are implementation decisions, but the contract is not. Each release must include:

- a unique immutable release ID; creation time; release status; and compatible registry/workflow versions;
- the condition-grain dataset and only the small lookups required by that workflow;
- the declared output key, row meaning, Arrow schema, partition coverage, row count, and byte size;
- producer Git commit, producer/configuration versions, dependency versions, and exact build command or normalized build parameters;
- one upstream receipt per raw state/table or source archive: DataMart URL or distributor identifier, retrieval time, filename, byte size, and cryptographic hash;
- the FIADB documentation/database version known at acquisition, plus observed `PLOT.MANUAL` coverage; these are different version concepts and must not be collapsed;
- semantic, schema, grain, identifier, coverage, default-filter, raw-code, and defect-exclusion QA results;
- content checksums for every published file and a terminal success marker written only after all validation passes.

The raw source files themselves are not copied into the explorer release and are not downloadable through the app. The upstream receipt links the release to the separately preserved raw snapshot.

#### Publication and rollback

1. Acquire or select an immutable raw FIA snapshot in the build environment. Do not mix state/table downloads from undocumented refresh dates in one release.
2. Run pinned producers into a new staging directory; never build in the currently served release.
3. Run all release-blocking QA and create the release manifest and checksums.
4. Mark the staged release complete only after every required check passes.
5. Transfer the complete release to a new UCSB staging location using a resumable approved mechanism.
6. Recompute or verify checksums on the destination and validate schema, coverage, manifest compatibility, and `_SUCCESS` without reading from an old release.
7. Promote atomically by changing a small administrator-controlled release pointer. Never replace files underneath a running release.
8. Refresh the runtime inventory. Existing requests either finish against the old release or start against the new one; one execution may not mix releases.
9. Retain at least the immediately previous compatible release, subject to storage policy, so promotion can be rolled back by changing the pointer rather than rebuilding.

An application request records the promoted release ID and product fingerprints. Repeating a request against a later release is a new execution and may yield different rows; the interface must make that distinction visible.

#### Server without raw FIA

Absence of raw FIA on the UCSB server is acceptable and preferable for least-privilege runtime operation when all of the following are true:

- the promoted explorer release is present, complete, compatible, and read-only;
- the release manifest identifies and hashes the raw snapshot used to build it;
- that exact raw snapshot or its original archives are preserved in durable project-controlled storage;
- the server can verify the published product without contacting DataMart;
- maintainers have a documented refresh path that starts in the build environment.

If the server has processed repository files but no conforming release manifest, checksums, QA report, and success marker, those files are **unmanaged candidates**, not production explorer inputs. They may be inspected during the environment spike but must be rebuilt or explicitly packaged and validated before promotion. If neither a valid release nor a durable source snapshot exists, the September workflow is blocked rather than silently reconstructed from the live FIADB API.

### Data-access rules

1. Resolve paths only through the product registry and configured data roots; the user never enters a path.
2. Check product existence, completeness, schema, fingerprint, QA status, freshness policy, and certification compatibility before execution.
3. Project only required output, identity, filter, join, and provenance columns.
4. Apply every filter expressible against a source dataset in the PyArrow scan.
5. Convert only the narrowed table to Pandas.
6. Estimate or check rows and memory before bounded joins; fail with a useful message when a limit is exceeded.
7. Never silently fall back to loading a full national product.
8. Keep raster processing, GeoPackage-wide spatial processing, climate extraction, and expensive scientific transformations offline.
9. Open registered scientific products read-only; the interactive process must not rebuild, repair, or mutate them.
10. Treat a missing or incompatible product as an availability failure with a maintainer-oriented producer command, not permission for an automatic download or rebuild.
11. Pin one release ID for the entire request so an inventory refresh or promotion cannot mix files from two releases.
12. Accept only products admitted by the release contract; nearby unmanifested Parquet files are not implicitly available.

### FIA DataMart and FIADB-API boundary

- The repository's deliberate bulk-ingestion workflow remains the source of raw FIA snapshots. `rFIA::getFIA()` and FIA DataMart downloads run offline from the interactive application, after which producers and QA create reviewed products.
- Because the serving server has no raw FIA, refreshes begin in the controlled build environment and reach the server only through verified release publication. The application does not need DataMart network access.
- The application records the product fingerprint and source/rebuild provenance. It does not silently refresh FIA between preview and export or between nominally identical requests.
- FIADB-API `/fullreport` is reserved for a future workflow that explicitly implements official population-estimation semantics, evaluation selection, groupings, sampling errors, and API-response provenance.
- A live FIADB-API result may never be substituted for a missing condition-row product because it has a different purpose and cannot reproduce repository transformations, certification, or non-FIA joins.
- If an administrator later adds an on-demand DataMart refresh action, it must be a separate authenticated maintenance process with locking, staging, validation, and atomic promotion—not a researcher-facing button.

### Access control, concurrency, and export lifecycle

The application must inherit an approved UCSB access boundary rather than invent authentication. Acceptable initial patterns include access through an institutional VPN plus a localhost/SSH tunnel, or a UCSB-managed reverse proxy that supplies authentication and TLS. Binding an unauthenticated development server to a public interface is not acceptable. The environment spike must identify the actual owner and approved pattern before the boss receives a shared URL.

The Streamlit process should have:

- read-only permission on registered source products and registries at runtime;
- write permission only to a dedicated request-scoped export root and application cache;
- no permission to overwrite producer outputs, raw FIA files, or repository code;
- no interface for arbitrary paths, SQL, Python/R expressions, shell commands, or uploads interpreted as requests without schema validation;
- bounded concurrent work, with a clear “busy/try again” response rather than uncontrolled duplicate national scans.

Each execution receives a generated request ID and a private staging directory. Data and manifest files are written completely before being offered for download. Incomplete files are removed on failure. Successful staged exports receive a configured time-to-live and are deleted by a documented cleanup job; the UI states that retention window. File names derive from controlled workflow metadata and the request hash, not raw user text.

The September release should default to browser download for bounded results. If a permitted export is too large for a reliable Streamlit download, the executor must fail safely with a narrower-filter recommendation unless UCSB approves a server-side delivery location. Emailing attachments, creating public links, or writing into arbitrary shared folders is not inferred authorization.

Logs record request ID, workflow/version, request hash, product fingerprints, status, timings, row counts, byte counts, and non-sensitive error categories. They do not record credentials, private paths, or exported row contents. Public FIADB coordinates remain approximate and are governed by the interface caveats already specified; confidential true FIA coordinates are outside the system boundary.

## Logical metadata contracts

The physical registry location remains an implementation-spike decision. The spike must test Python imports, test discovery, packaging, dashboard launch from the repository root, and the ability to generate documentation without duplicating curated facts. Regardless of location, one logical registry is authoritative.

### Product registry

Each product entry needs:

- stable product identifier and reviewed display name;
- logical data-root ID, relative path pattern, format, partitioning, and completeness rule;
- required release-artifact role and compatible release-contract versions;
- producer, upstream dependencies, and source fingerprint method;
- observed schema and logical grain;
- identifying and spatial/temporal fields;
- safe joins with cardinality and relationship type;
- certification/caveat references;
- operations suitable for interactive access.

Observed file metadata such as columns, Arrow types, size, row count, partitions, and modification fingerprint should be generated. Display names, scientific definitions, grains, join meaning, aggregation, certifications, and caveats must be curated.

### Runtime availability inventory

The semantic registry describes what the application knows how to use; the runtime inventory describes what is physically usable in one environment at one time. They must not be collapsed into one file or one Boolean.

For each registered product, the inventory records:

- environment label and inventory generation time;
- promoted release ID, release-manifest fingerprint, and promotion time;
- resolved logical root and product ID, without publishing a private absolute path;
- status: `available`, `partial`, `stale`, `incompatible`, `missing`, `unmanaged`, or `blocked`;
- discovered files or partitions, including state/year coverage where declared;
- Arrow schema and schema-contract result;
- row count, byte size, modification time, and reproducible content or metadata fingerprint;
- producer/QA marker and the freshness rule evaluated;
- release completeness, checksum, registry-compatibility, and success-marker results;
- a concise reason and maintainer action when the product is not available.

`available` means the promoted release, required files, partitions, schema, checksums, QA marker, success marker, registry compatibility, and applicable freshness rule satisfy the product contract. The lack of raw FIA on the serving server does not make a valid published product unavailable. `partial` can support extraction only when the workflow explicitly permits partial coverage, displays that coverage before selection, constrains the request to it, and records it in provenance. `stale`, `incompatible`, `missing`, `unmanaged`, and `blocked` products cannot execute a September workflow.

Connected mode generates or refreshes this inventory from the live data roots at startup and after an administrator-directed refresh. Expensive content hashing is not performed on every Streamlit rerun; inventory results are cached against stable file metadata and registry version. The executor revalidates every referenced product immediately before extraction so a cached UI status cannot authorize a vanished or changed input.

A compact, non-sensitive `available_products.json` may be exported for request-builder mode. It must carry the environment label, timestamp, registry version, product fingerprints, coverage, and an explicit `live: false` marker. It is informative until the server executor performs authoritative validation.

### Variable registry

Each exposed variable or concept needs:

- display label and plain-language definition;
- source table/field and native grain;
- official definition and exact documentation/version;
- collection applicability and code system;
- temporal meaning;
- allowed filters, grouping, joins, and aggregation;
- compatible output grains;
- manual, regional, state, or historical variation;
- repository transformation;
- certification state and enforceable constraints;
- scientific caveats and domain-review record;
- raw code fields to preserve in exports.

### Guided workflow registry

Each workflow declares its row meaning, output key, allowed concepts and filters, automatic identity/protocol fields, source products, joins, reviewed transformations, defaults, limits, warnings, and export formats. A workflow can reference only certified or enforceably constrained variables.

### Request and manifest

The versioned portable `request` contains workflow ID/version, output grain, filters including defaults, selected concepts, export options, and enforced constraint selections. It contains no machine-specific paths, credentials, arbitrary SQL, executable code, or user-controlled filesystem locations.

The `execution` manifest contains timestamp, non-sensitive execution-environment label, execution mode, application/registry versions, availability-inventory version, promoted release ID and release-manifest fingerprint, resolved product IDs and fingerprints, source schemas and partition coverage, raw-snapshot acquisition receipt identifiers/hashes inherited from the release, documentation versions, normalized filter expressions, automatic columns, enforced constraints, derivations, warnings, output schema/grain, row count, and request hash. It must not disclose credentials, private absolute paths, or private archive locations. This split permits later preset loading without pretending an old product fingerprint or availability snapshot is portable.

The command-line executor accepts only a request file and administrator-configured environment settings. It returns the same normalized request, data, warnings, and manifest as connected mode. Given the same request and the same product fingerprints, the two modes must pass byte-equivalent Parquet or schema/value-equivalent CSV tests, allowing only declared nondeterministic manifest fields such as execution time.

## Safe joins, aggregations, and relationship language

Every proposed relationship must be classified:

| Relationship type | Meaning | Example |
|---|---|---|
| Structural database relationship | Key defined by FIADB structure | `PLOT.CN = COND.PLT_CN`; current `PLOT.PREV_PLT_CN` to previous `PLOT.CN` |
| Observational containment | A record was observed within a containing sampling unit | A tree assigned to a condition and subplot in a plot visit |
| Scientifically justified aggregation | A documented transformation to a coarser grain | Summing condition proportions to an audited plot forest share |
| Broad-category crosswalk | Different concepts share a reviewed reporting category | Condition insect disturbance and tree insect cause retained as separate measures |
| Causal interpretation | One observation is said to cause another | Not allowed without supporting FIA documentation or approved project methodology |

Structural compatibility does not by itself justify aggregation or causal interpretation.

### Condition and plot rules

- A condition joins to its plot visit through `PLT_CN`; `CONDID` completes the condition key within that visit.
- `CONDID` may change at remeasurement. Condition-area change must use `SUBP_COND_CHNG_MTRX` or another reviewed method, not equality of `CONDID`.
- `CONDPROP_UNADJ` describes the unadjusted share of a plot in a condition. The sum over conditions is expected to be one, subject to source validity; population area requires additional estimation machinery.
- Plot-level categorical fields may not be duplicated across conditions and then summed.

### Tree, sapling, and seedling rules

- `TPA_UNADJ` is an unadjusted sample-design expansion and must not be presented as a population estimate.
- Removal of `SUBP` is allowed only after the measure-specific rule is documented for the relevant sample element.
- Raw seedling counts require protocol review because older manuals may record or calculate “six or more” differently.
- Condition-proportion weighting and combination of life stages are domain-review questions, not assumed rules.

## Risks and concerns

### Confirmed data or pipeline defect

**`TRTYR3` Boolean corruption — repaired 2026-07-21.**

- **Repository finding:** state partition schema inference produced integer partitions where values exist and Boolean all-null partitions elsewhere. National collection used a Boolean schema and coerced 5,403 real third-slot treatment years to `TRUE`/`1`.
- **Official FIA support:** FIADB defines `TRTYR1-3` as four-digit numeric treatment-year attributes.
- **Repository finding (2026-07-22):** the defect is no longer present. `TRTYR3` is `int32` in all fifty state condition partitions and in `plot_condition_metadata.parquet`, and holds 5,403 four-digit years spanning 1986–2025 with no value of `0` or `1` — the same 5,403 records previously reported as coerced. `plot_treatment_history.parquet` gained `TRTYR_raw`, `TRTYR_calendar`, and `treatment_year_status`, the last separating `calendar_year` (81,436), `missing` (9,025), `after_inventory_year` (1,360), and `continuous_or_unknown` (1). The rebuild is recorded in `scratch_output/fia_fix_temp/rebuild_manifest.json`.
- **Disposition:** the block is lifted for the values themselves. Treatment timing may be reconsidered for exposure, provided `treatment_year_status` is carried alongside any year so that missing, continuous, and post-inventory values cannot be read as calendar years. The rebuild has not itself been through release QA, so this is a finding about the current local build, not a promotion decision.

**IDS identifiers are not unique in the built GeoPackage.**

- **Repository finding (2026-07-22):** `damage_areas` holds 3,500 rows that repeat another row across 3,498 identifier groups, so `OBSERVATION_ID × DAMAGE_AREA_ID` is not unique. Geometry is identical within every group; attributes agree in all but 6; 3,295 groups have each copy from a different regional archive. `damage_points` has one such pair. The cause is `bind_rows()` over overlapping regional geodatabases in `03_clean_ids.R` with no de-duplication step.
- **Why it matters more than the row count suggests:** the repeats are 0.08% of rows but the duplicated polygons average 12,869 acres against an overall mean of 131, so reported acreage is inflated by 7.66% (45,017,025 of 587,582,463 acres).
- **Disposition:** fixed in `scripts/utils/ids_dedupe.R`, called from `03_clean_ids.R`, with unit tests in `01_ids/tests/testthat/test_ids_dedupe.R`. The GeoPackage in the current data root predates the fix and still contains the duplicates; rerunning the producer should yield 4,472,333 rows and 542,565,563 acres and leave the 6 genuine conflicts in a QA file for review. Any IDS workflow that sums acreage is blocked until the layer is rebuilt.

### Producer/product contract drift requiring technical investigation

**Tree condition/subplot grain.**

- **Repository finding:** the current tree producer reads `CONDID` and `SUBP` but its aggregation columns omit them, while downstream species builders require them and locally observed species products contain them.
- **Disposition:** investigate version/provenance and establish one producer contract before certifying tree or sapling species workflows. This is contract drift; the investigation has not classified every existing product value as corrupted.

### Suspected linkage problem requiring validation

**Repeated-visit pairing.**

- **Repository finding:** the current community-change script groups by composite `stable_plot_id`, sorts by `INVYR`, and lags visits without requiring the current `PREV_PLT_CN` to match the lagged visit. A read-only audit found candidate adjacent pairs that did not match that structural link, many because `PREV_PLT_CN` was absent.
- **Official FIA support:** `PREV_PLT_CN` is the previous-visit structural link; condition change has a dedicated change matrix.
- **Disposition:** treat this as a suspected linkage problem, not a confirmed defect, until population, missing-link, replacement-plot, and historical cases are validated. Repeated-visit exports remain unavailable.

### Scientific or methodological questions requiring domain review

- Whether abundance derived from tree, sapling, or seedling expanders should also be multiplied by `CONDPROP_UNADJ` for each intended CWM or composition result.
- Which values are summed, retained, normalized, or transformed when removing `SUBP`, separately for each life stage.
- Whether crown fire (`DSTRBCD = 32`) is an acceptable project-specific proxy for high-severity fire.
- Whether a broad-category crosswalk between condition disturbance, tree cause, tree damage, and external detections is useful for a stated hypothesis.
- Whether older seedling count protocols are sufficiently comparable for a proposed analysis.

These are not described as repository defects. Until reviewed, the affected concepts are `domain_review_required`.

### Inherent data limitation

**Approximate public coordinates.**

- **Official FIA support:** public FIADB coordinates are fuzzed, and coordinates for some private plots are swapped within a county to protect privacy and plot integrity.
- **Repository finding:** the disturbance-linkage scaffold buffers public FIA coordinates by 800 m before intersecting MTBS/IDS data.
- **Disposition:** this is an inherent source limitation, not a repository defect. The tool may support coarse location filters with an always-visible warning. Exact exposure or event-linkage claims are deferred pending a reviewed spatial method.

### Other delivery risks

- The current condition summary lacks required protocol fields.
- The server has no raw FIA, so a server-only rebuild is impossible by design. Loss of the build-environment raw snapshot would prevent exact investigation or reconstruction of an older release even if the served Parquet remained usable.
- Keeping the only raw snapshot on an individual workstation is a single point of failure. A durable project-controlled archive and named retention owner are prerequisites for calling a release operationally reproducible.
- Copying loose processed files to the server can create mixed-version or partial products. Immutable release staging, destination checksum verification, a terminal success marker, and atomic pointer promotion are required.
- FIA DataMart may change after a release. A URL and download date alone are insufficient provenance; source-file hashes and retained source archives are needed for exact reconstruction.
- Server storage may not support symlinks or atomic rename semantics across filesystems. The promotion spike must select a pointer or manifest mechanism whose atomicity is supported on the actual UCSB storage.
- Registry curation can become duplicative unless generated and curated metadata have explicit ownership.
- Large exports can exceed desktop memory even when scans are fast.
- The UCSB host may not permit a long-running Streamlit process or may require a managed reverse proxy, service account, scheduler allocation, or approved storage mount.
- A runtime availability cache can become stale while files are rebuilt or replaced; execution-time revalidation and atomic producer promotion are required.
- Concurrent extractions can exhaust server memory, saturate shared storage, or fill an export filesystem unless concurrency, byte, row, time, and retention limits are enforced.
- A request-builder snapshot can describe products that have since changed; disconnected mode must never claim live execution readiness.
- Local and server environments can diverge in Python/R versions, registry commits, path layout, or product fingerprints. The launch diagnostics and manifest must make that divergence visible.
- Official guidance is versioned and regional; a current guide alone does not certify historical data.
- The researcher may prefer different first-slice terminology or exclusions; labels and caveats need review before UI implementation.

## Performance strategy

- Use Parquet metadata for product discovery and schema checks.
- Push state, year, status, code, and other source predicates into Arrow scans.
- Cache only small lookups and metadata, not national Pandas frames.
- Cap preview rows independently from export rows.
- Estimate output size and require narrower filters when configured server-memory, browser-download, or export-storage thresholds would be exceeded.
- Stream or batch exports if the chosen Streamlit download path otherwise duplicates the full result in memory.
- Record scan, conversion, join, and export row counts for diagnostics.
- Bound concurrent executions and time spent waiting for shared storage; do not allow every browser rerun to start a new scan.
- Cache the runtime inventory using registry version and file metadata, while revalidating referenced products at execution.
- Benchmark the complete first-slice request set on both the development fixture and the intended UCSB handoff host before release.

No database should be added merely for anticipated scale. Reconsider DuckDB only if measured, reviewed multi-product joins cannot meet the interaction target with Arrow plus bounded Pandas.

## Testing and validation strategy

### Registry and contract tests

- Registry schema, unique IDs, valid certification states, resolvable product references, and no duplicated curated definitions.
- Observed schemas and logical keys match declared contracts.
- A workflow cannot reference an unavailable, unreviewed, domain-review, or defect-blocked concept.
- Constrained concepts declare executable restrictions and provenance text.
- Logical paths resolve only inside configured roots and registry/request files cannot inject absolute paths or traversal.
- Runtime statuses distinguish complete, partial, stale, incompatible, missing, unmanaged, and blocked products with deterministic reasons.
- Partial products execute only for workflows and coverage explicitly declared safe.
- Release manifests require an immutable release ID, compatible registry version, declared grain/schema/coverage, upstream acquisition receipts, QA result, checksums, and terminal success marker.
- Unmanifested processed files and releases with missing, extra, changed, or checksum-failing artifacts are `unmanaged` or `blocked`, never `available`.
- Source receipts identify every required raw state/table or source archive without exposing the private archive path to browser users.

### Semantic tests

- Golden records spanning relevant `PLOT.MANUAL`, regions, status codes, disturbance/treatment slots, and sentinels.
- Plain-language labels round-trip to preserved official raw codes.
- `CONDID` is treated as visit-local.
- Forest-share derivation equals the documented condition-proportion formula.
- Coordinate, protocol, and population-estimation caveats appear whenever applicable.
- Exact guide title/version/source is present for every exposed condition concept.

### Extraction tests

- Instrument scans to prove column projection and compatible predicate pushdown occur before `to_pandas()`.
- Ensure a full national product cannot be loaded through fallback behavior.
- Validate row and memory limits, empty results, missing products, schema drift, invalid filters, and incompatible requests.
- Assert output uniqueness at the declared grain and absence of accidental many-to-many duplication.
- Validate CSV/Parquet schemas and manifest request hashes.
- Assert connected Streamlit and command-line execution use the same engine and produce equivalent normalized requests, values, schemas, and provenance for the same fixture and fingerprints.
- Change or remove an input after UI inventory and prove execution-time validation fails safely rather than using stale status.
- Exercise concurrent requests, interrupted exports, cleanup, disk-full simulation where practical, and controlled rejection when limits are exceeded.

### Deployment and availability tests

- Start from a code-only checkout and show registered products as missing without crashing or silently substituting live API data.
- Start the serving environment with a valid promoted explorer release and no raw FIA files; prove the full September extraction succeeds without DataMart or FIADB-API network access.
- Start against a populated test root and verify schema, partition coverage, fingerprints, QA markers, and environment label.
- Stage a release with a missing partition, failed QA result, absent `_SUCCESS`, incompatible registry version, and one corrupted file in turn; prove each is rejected before extraction.
- Promote between two test releases while an extraction is active; prove each request uses exactly one pinned release and rollback changes only the promotion pointer.
- Verify the application source tree and scientific product roots are read-only to the runtime identity; only the export/cache roots are writable.
- Verify browser-visible diagnostics and manifests omit credentials and private absolute paths.
- Generate a disconnected availability snapshot, display its timestamp and `live: false` status, then require authoritative server validation before execution.
- Run a smoke extraction through the actual UCSB access path using a non-sensitive golden fixture before enabling the full reviewed product.

### Defect and exclusion tests

- Treatment timing fields cannot be exposed without `treatment_year_status`, and a year whose status is not `calendar_year` is never presented as one.
- IDS layers whose declared identifiers are not unique cannot back an acreage total.
- Repeated-visit workflows cannot use chronological lag without validated `PREV_PLT_CN` behavior.
- Species workflows remain unavailable until producer/product and aggregation gates pass.
- External linkage cannot be described as exact plot exposure.

### Acceptance checks

A researcher should be able to complete the condition workflow from a clean connected launch without consulting repository documentation, recognize what one row means, explain every default, see current product coverage, and reproduce the same request from the manifest. The portable request must also execute through the documented server command with equivalent results. A knowledgeable reviewer should be able to trace every scientific column to its source, transformation, cited FIA definition, and exact product fingerprint.

## Handoff and maintenance

- Keep one authoritative registry consumed by the application, validation, and generated reference documentation.
- Generate mechanical metadata from files; curate scientific meaning once.
- Give each workflow an owner, certification review date, source documentation version, and small golden test fixture.
- Include a short maintainer procedure for adding a product or variable: propose workflow demand, trace source, certify semantics, add constraints, add tests, then expose it.
- Make missing products and stale fingerprints visible in the UI rather than relying on institutional knowledge.
- Preserve portable request JSON so future preset support does not require redesigning extraction semantics.
- Maintain a server runbook covering environment creation, data-root configuration, launch/restart, approved access path, log location, inventory refresh, export cleanup, resource limits, and rollback.
- Maintain a data-refresh runbook that keeps DataMart download, product rebuild, QA, and atomic product promotion outside the interactive process.
- Maintain a release-publication runbook covering raw-snapshot selection, durable archival, acquisition receipts, build staging, QA gates, checksum creation, transfer verification, atomic promotion, inventory refresh, retention, and rollback.
- Assign ownership separately for raw-snapshot preservation, scientific product approval, server promotion, and application operation; do not assume one maintainer implicitly owns all four.
- Pin the application environment and record the Git commit, registry version, Python and PyArrow versions, and non-sensitive environment label in launch diagnostics.
- Assign operational ownership for restarting the app, reviewing failures, managing export storage, and approving new products; scientific registry ownership alone is insufficient.

## Scope through September 15

### Smallest complete release

- Existing dashboard retained for repository orientation.
- One guided condition-grain workflow.
- Reviewed plain-language variables and filters with optional official-code detail.
- Visible and recorded accessible-forest default.
- Automatic identity and protocol context.
- Arrow-first extraction, bounded preview, CSV/Parquet export, and JSON manifest.
- One extraction engine shared by connected Streamlit and a portable-request command-line executor.
- Configurable logical data root plus live registry-aware availability and compatibility reporting.
- One immutable, validated condition-explorer release published to the UCSB server, with a durable off-server raw snapshot, upstream acquisition receipt, checksums, QA report, and rollback identifier.
- A validated UCSB launch/access/export pattern, or request-builder fallback if shared hosting is not approved in time.
- Tests and a short user/maintainer handoff guide.

### Implementation sequence

0. **UCSB environment and data-inventory spike:** inspect the intended host read-only; confirm that raw FIA is absent; inventory candidate processed products; establish the approved access method, code/data/release layout, logical roots, runtime identity and permissions, Python environment, export/cache roots, quotas, cleanup mechanism, and whether a long-running Streamlit process is permitted. Do not treat unmanifested candidates as production inputs and do not rebuild during this spike.
1. **Certification, product, and release contracts:** confirm the first-slice variable matrix, source documentation, constraints, condition-grain product contract, release contents, availability states, completeness rules, source-receipt fields, and QA gates; decide registry location through the import/test spike.
2. **Raw preservation and first release publication:** identify the exact local raw snapshot and whether its state/table acquisitions are internally consistent; copy or archive it in durable project-controlled storage; build the protocol-complete condition product in new staging; validate it; create the release manifest/checksums; transfer it to UCSB staging; verify it; and atomically promote it. If the current raw snapshot cannot be documented and preserved, reacquire a coherent DataMart snapshot before publishing.
3. **Shared extraction engine:** implement registry validation, promoted-release resolution, live inventory, request normalization, Arrow scans, limits, exports, and manifests behind a UI-independent API. Prove operation against the promoted release with no raw FIA or outbound FIA service dependency.
4. **Two entry points:** add the command-line request executor first, then implement workflow selection, grain explanation, filters, variable groups, release/coverage display, review, preview, and downloads within Streamlit using that same engine.
5. **Connected deployment validation:** configure the UCSB release/data roots and protected access path; validate read-only release permissions, request-scoped export staging, cleanup, concurrency limits, diagnostics, promotion/rollback behavior, and connected/CLI equivalence.
6. **Validation and handoff:** add semantic/contract/extraction/deployment tests, researcher usability review, server performance checks, and maintainer, raw-preservation, data-refresh, release-publication, and rollback runbooks.
7. **Conditional extension:** consider plot-condition-species only if all technical and domain gates close without jeopardizing the complete condition slice.

### Explicit non-goals

- Exhaustive FIADB semantic certification.
- Production data repair or scientific pipeline refactoring as part of the explorer build.
- Database migration, separate application API, separate frontend, public Community Cloud data deployment, or migration of large products into Git/Git LFS.
- High-availability operations, public internet hosting, custom identity management, or an institutional production-service commitment beyond the approved UCSB launch/access pattern.
- Automatic DataMart downloads or product rebuilds initiated by researcher interaction.
- Requiring raw FIA tables to be mounted on the Streamlit serving host, or migrating the full raw archive there merely to make the app run.
- Population estimation.
- Preset loading or generated analysis code.
- Mortality/removal, repeated change, spatial disturbance linkage, or arbitrary cross-product query building.
- Exact map presentation of FIA plot locations.

## Decisions requiring researcher or FIA-domain input

Before UI implementation is finalized, obtain review of:

1. The first-slice plain-language labels, definitions, and default variable set.
2. Whether the default accessible-forest restriction fits the initial research use, while keeping its criterion visible and recorded.
3. Whether condition disturbance years should join the September presence concepts after manual-specific review, or remain deferred.
4. Any desired cross-grain comparison; the default is to keep concepts separate.
5. Future abundance and subplot aggregation methods.
6. Future use, if any, of crown fire as a project-specific severity proxy.
7. The scientific acceptability of approximate-coordinate linkage for future work.

## Decisions requiring UCSB infrastructure or project-owner input

Before connected deployment is committed, determine:

1. Which UCSB host is approved for a long-running Streamlit process and whether it is a login node, compute allocation, virtual machine, container service, or another managed environment.
2. Whether access will use VPN plus SSH tunneling or an authenticated TLS reverse proxy, and who owns that configuration.
3. The authoritative data-root location, whether code and data are separated, and which runtime identity receives read-only product access.
4. The approved export/cache root, per-request and total quotas, retention period, cleanup owner, and whether browser download is acceptable for anticipated sizes.
5. Whether multiple simultaneous users are expected and what CPU, memory, elapsed-time, and I/O limits are appropriate.
6. Which products constitute the server's authoritative September snapshot and how successful QA and atomic promotion are signaled.
7. Whether application logs and request metadata have institutional retention or privacy requirements.
8. Where the exact raw FIA snapshot will be durably archived, who owns retention, and whether the existing local snapshot is coherent and sufficiently documented or must be reacquired.
9. Which approved transfer mechanism and server filesystem operation provide destination checksum verification, atomic promotion, and rollback without exposing private paths in the app.

## Official FIA references

- USDA Forest Service. *FIADB User Guides — Volume: Database Description, version 9.4, revision August 2025.* [PDF](https://research.fs.usda.gov/sites/default/files/2025-08/wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf)
- USDA Forest Service. *National Core Field Guide for the Nationwide Forest Inventory, version 9.4, September 2024.* [PDF](https://research.fs.usda.gov/sites/default/files/2024-09/wo-v9-4_sep2024_fg_nfi_natl.pdf)
- USDA Forest Service. *Nationwide Forest Inventory Field Guide, version 9.5, September 2025* and links to older and regional versions. [Guide page](https://research.fs.usda.gov/understory/nationwide-forest-inventory-field-guide)
- USDA Forest Service. *FIADB Population Estimation User Guide for Phase 2, edition November 2018.* [PDF](https://research.fs.usda.gov/sites/default/files/2024-05/wo-nov2018_ug_population_estimation.pdf)
- USDA Forest Service. *EVALIDator User Guide, version 2.1, February 2024.* [PDF](https://research.fs.usda.gov/sites/default/files/2024-03/v2.1_feb2024_ug_evalidator.pdf)
- USDA Forest Service. *FIADB-API & EVALIDator User Documentation.* The live page states that endpoint documentation is still under construction. [Documentation](https://apps.fs.usda.gov/fiadb-api)
- USDA Forest Service. *FIA DataMart.* [Data and documentation hub](https://research.fs.usda.gov/products/dataandtools/fia-datamart)
- USDA Forest Service. *The Forest Inventory and Analysis Database: Database Description and Users Manual, version 4.0 for Phase 2* (historical protocol reference). [PDF](https://research.fs.usda.gov/download/treesearch/37446.pdf)

## Platform references

- Streamlit. *File organization for your Community Cloud app.* Community Cloud creates the runtime from the repository and requires files needed locally by the application to be available in that environment. [Documentation](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/file-organization)
- Streamlit. *Static file serving.* Generated files on Community Cloud are not guaranteed to persist across sessions, and large/many served files can exceed service limits. [Documentation](https://docs.streamlit.io/develop/concepts/configuration/serving-static-files)

## Exact next implementation step

After this revision is approved, perform two bounded discovery tasks before building the broader interface. First, inspect the UCSB server read-only: record the approved host/access pattern, code/data/release roots, runtime permissions, Python environment, candidate processed-product coverage/schema/fingerprints, export/cache location and quota, and whether a supervised Streamlit process is permitted. Confirm that raw FIA is absent and classify existing processed files as unmanaged candidates until release evidence is verified. Do not rebuild or mutate server products.

Second, inspect the local raw snapshot and build lineage read-only: inventory state/table files, acquisition dates where available, hashes, schema/manual coverage, and the producer commit/configuration that created the current condition products. Decide whether this snapshot can be preserved and used or whether one coherent DataMart reacquisition is required. Identify the durable raw-archive owner and location before calling the first release reproducible.

Then, in the first code spike: (1) choose the registry location using the stated import/test criteria; (2) define and test the release-manifest and logical-root contracts; (3) construct a schema-validated condition-grain fixture carrying `PLOT.MANUAL` and required protocol context; and (4) prove one constrained request through the shared command-line engine with an export and manifest against a miniature promoted release while raw FIA is absent from the serving fixture. Build and publish the national condition release only after the condition contract, certification matrix, source-snapshot decision, release QA gates, availability checks, and safe-failure tests pass. Connect Streamlit after the shared engine and publication boundary are proven.
