# forest_explorer

The catalog layer for the Forest Data Explorer. It answers three questions about every data product in this repository:

1. What does one row mean, and what identifies it?
2. What is the interface allowed to do with it?
3. Is it actually there, and does it have the grain we claim?

Questions 1 and 2 are curated by people. Question 3 is measured from the data. Keeping those separate is the point of this directory.

```
forest_explorer/
  registry/products.yaml            curated — meaning, keys, access rules, caveats
  catalog/snapshot/catalog.json    committed — portable schemas for the dashboard
  registry/joins.yaml               curated — join keys, cardinality, warnings
  registry/query_presets.yaml       curated — reusable research recipes
  catalog/snapshot/joins.json      committed — portable compatibility graph
  catalog/snapshot/query_presets.json committed — portable recipes
  catalog/build_snapshot.R         generator — refreshes JSON and Markdown snapshots
  catalog/build_inventory.py       local audit — measures a specific data root
  tests/test_registry.py           contract tests for the registry
```

## Three things that are not the same

These get conflated constantly, so the registry keeps them apart:

| Question | Field | Who decides |
|---|---|---|
| Is this product expected to exist at all? | `lifecycle` | curated |
| Is its scientific use approved? | `review_status` | curated |
| Is a valid copy physically here? | `availability` | measured |

A `planned` product being absent is expected. An `active` one being absent is a problem. A product can be present and still unusable because its meaning is unsettled. Do not collapse these into one status.

Grain and lineage are likewise separate. `grain_id` says which grain a row sits at, with the observation hierarchy declared once in the `grains:` block; `derived_from` says which product was used to build this one. Two products can share a grain without either being built from the other — `plot_condition_metadata` and `fia_condition_disturbance_flags` both sit at `fia_condition_visit`.

The committed, GitHub-searchable outputs are [`docs/DATA_CATALOG.md`](../docs/DATA_CATALOG.md) and [`docs/QUERY_GUIDE.md`](../docs/QUERY_GUIDE.md). The dashboard reads matching JSON snapshots and does not inspect data directories.

## Refresh the portable catalog

```bash
Rscript forest_explorer/catalog/build_snapshot.R
```

This maintainer command refreshes the product catalog, join graph, research recipes, and both GitHub-searchable Markdown pages. Researchers only need the committed files.

## Audit a specific data root

```bash
python3 forest_explorer/catalog/build_inventory.py
```

Add `--verify-all` to check declared keys on every product regardless of size. Without it, products above eight million rows are reported as `not checked` rather than assumed correct.

Point it at a different data root when code and data are separated:

```bash
FOREST_DATA_ROOT=/path/to/products \
  python3 forest_explorer/catalog/build_inventory.py --root-label ucsb-server
```

The generator is read-only. It opens products for metadata and key columns only and writes nothing into any data directory.

## Test the registry

```bash
python3 -m pytest forest_explorer/tests/test_registry.py
```

These need no data present, so they run in a code-only checkout. They enforce the rules that keep the registry safe to resolve — unique ids, resolvable references, relative paths with no machine-specific parts, a declared key on anything offered for extraction, and no unreviewed product being offered to a researcher.

## Adding a product

1. Add an entry to `registry/products.yaml`. Every field is described in the header of that file.
2. Start it at `access_mode: catalog_only` and `review_status: not_reviewed`. The tests will reject any other combination until someone has checked it.
3. Rerun the generator and read the grain check. If the declared key is not unique, that is a finding about the product — fix the producer or change the declared grain, but do not delete the key to make the check pass.
4. Refresh the portable catalog snapshot, then rerun the tests.

## Query planning

`registry/joins.yaml` is the compatibility graph: every listed join records its keys, cardinality, resulting grain, review state, and warning. `registry/query_presets.yaml` turns recurring research requests into editable recipes, including a wide baseline export, without creating duplicate canonical data.

The dashboard Query Builder uses those snapshots to generate DuckDB SQL. It allows only one `1:many` join per query so two independent detail tables cannot silently form a cross-product. It generates queries but never executes them.

Match-rate measurements are not yet stored in the portable snapshot. Validate match rates against the intended data root before certifying a new join.

## Research bundle

`registry/research_bundle.yaml` names the tables a collaborator needs for a set of research questions, which tables answer which question, how to link them, and a few convenience views. The builder copies those products, unchanged, into one DuckDB file with a generated README:

```bash
Rscript forest_explorer/export/build_research_bundle.R
```

The default output is `scratch_output/research_bundle/forest_research_bundle.duckdb`, which is gitignored. Pass `--output=<path>` to write elsewhere and `--overwrite` to replace an existing bundle. The bundle is an export; the registered products stay authoritative, and `bundle_info` records the commit it was built from.
