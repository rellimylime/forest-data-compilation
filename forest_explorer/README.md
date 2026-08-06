# forest_explorer

The catalog layer for the Forest Data Explorer. It answers three questions about every data product in this repository:

1. What does one row mean, and what identifies it?
2. What is the interface allowed to do with it?
3. Is it actually there, and does it have the grain we claim?

Questions 1 and 2 are curated by people. Question 3 is measured from the data. Keeping those separate is the point of this directory.

```
forest_explorer/
  registry/products.yaml          curated — meaning, keys, access rules, caveats
  catalog/build_inventory.py      generator — measures a data root
  catalog/generated/inventory.json  generated — machine-readable inventory
  tests/test_registry.py          contract tests for the registry
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

The human-readable output is [`docs/MASTER_PRODUCT_INVENTORY.md`](../docs/MASTER_PRODUCT_INVENTORY.md).

## Rebuild the inventory

```bash
python forest_explorer/catalog/build_inventory.py
```

Add `--verify-all` to check declared keys on every product regardless of size. Without it, products above eight million rows are reported as `not checked` rather than assumed correct.

Point it at a different data root when code and data are separated:

```bash
FOREST_DATA_ROOT=/path/to/products \
  python forest_explorer/catalog/build_inventory.py --root-label ucsb-server
```

The generator is read-only. It opens products for metadata and key columns only and writes nothing into any data directory.

## Test the registry

```bash
python -m pytest forest_explorer/tests/test_registry.py
```

These need no data present, so they run in a code-only checkout. They enforce the rules that keep the registry safe to resolve — unique ids, resolvable references, relative paths with no machine-specific parts, a declared key on anything offered for extraction, and no unreviewed product being offered to a researcher.

## Adding a product

1. Add an entry to `registry/products.yaml`. Every field is described in the header of that file.
2. Start it at `access_mode: catalog_only` and `review_status: not_reviewed`. The tests will reject any other combination until someone has checked it.
3. Rerun the generator and read the grain check. If the declared key is not unique, that is a finding about the product — fix the producer or change the declared grain, but do not delete the key to make the check pass.
4. Rerun the tests.

## What this does not do yet

The registry describes products one at a time. It does not yet declare the relationships between them — which joins are safe, at what cardinality, with what match rate. That is the compatibility graph, and it is the next piece.
