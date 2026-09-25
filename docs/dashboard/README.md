# Forest Data Explorer Dashboard

The Streamlit dashboard is the easiest local interface for navigating this repository. It searches committed metadata snapshots, so its catalog, join map, recipes, and generated query plans work even when the large data products are stored elsewhere.

## Start the dashboard

From the repository root:

```bash
pip install -r requirements.txt
streamlit run docs/dashboard/app.py
```

Streamlit prints a local URL, normally `http://localhost:8501`. Keep that terminal open while using the app.

## Which page to use

| Need | Page |
|---|---|
| Understand the overall workflow | Architecture |
| Find an existing product or variable | Catalog |
| See safe joins or build a custom export | Query Builder / Build Data |
| Find a raw FIADB table or field not yet extracted | FIA Navigator |
| Inspect a workstream visually | IDS, Climate, FIA Forest, or Thermophilization |

## Build a dataset

The Query Builder does four things without opening repository data:

1. Searches products, observed variables, documented joins, and research recipes.
2. Starts from an anchor product whose row scale controls the result.
3. Shows join keys, cardinality, review state, and warnings before fields are selected.
4. Generates a DuckDB `SELECT` or Parquet `COPY` statement for deliberate execution.

The **Baseline research table** preset is the current wide-file handoff requested in the project meeting. It combines condition-visit life-stage climate affinity, condition disturbance, elevation, and plot tree/seedling metrics. It also states what remains unavailable: aligned visit/interval mortality, condition-level understory change, slope/aspect, and reviewed agent nativeness.

The builder allows at most one `1:many` join in a query. This prevents two detail tables from silently multiplying one another. A `many:1` join preserves anchor rows, but the joined value repeats at the finer anchor grain and must not be summed.

## Execute a generated query

Download `forest_query.sql`, review it, then run it from the repository root with DuckDB. The default export goes to the gitignored `scratch_output/` directory, which DuckDB does not create for you:

```bash
mkdir -p scratch_output
duckdb < forest_query.sql
```

Or use R:

```r
library(DBI)
library(duckdb)
dir.create("scratch_output", showWarnings = FALSE)
con <- dbConnect(duckdb())
sql <- paste(readLines("forest_query.sql"), collapse = "\n")
dbExecute(con, sql)
dbDisconnect(con, shutdown = TRUE)
```

The generated paths are repository-relative. Queries use `LEFT JOIN` so unmatched anchor rows remain visible for validation. The dashboard does not execute SQL, create exports, or modify canonical products.

## Snapshot sources

- `forest_explorer/catalog/snapshot/catalog.json` — products and observed schemas
- `forest_explorer/catalog/snapshot/joins.json` — curated compatibility graph
- `forest_explorer/catalog/snapshot/query_presets.json` — research recipes
- `docs/DATA_CATALOG.md` — GitHub-searchable product and variable index
- `docs/QUERY_GUIDE.md` — GitHub-searchable join map and recipe guide

Maintainers refresh all five artifacts together:

```bash
Rscript forest_explorer/catalog/build_snapshot.R
```

Researchers using the committed dashboard do not need to run this command. A snapshot describes the products present when it was generated; it is intentionally not a live filesystem scan.

## Troubleshooting

- If a snapshot is missing, restore the repository state or ask a maintainer to refresh and commit it.
- If a product is marked missing, the query can still be planned, but it will not execute until that product exists at its documented path.
- If a field is found only in FIA Navigator, it is a raw FIADB field and may need to be added to an extraction script before it can be queried as a repository product.
- Scientific-review warnings are not software errors. They indicate that a reproducible join exists but its interpretation still needs methods review.
