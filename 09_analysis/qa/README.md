# Analysis QA

The active numbered analysis scripts write compact checks to
`outputs/<producer>/`. The folder name starts with the script number, so each
result can be traced without guessing which analysis step created it.

`qa_products.csv` is the source of truth for QA provenance. It records every
expected result, its producer, and whether a complete rebuild must create it.
The TerraClimate missing-site file is optional because the extractor writes it
only when a site is missing.

Run the provenance check after rebuilding the analysis:

```powershell
Rscript 09_analysis/qa/scripts/validate_qa_products.R --require-outputs
```

The check fails when:

- a registered producer is missing or is not tracked by Git;
- a producer does not name the result it is supposed to create;
- an unregistered result appears in `qa/outputs/`; or
- a required result is absent when `--require-outputs` is used.

QA files are generated results. The scripts, manifest, tests, and directory
structure are tracked; the CSV outputs can be regenerated on a new server.
Row-level analysis data remain in `../data/`, and model-specific results remain
inside `../results/model_runs/<run_id>/`.
