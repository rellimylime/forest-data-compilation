Implement the dashboard redesign described in `docs/dashboard/redesign/SPEC.md`. Read `docs/dashboard/redesign/mockups/home.html` and `analysis-results.html` for the target layout, sizes and copy. They are source files and do not render in a browser; values in them are placeholders.

Constraints:
- Presentation only. Do not change data loading, `forest_explorer/`, the query planner, or generated SQL.
- Follow the spec's order of work: shared CSS and helpers, then Home, then Analysis. Run the dashboard with the real data, take screenshots at 1280px and 800px, and compare values before and after as the spec's verification section says. Then stop so I can preview before you continue with the other pages.
- Where the spec conflicts with what the real data supports (for example the sign convention for the coefficient axis labels), follow the data and tell me.
- Keep it simple. Do not add abstractions beyond the four helpers the spec names.
- Commit to a feature branch in small commits. Do not push or open a PR until I say so.
