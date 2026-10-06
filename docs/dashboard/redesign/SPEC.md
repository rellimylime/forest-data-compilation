# Dashboard redesign spec

Handoff for implementing a cleaner, more intuitive dashboard. Written without access to the repository data, so **presentation only**: do not change what data is loaded, how it is loaded, or the query planner and Query Builder logic. Everything below must be verified against the real data on this server.

Reference mockups: `mockups/home.html` and `mockups/analysis-results.html` are Design Component source files (they need a `support.js` runtime that is not in this repo), so they do not render when opened directly. Read them for exact sizes, colors, spacing and copy. For the rendered view, ask the user for the screenshots or the live canvas link. Chart positions and numbers in them are fake placeholders.

## Problems being fixed

Found in `utils.py` and the pages:

1. Links are `st.page_link` styled as 0.86rem grey text (`[data-testid="stPageLink"] a`). They do not read as buttons.
2. Things that look clickable are not: `.fd-card`, `.fd-route-card`, `.fd-step-card`, `.fd-pill` are plain HTML with borders and hover-like styling.
3. Three card systems (`fd-card`, `fd-route-card`, `fd-step-card`) plus `metric-card`, each with its own padding and font sizes (11px to 0.95rem). `fd-grid` is auto-fit, so column counts change per page.
4. Results are hidden behind widgets. `6_Analysis.py`: coefficients need two multiselects; figures need three chained selectboxes and show one figure at a time; table preview is a selectbox. About 15 selectbox/radio/multiselect widgets across pages, plus tabs nested in tabs.
5. Page flow is inconsistent: header, then `page_intro` pill, then metric cards, then route cards, then tabs.

## Design rules

- **One button system.** Primary (filled green `#315c43`, white text) for "go to" and "download". Secondary (white, 2px green border, green text) for secondary actions. Min height 44px, 15px, weight 600. Nothing else gets a border, shadow or hover state that suggests a click.
- **Plain links** only inline in prose, underlined.
- **One card.** Border `1px #d7d2c5`, radius 12px, padding 20 to 24px, background `#fffefa`. Cards in one row have equal width (`st.columns`, never auto-fit) and equal min height. Card contents: title 18 to 20px bold, body 16px, one action button at the bottom if the card is a destination.
- **Type scale, nothing smaller than 14px:** page title 40px, section title 26px, card title 18 to 20px, body 16px, caption 14px. Drop the uppercase letter-spaced micro labels (`fd-section-label`, `fd-kicker`, `metric-card .label`).
- **Palette stays** as in `.streamlit/config.toml`: bg `#f5f3ec`, surface `#fffefa`, text `#20251f`, primary `#315c43`, border `#d7d2c5`, muted text `#5a6055` (check contrast, keep 4.5:1).
- **Show before asking.** Default view of every page shows the content a visitor most likely wants. No dropdown required to see the first useful thing.
- **Tabs only for true alternatives of the same view** (for example the three climate responses). Never tabs inside tabs. Never radio/selectbox to switch between views of the same thing: use `st.tabs` or `st.segmented_control`.
- **Filters are for narrowing, not for seeing.** If a selectbox picks one of N things, show all N (grid, list, or tabs) instead. Where N is large (catalog, FIA fields), keep search plus filters but show results immediately, unfiltered, on first load.
- **Page skeleton, same on every page:** title, one-sentence lead, optional primary action(s), content sections in reading order, a "Next" row of 1 to 2 secondary buttons. Remove `page_intro` pill row and the standalone route-card strips; fold their information into the lead and the "Next" row.
- **Plain language.** Replace jargon in headings with what the user is doing ("Effect of each predictor"). Keep exact variable names in captions.

## Implementation notes (Streamlit)

- Streamlit >= 1.46 is already required. Use `st.button(type="primary"|"secondary")`, `st.download_button(type="primary")`, `st.page_link`, `st.segmented_control`, `st.tabs`, `st.columns(n, border=True)` or `st.container(border=True)` for cards. Prefer native widgets over HTML cards so they are accessible and keyboard focusable.
- Style `st.page_link` as a button through the existing CSS block in `utils.py` (`[data-testid="stPageLink"] a`): border, padding, min-height 44px, radius 8px, weight 600. Use a `primary` variant via a wrapper container class if needed. Verify the selector against the installed Streamlit version.
- Equal-height cards: wrap in `st.container(border=True, height=...)` or set min-height in CSS via the container's test id. Check on the server at 1280px and ~800px widths.
- Consolidate to a small set of helpers in `utils.py`: `page_header(title, lead, actions=())`, `card(title, body, cta=None)`, `stat_row(items)`, `next_row(links)`. Delete `route_card`, `route_grid`, `workflow_step/grid`, `page_intro` and the unused CSS once nothing calls them (grep first).
- Do not change `.streamlit/config.toml` server settings.

## Page by page

### Home (`home.py`) — mockup `mockups/home.html`
1. Title, one-sentence research question, two buttons: primary "See the results" (Analysis), secondary "See how it is built".
2. Search box directly under, labelled "Looking for a table, variable or script?". Keep the existing `search_workflow` logic untouched; only move and restyle. Results render under the box as equal cards, each with a button.
3. "What do you want to do?": three equal cards (read the results, find or build a dataset, look up a raw FIA field), each with one button. Replaces `START_CARDS`.
4. "How the results are produced": three numbered steps with the present/total table counts (keep `present_count`). Each has a plain link. Not styled as a button.
5. Remove the "Current results" metric strip from Home (it duplicates Analysis). Footer line links to IDS, Climate, Repository map.

### Analysis (`pages/6_Analysis.py`) — mockup `mockups/analysis-results.html`
Order the page as: header with the two report downloads as buttons, stat row, results, figures, robustness, pipeline/tables/related work. Today results are the third tab and pipeline is first; the visitor's likely goal is the results.
1. **Stat row:** four equal cards from `model_fit` (models, conditions range, stable plots, last rebuild), same values as now.
2. **Effect of each predictor:** `st.segmented_control` for response (temperature, precipitation, CWD), default first. One Plotly forest plot per life-stage group side by side (`facet_col`), estimate dot and `conf_low`/`conf_high` interval, vertical zero line, intercepts excluded, shared x axis so groups are comparable. Plain-language axis ends ("shifted to cooler/wetter", "warmer/drier") only if the sign convention in `coefficients.csv` supports it: check against `09_analysis` docs before using that wording. Replaces the Response and Group multiselects and the "Show intercepts" checkbox. Keep the full coefficient table behind a "Show full coefficient table" control (`st.expander` is fine here) with the p-values.
3. **Figures:** for the selected response show every figure in the manifest as a grid of `st.image` (3 columns) with the caption under each. Replaces the three chained selectboxes. Group by section with subheadings if there are many.
4. **Robustness (tree weighting):** keep the explanation, show the comparison table with a readable "same direction" yes/no column instead of eight raw columns. Move the model-fit table into an expander.
5. **Pipeline / Tables / Related work:** below the results, as three tabs (these are alternatives, not a flow). In Tables, show the table list and let each row's preview open on demand (`st.dataframe` selection or expander per table), not a selectbox.

### Other pages
Apply the rules above; no mockups. Checklist from the widget inventory:
- `5_Data_Catalog.py` (Find data): results list visible immediately, search plus the two multiselects as narrowing filters, each result a card with a clear primary action. Expander contents shown inline if short.
- `8_Query_Builder.py`: the four selectboxes (recipe, domain, grain, anchor) demand domain knowledge. Lead with the recipes as visible cards or tabs, with the "Baseline research table" recipe first. Keep the existing planner calls and the generated SQL tabs unchanged. Behaviour must be byte-identical for the same selections.
- `3_FIA_Forest.py`: nested tabs plus several radios and multiselects. Flatten to one level of tabs; turn radios that switch views (`view`, `size_class`, `ba_metric`, `seed_type`) into `st.segmented_control` or tabs, with a sensible default so a chart shows on load.
- `1_IDS_Survey.py`, `2_Climate.py`, `4_Architecture.py`: one level of tabs, `layer_sel`, `grid_dataset` and `dataset` selectors become tabs or segmented controls, charts shown on load.
- `7_FIA_Navigator.py`: same search-first rule as Find data.

## Order of work

1. Shared CSS and helpers in `utils.py`; check the existing pages still render.
2. Home, then Analysis. Stop and let the user preview.
3. Remaining pages one per commit.

## Verification (required, on this server with real data)

- `docs/dashboard/run_dashboard.sh` or `streamlit run docs/dashboard/app.py` starts and every page loads with no exception and no empty section that was populated before.
- For each changed page, compare values before and after: stat numbers, row counts in tables, coefficient estimates, figure counts, generated SQL for each Query Builder recipe (diff the text).
- Screenshots at 1280px and 800px (Playwright with the preinstalled Chromium: `executablePath: '/opt/pw-browsers/chromium'` if present, do not run `playwright install`) of Home, Analysis and each page changed. Look for clipped text, unequal card heights, text under 14px, links that do not look like buttons.
- Keyboard: Tab reaches every button and link with a visible focus ring.
- Existing tests: `pytest forest_explorer/tests`.
- Grep for removed helpers before deleting them.

## Out of scope

Data loading, `utils.load_*` functions, `forest_explorer/`, the query planner, catalog snapshots, R scripts, server and CORS settings.
