# ==============================================================================
# docs/dashboard/utils.py
# Shared utilities for the forest-data-compilation unified dashboard
# ==============================================================================

import os
import inspect
import html
import json
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# ------------------------------------------------------------------------------
# Repo root — resolved relative to this file's location (docs/dashboard/)
# ------------------------------------------------------------------------------

DASHBOARD_DIR = Path(__file__).parent
STATIC_DIR = DASHBOARD_DIR / "static"
REPO_ROOT = DASHBOARD_DIR.parent.parent
GITHUB_BLOB_BASE = "https://github.com/rellimylime/forest-data-compilation/blob/main"


def repo_path(*parts) -> Path:
    """Return an absolute path relative to the repo root."""
    return REPO_ROOT.joinpath(*parts)


def static_path(*parts) -> Path:
    """Return an absolute path relative to docs/dashboard/static."""
    return STATIC_DIR.joinpath(*parts)


def load_static_json(*parts, default=None):
    """Load dashboard metadata from docs/dashboard/static for GitHub-hosted apps."""
    path = static_path(*parts)
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


@st.cache_data(show_spinner=False)
def load_static_data_json(name: str, subdir: str = "fia"):
    """Load a JSON aggregate emitted by build_static_figures.py."""
    return load_static_json("data", subdir, f"{name}.json", default=None)


@st.cache_data(show_spinner=False)
def load_static_data_csv(name: str, subdir: str = "fia"):
    """Load a CSV aggregate emitted by build_static_figures.py."""
    path = static_path("data", subdir, f"{name}.csv")
    if not path.is_file():
        return None
    try:
        return pd.read_csv(path)
    except (OSError, pd.errors.ParserError):
        return None


# ------------------------------------------------------------------------------
# Dark-theme CSS (apply once in app.py)
# ------------------------------------------------------------------------------

DARK_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&family=DM+Mono:wght@300;400;500&display=swap');

  :root {
    --fd-bg: #0d1a12;
    --fd-bg2: #111f17;
    --fd-bg3: #162219;
    --fd-bg4: #1a2a1f;
    --fd-border: #1e3024;
    --fd-border2: #2a4035;
    --fd-text: #d4e8da;
    --fd-text2: #8aab94;
    --fd-text3: #567264;
    --fd-accent: #5c9e72;
    --fd-accent2: #7bbf92;
    --fd-amber: #c48f3f;
    --fd-red: #c45050;
    --fd-blue: #4a82a8;
    --fd-radius: 8px;
    --fd-radius-lg: 12px;
    --fd-font: 'DM Sans', system-ui, sans-serif;
    --fd-mono: 'DM Mono', 'Fira Mono', monospace;
  }

  html, body, [class*="css"] {
    font-family: var(--fd-font);
  }

  .stApp {
    background: var(--fd-bg);
    color: var(--fd-text);
  }

  [data-testid="stHeader"] {
    display: none !important;
  }

  .block-container {
    max-width: 1120px;
    padding-top: 3.25rem;
    padding-bottom: 4rem;
  }

  [data-testid="stSidebar"],
  [data-testid="stSidebarNav"],
  [data-testid="stSidebarCollapsedControl"],
  button[kind="header"],
  button[title="View fullscreen"] {
    display: none !important;
  }

  section[data-testid="stSidebar"] {
    min-width: 0 !important;
    width: 0 !important;
  }

  [data-testid="stAppViewContainer"] > .main {
    margin-left: 0 !important;
  }

  h1, h2, h3, h4, h5, h6 {
    color: var(--fd-text);
    font-family: var(--fd-font);
    letter-spacing: 0;
  }

  h1 {
    font-size: 1.55rem;
    font-weight: 600;
    margin-bottom: 0.25rem;
  }

  h2, [data-testid="stHeader"] + div h2 {
    font-size: 1.15rem;
    font-weight: 600;
    margin-top: 1.7rem;
  }

  h3 {
    font-size: 0.98rem;
    font-weight: 600;
  }

  p, li, .stMarkdown, [data-testid="stCaptionContainer"] {
    color: var(--fd-text2);
  }

  a {
    color: var(--fd-accent2) !important;
    text-decoration: none;
  }

  a:hover {
    text-decoration: underline;
  }

  hr {
    border-color: var(--fd-border);
    margin: 1.45rem 0;
  }

  code {
    background: rgba(92, 158, 114, 0.1);
    color: var(--fd-accent2);
    border-radius: 4px;
    font-family: var(--fd-mono);
    font-size: 0.78rem;
    padding: 1px 5px;
  }

  pre, pre code {
    background: #080f0a !important;
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    color: #9ad4a8 !important;
    font-family: var(--fd-mono);
    font-size: 0.78rem;
    line-height: 1.65;
  }

  .fd-page-title {
    font-size: 1.55rem;
    font-weight: 600;
    color: var(--fd-text);
    line-height: 1.35;
    margin-bottom: 0.25rem;
    padding-top: 0.15rem;
  }

  .fd-page-lead {
    color: var(--fd-text2);
    max-width: 760px;
    line-height: 1.65;
    margin-bottom: 1.35rem;
  }

  .fd-section-label {
    color: var(--fd-text3);
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin: 1.7rem 0 0.7rem;
  }

  .fd-navbar-brand {
    color: var(--fd-text);
    font-size: 0.95rem;
    font-weight: 600;
    line-height: 2.35rem;
    white-space: nowrap;
  }

  .fd-navbar-rule {
    border-bottom: 1px solid var(--fd-border);
    margin: 0 0 1.35rem;
    padding-bottom: 0.45rem;
  }

  [data-testid="stPageLink"] {
    align-items: center;
    min-height: 2.35rem;
  }

  [data-testid="stPageLink"] a {
    color: var(--fd-text2) !important;
    font-size: 0.86rem;
    font-weight: 500;
    line-height: 1.25;
    text-decoration: none !important;
    white-space: normal;
  }

  [data-testid="stPageLink"] a:hover {
    color: var(--fd-accent2) !important;
  }

  .fd-nav-link {
    color: var(--fd-text2) !important;
    display: inline-flex;
    font-size: 0.86rem;
    font-weight: 500;
    line-height: 2.35rem;
    text-decoration: none !important;
    white-space: normal;
  }

  .fd-nav-link:hover {
    color: var(--fd-accent2) !important;
  }

  .metric-card {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    padding: 14px 16px 12px;
    margin-bottom: 8px;
  }

  .metric-card .label {
    color: var(--fd-text3);
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 0.07em;
    margin-bottom: 4px;
    text-transform: uppercase;
  }

  .metric-card .value {
    color: var(--fd-text);
    font-size: 24px;
    font-weight: 600;
    line-height: 1;
  }

  .metric-card .sub {
    color: var(--fd-text3);
    font-size: 11px;
    margin-top: 5px;
  }

  .fd-card {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius-lg);
    padding: 16px 18px;
  }

  .fd-card-title {
    color: var(--fd-text);
    font-size: 0.95rem;
    font-weight: 600;
    margin-bottom: 0.25rem;
  }

  .fd-card-body {
    color: var(--fd-text3);
    font-size: 0.82rem;
    line-height: 1.55;
  }

  .fd-card-body strong {
    color: var(--fd-text2);
    font-weight: 600;
  }

  .fd-kicker {
    align-items: center;
    background: rgba(92, 158, 114, 0.07);
    border: 1px solid rgba(92, 158, 114, 0.3);
    border-radius: 999px;
    color: var(--fd-accent2);
    display: inline-flex;
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.09em;
    margin-bottom: 0.8rem;
    padding: 4px 10px;
    text-transform: uppercase;
  }

  .fd-grid {
    display: grid;
    gap: 12px;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    margin: 0.8rem 0 1.2rem;
  }

  .fd-workflow-grid {
    display: grid;
    gap: 10px;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    margin: 0.8rem 0 1.3rem;
  }

  .fd-step-card {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    min-height: 132px;
    padding: 14px 15px;
    position: relative;
  }

  .fd-step-card::before {
    background: var(--fd-accent);
    border-radius: 99px;
    content: "";
    height: calc(100% - 26px);
    left: 0;
    opacity: 0.45;
    position: absolute;
    top: 13px;
    width: 2px;
  }

  .fd-step-label {
    color: var(--fd-text3);
    font-family: var(--fd-mono);
    font-size: 0.64rem;
    letter-spacing: 0.07em;
    margin-bottom: 8px;
    text-transform: uppercase;
  }

  .fd-step-title {
    color: var(--fd-text);
    font-size: 0.92rem;
    font-weight: 600;
    line-height: 1.35;
    margin-bottom: 5px;
  }

  .fd-step-body {
    color: var(--fd-text2);
    font-size: 0.8rem;
    line-height: 1.55;
  }

  .fd-route-card {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    min-height: 118px;
    padding: 15px 16px;
  }

  .fd-route-card:hover {
    border-color: var(--fd-border2);
  }

  .fd-route-title {
    color: var(--fd-text);
    font-size: 0.93rem;
    font-weight: 600;
    line-height: 1.32;
    margin-bottom: 5px;
    overflow-wrap: anywhere;
  }

  .fd-route-body {
    color: var(--fd-text2);
    font-size: 0.8rem;
    line-height: 1.55;
  }

  .fd-pill {
    background: var(--fd-bg3);
    border: 1px solid var(--fd-border2);
    border-radius: 999px;
    color: var(--fd-text3);
    display: inline-flex;
    font-family: var(--fd-mono);
    font-size: 0.68rem;
    line-height: 1.4;
    margin: 3px 5px 3px 0;
    padding: 2px 8px;
  }

  .fd-pill-green {
    background: rgba(92, 158, 114, 0.13);
    border-color: rgba(92, 158, 114, 0.32);
    color: var(--fd-accent2);
  }

  .fd-pill-blue {
    background: rgba(74, 130, 168, 0.13);
    border-color: rgba(74, 130, 168, 0.32);
    color: #90b8d0;
  }

  .fd-pill-amber {
    background: rgba(196, 143, 63, 0.13);
    border-color: rgba(196, 143, 63, 0.32);
    color: #d4aa64;
  }

  .fd-mini-table {
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    margin: 0.7rem 0 1rem;
    overflow: hidden;
  }

  .fd-mini-row {
    display: grid;
    gap: 10px;
    grid-template-columns: 1fr 1.4fr;
    padding: 10px 12px;
  }

  .fd-mini-row:nth-child(odd) {
    background: rgba(255, 255, 255, 0.012);
  }

  .fd-mini-key {
    color: var(--fd-text);
    font-weight: 600;
  }

  .fd-mini-value {
    color: var(--fd-text2);
  }

  .fd-file-path {
    color: var(--fd-accent2);
    font-family: var(--fd-mono);
    font-size: 0.74rem;
    overflow-wrap: anywhere;
  }

  .fd-status-line {
    color: var(--fd-text3);
    font-family: var(--fd-mono);
    font-size: 0.72rem;
    margin-top: 7px;
  }

  .fd-callout {
    border: 1px solid rgba(74, 130, 168, 0.25);
    border-radius: var(--fd-radius);
    background: rgba(74, 130, 168, 0.08);
    color: #90b8d0;
    font-size: 0.86rem;
    line-height: 1.6;
    padding: 12px 15px;
  }

  .fd-pipeline-row {
    margin-bottom: 10px;
  }

  .fd-pipeline-head {
    align-items: center;
    display: flex;
    justify-content: space-between;
    margin-bottom: 5px;
  }

  .fd-pipeline-name {
    color: var(--fd-text);
    font-size: 0.86rem;
    font-weight: 500;
  }

  .fd-pipeline-count {
    color: var(--fd-text3);
    font-family: var(--fd-mono);
    font-size: 0.76rem;
  }

  .fd-progress {
    background: var(--fd-bg3);
    border: 1px solid var(--fd-border);
    border-radius: 99px;
    height: 7px;
    overflow: hidden;
  }

  .fd-progress-fill {
    background: linear-gradient(90deg, var(--fd-accent), var(--fd-accent2));
    border-radius: 99px;
    height: 100%;
  }

  .file-ok {
    color: #6dba86;
    font-weight: 600;
  }

  .file-miss {
    color: #d07070;
  }

  .section-header {
    color: var(--fd-accent2);
    font-size: 1.1em;
    font-weight: 600;
    margin: 0.5em 0 0.2em;
  }

  [data-testid="stMetric"] {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    padding: 13px 15px 11px;
  }

  [data-testid="stMetricLabel"] p {
    color: var(--fd-text3);
    font-size: 0.68rem;
    letter-spacing: 0.07em;
    text-transform: uppercase;
  }

  [data-testid="stMetricValue"] {
    color: var(--fd-text);
    font-size: 1.45rem;
    font-weight: 600;
  }

  .stTabs [data-baseweb="tab-list"] {
    border-bottom: 1px solid var(--fd-border);
    gap: 2px;
  }

  .stTabs [data-baseweb="tab"] {
    background: transparent;
    color: var(--fd-text3);
    font-family: var(--fd-font);
    font-size: 0.84rem;
    font-weight: 400;
    padding: 8px 13px;
  }

  .stTabs [aria-selected="true"] {
    color: var(--fd-accent2) !important;
    font-weight: 500;
  }

  .stTabs [data-baseweb="tab-highlight"] {
    background-color: var(--fd-accent);
  }

  [data-testid="stDataFrame"],
  [data-testid="stTable"] {
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
    overflow: hidden;
  }

  [data-testid="stExpander"] {
    background: var(--fd-bg2);
    border: 1px solid var(--fd-border);
    border-radius: var(--fd-radius);
  }

  [data-testid="stExpander"] summary:hover {
    background: var(--fd-bg3);
  }

  .stAlert {
    border-radius: var(--fd-radius);
  }

  div[data-baseweb="select"] > div,
  div[data-baseweb="input"] > div,
  textarea,
  input {
    background: var(--fd-bg2) !important;
    border-color: var(--fd-border2) !important;
    color: var(--fd-text) !important;
  }

  .stButton button,
  .stDownloadButton button {
    background: var(--fd-bg3);
    border: 1px solid var(--fd-border2);
    border-radius: var(--fd-radius);
    color: var(--fd-text2);
  }

  .stButton button:hover,
  .stDownloadButton button:hover {
    border-color: var(--fd-accent);
    color: var(--fd-accent2);
  }
</style>
"""


def apply_dark_css():
    st.markdown(DARK_CSS, unsafe_allow_html=True)


def _safe_page_link(container, page: str, label: str) -> None:
    """Render Streamlit page links, with a direct-run fallback for page QA."""
    try:
        container.page_link(page, label=label)
    except KeyError:
        container.markdown(
            f'<a class="fd-nav-link" href="{html.escape(page)}">{html.escape(label)}</a>',
            unsafe_allow_html=True,
        )


def render_top_nav() -> None:
    """Render the shared top navigation for the multipage dashboard."""
    cols = st.columns([1.35, 0.58, 0.98, 0.68, 0.75, 0.88, 0.68, 0.72, 1.05])
    cols[0].markdown('<div class="fd-navbar-brand">Forest Data Explorer</div>', unsafe_allow_html=True)
    _safe_page_link(cols[1], "app.py", "Home")
    _safe_page_link(cols[2], "pages/4_Architecture.py", "Architecture")
    _safe_page_link(cols[3], "pages/1_IDS_Survey.py", "IDS")
    _safe_page_link(cols[4], "pages/2_Climate.py", "Climate")
    _safe_page_link(cols[5], "pages/3_FIA_Forest.py", "FIA Forest")
    _safe_page_link(cols[6], "pages/6_Thermophilization.py", "Thermo")
    _safe_page_link(cols[7], "pages/5_Data_Catalog.py", "Catalog")
    _safe_page_link(cols[8], "pages/7_FIA_Navigator.py", "FIA Navigator")
    st.markdown('<div class="fd-navbar-rule"></div>', unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# Plotly dark theme helpers
# ------------------------------------------------------------------------------

PLOTLY_DARK = dict(
    plot_bgcolor="#0d1a12",
    paper_bgcolor="#0d1a12",
    font_color="#d4e8da",
    xaxis=dict(gridcolor="#1e3024", linecolor="#2a4035"),
    yaxis=dict(gridcolor="#1e3024", linecolor="#2a4035"),
)


def dark_fig(fig):
    fig.update_layout(**PLOTLY_DARK, margin=dict(l=40, r=20, t=30, b=40))
    return fig


def scatter_geo_usa(df, lat_col, lon_col, color_col, color_map=None,
                    title="", hover_name=None, size=3):
    """Plotly scatter_geo limited to USA extent."""
    kwargs = dict(lat=lat_col, lon=lon_col, color=color_col,
                  scope="usa", title=title, opacity=0.7,
                  hover_name=hover_name)
    if color_map:
        kwargs["color_discrete_map"] = color_map
    fig = px.scatter_geo(df.dropna(subset=[lat_col, lon_col, color_col]), **kwargs)
    fig.update_traces(marker_size=size)
    fig.update_layout(
        height=560,
        paper_bgcolor="#0d1a12",
        plot_bgcolor="#0d1a12",
        geo=dict(bgcolor="#0d1a12", landcolor="#162219",
                 lakecolor="#0d1a12", coastlinecolor="#2a4035",
                 showland=True, showlakes=True, showcoastlines=True),
        font_color="#d4e8da",
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(bgcolor="#111f17", bordercolor="#1e3024", borderwidth=1),
    )
    return fig


# ------------------------------------------------------------------------------
# Metric card HTML
# ------------------------------------------------------------------------------

def metric_card(label, value, sub=""):
    return (f'<div class="metric-card">'
            f'<div class="label">{label}</div>'
            f'<div class="value">{value}</div>'
            f'<div class="sub">{sub}</div>'
            f'</div>')


def page_header(kicker: str, title: str, lead: str) -> str:
    """Return the shared page header HTML."""
    return (
        f'<div class="fd-kicker">{html.escape(kicker)}</div>'
        f'<div class="fd-page-title">{html.escape(title)}</div>'
        f'<div class="fd-page-lead">{html.escape(lead)}</div>'
    )


def route_card(title: str, body: str, pills: list[str] | None = None) -> str:
    pill_html = "".join(f'<span class="fd-pill">{html.escape(pill)}</span>' for pill in (pills or []))
    return (
        '<div class="fd-route-card">'
        f'<div class="fd-route-title">{html.escape(title)}</div>'
        f'<div class="fd-route-body">{html.escape(body)}</div>'
        f'{pill_html}'
        '</div>'
    )


def route_grid(cards: list[dict]) -> str:
    return '<div class="fd-grid">' + "".join(
        route_card(card["title"], card["body"], card.get("pills")) for card in cards
    ) + '</div>'


def workflow_step(label: str, title: str, body: str) -> str:
    return (
        '<div class="fd-step-card">'
        f'<div class="fd-step-label">{html.escape(label)}</div>'
        f'<div class="fd-step-title">{html.escape(title)}</div>'
        f'<div class="fd-step-body">{html.escape(body)}</div>'
        '</div>'
    )


def workflow_grid(steps: list[dict]) -> str:
    return '<div class="fd-workflow-grid">' + "".join(
        workflow_step(step["label"], step["title"], step["body"]) for step in steps
    ) + '</div>'


def github_code_url(path: str, line: int | None = None, end_line: int | None = None) -> str:
    """Return a GitHub blob URL for a repository path, optionally anchored to lines."""
    url = f"{GITHUB_BLOB_BASE}/{path.replace(os.sep, '/')}"
    if line and end_line and end_line != line:
        return f"{url}#L{line}-L{end_line}"
    if line:
        return f"{url}#L{line}"
    return url


def plot_source_link(
    path: str,
    label: str = "Source code",
    line: int | None = None,
    end_line: int | None = None,
) -> None:
    """Render a compact GitHub link to the code that generated the displayed plot."""
    url = github_code_url(path, line=line, end_line=end_line)
    line_label = f":L{line}" if line else ""
    st.caption(f"{label}: [`{path}{line_label}`]({url})")


def plotly_chart_with_source(fig, path: str, line: int | None = None, **kwargs) -> None:
    """Render a Plotly chart followed by the code link for the chart."""
    st.plotly_chart(fig, **kwargs)
    if line is None:
        frame = inspect.currentframe()
        caller = frame.f_back if frame else None
        line = caller.f_lineno if caller else None
    plot_source_link(path, line=line)


# ------------------------------------------------------------------------------
# Parquet loaders — cached
# ------------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def load_parquet(path: str):
    """Load a parquet into pandas. Returns (df, None) or (None, error_msg)."""
    if not os.path.isfile(path):
        return None, f"Not found: {path}"
    try:
        return pd.read_parquet(path), None
    except Exception as e:
        return None, str(e)


@st.cache_data(show_spinner=False)
def parquet_meta(path: str) -> dict:
    """Return schema + row count without loading the full file."""
    if not PYARROW_AVAILABLE or not os.path.isfile(path):
        return {"exists": False, "rows": None, "columns": [], "size_mb": None}
    try:
        meta = pq.read_metadata(path)
        schema = pq.read_schema(path)
        size_mb = os.path.getsize(path) / 1e6
        return {
            "exists":   True,
            "rows":     meta.num_rows,
            "columns":  schema.names,
            "dtypes":   [str(schema.field(n).type) for n in schema.names],
            "size_mb":  size_mb,
        }
    except Exception as e:
        return {"exists": True, "rows": None, "columns": [], "size_mb": None, "error": str(e)}


@st.cache_data(show_spinner=False)
def load_sample(path: str, n: int = 5):
    """Read first n rows from a parquet without loading the whole file."""
    if not PYARROW_AVAILABLE or not os.path.isfile(path):
        return None
    try:
        pf = pq.ParquetFile(path)
        batch = pf.read_row_group(0)
        return batch.to_pandas().head(n)
    except Exception:
        return None


def file_status(path: str) -> str:
    """Return '✅' if file exists, '❌' otherwise."""
    return "✅" if os.path.isfile(path) else "❌"


# ------------------------------------------------------------------------------
# CSV loader
# ------------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def load_csv(path: str):
    if not os.path.isfile(path):
        return None, f"Not found: {path}"
    try:
        return pd.read_csv(path), None
    except Exception as e:
        return None, str(e)


# ------------------------------------------------------------------------------
# Color status helper for styled dataframes
# ------------------------------------------------------------------------------

def color_status(val):
    return "color: #3fb950" if val in ("✅", "✓") else "color: #f85149"
