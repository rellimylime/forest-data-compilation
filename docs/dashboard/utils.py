# ==============================================================================
# docs/dashboard/utils.py
# Shared utilities for the forest-data-compilation unified dashboard
# ==============================================================================

import os
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

REPO_ROOT = Path(__file__).parent.parent.parent


def repo_path(*parts) -> Path:
    """Return an absolute path relative to the repo root."""
    return REPO_ROOT.joinpath(*parts)


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


def plot_source_link(path: str, label: str = "Source code") -> None:
    """Render a compact link to the script that generated the displayed plot."""
    st.caption(f"{label}: [`{path}`]({path})")


def plotly_chart_with_source(fig, path: str, **kwargs) -> None:
    """Render a Plotly chart followed by the code link for the chart."""
    st.plotly_chart(fig, **kwargs)
    plot_source_link(path)


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
