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
  .stApp { background-color: #0e1117; color: #ddd; }
  .block-container { padding-top: 1.2rem; }
  .metric-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 14px 18px 10px;
    margin-bottom: 8px;
  }
  .metric-card .label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: .05em; }
  .metric-card .value { font-size: 26px; font-weight: 700; color: #fff; }
  .metric-card .sub   { font-size: 11px; color: #666; margin-top: 2px; }
  .file-ok   { color: #3fb950; font-weight: 600; }
  .file-miss { color: #f85149; }
  .section-header { color: #58a6ff; font-size: 1.1em; font-weight: 600; margin: 0.5em 0 0.2em; }
</style>
"""


def apply_dark_css():
    st.markdown(DARK_CSS, unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# Plotly dark theme helpers
# ------------------------------------------------------------------------------

PLOTLY_DARK = dict(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="#ddd",
    xaxis=dict(gridcolor="#222", linecolor="#444"),
    yaxis=dict(gridcolor="#222", linecolor="#444"),
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
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        geo=dict(bgcolor="#0e1117", landcolor="#1c2128",
                 lakecolor="#0e1117", coastlinecolor="#444",
                 showland=True, showlakes=True, showcoastlines=True),
        font_color="#ddd",
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(bgcolor="#161b22", bordercolor="#30363d", borderwidth=1),
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
