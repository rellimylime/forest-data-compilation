"""
FIA Forest Explorer (v2)
========================
Educational tour of FIA's nested spatial scales: ecoregion -> plot grid ->
4-subplot cluster -> subplot -> microplot -> tree, plus a reference of every
variable measured at each scale and the processed parquet summaries derived
from FIADB.

The whole experience lives in static/v2.html. This script is a thin Streamlit
shell that hides Streamlit chrome and embeds the design as a full-window
component. Edit static/v2.html directly to iterate on the design.

Usage:
  streamlit run 05_fia/docs/dashboard/fiadb_dashboard.py
"""

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


HERE = Path(__file__).parent
HTML_PATH = HERE / "static" / "v2.html"


st.set_page_config(
    page_title="FIA Forest Explorer",
    page_icon="\U0001F332",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      [data-testid="stHeader"],
      [data-testid="stToolbar"],
      [data-testid="stSidebar"],
      [data-testid="stSidebarCollapsedControl"],
      footer { display: none !important; }
      .block-container { padding: 0 !important; max-width: 100% !important; }
      .stApp { background: #0c1610; }
      iframe { border: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

components.html(HTML_PATH.read_text(encoding="utf-8"), height=2400, scrolling=True)
