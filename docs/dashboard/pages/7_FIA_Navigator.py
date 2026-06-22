# ==============================================================================
# pages/7_FIA_Navigator.py
# Embed the static FIA visual explainer inside the Streamlit dashboard.
# ==============================================================================

from pathlib import Path
import sys

import streamlit as st
import streamlit.components.v1 as components

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, render_top_nav, repo_path


st.set_page_config(page_title="FIA Navigator", page_icon="🌲", layout="wide")
apply_dark_css()
render_top_nav()

HTML_PATH = repo_path("docs", "fia-explorer.html")

st.markdown(
    """
    <div class="fd-page-title">FIA Navigator</div>
    <div class="fd-page-lead">
      Static visual guide to FIA plot design, sampling grain, and FIADB tables.
    </div>
    """,
    unsafe_allow_html=True,
)

if not HTML_PATH.is_file():
    st.error(f"Could not find `{HTML_PATH}`.")
else:
    html_doc = HTML_PATH.read_text(encoding="utf-8")
    components.html(html_doc, height=950, scrolling=True)
