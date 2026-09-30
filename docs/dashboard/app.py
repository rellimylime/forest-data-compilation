# ==============================================================================
# docs/dashboard/app.py
# Forest Data Explorer — dashboard entrypoint and navigation
#
# Usage, from the repository root:
#   streamlit run docs/dashboard/app.py
#
# This file only declares the grouped top navigation. Each page lives in its
# own file: the Home page in home.py and the rest under pages/.
# ==============================================================================

import streamlit as st

st.set_page_config(
    page_title="Forest Data Explorer",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="collapsed",
)

NAVIGATION = {
    "": [
        st.Page("home.py", title="Home", url_path="home", default=True),
        st.Page("pages/6_Analysis.py", title="Analysis", url_path="analysis"),
    ],
    "Data": [
        st.Page("pages/5_Data_Catalog.py", title="Find data", url_path="catalog"),
        st.Page("pages/8_Query_Builder.py", title="Build a dataset", url_path="build-data"),
        st.Page("pages/3_FIA_Forest.py", title="Processed FIA data", url_path="fia"),
        st.Page("pages/7_FIA_Navigator.py", title="FIA field guide", url_path="fia-guide"),
    ],
    "Other workstreams": [
        st.Page("pages/1_IDS_Survey.py", title="IDS survey", url_path="ids"),
        st.Page("pages/2_Climate.py", title="Climate datasets", url_path="climate"),
        st.Page("pages/4_Architecture.py", title="Repository map", url_path="repository-map"),
    ],
}

st.navigation(NAVIGATION, position="top").run()
