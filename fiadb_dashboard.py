"""
Interactive dashboard for exploring the Forest Inventory and Analysis (FIA) Database.

This script uses Streamlit to provide an interface for exploring the schema and
relationships of a FIADB SQLite database. It also offers basic table viewers
and an entity–relationship (ER) diagram to help unfamiliar users understand how
the core tables connect (e.g., PLOT → COND → TREE → SEEDLING).

Usage:
  streamlit run fiadb_dashboard.py

Requires:
  - Python 3
  - streamlit
  - pandas
  - graphviz (for ER diagrams)
  - altair (optional, for charting)

The dashboard accepts a local path to a FIADB SQLite database. You can
download state or national FIADB SQLite files from the FIA DataMart and then
explore them here. If no file is provided, an in‑memory SQLite connection
without data is used and the ER diagram is drawn based on predefined
relationships.

Note: The FIADB SQLite files distributed via DataMart do not always declare
foreign key constraints, so relationships are inferred using conventional
column names (e.g., PLOT.CN ↔ COND.PLT_CN, COND.PLT_CN & COND.CONDID ↔ TREE.PLT_CN &
TREE.CONDID).
"""
import os
import sqlite3
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

# Try to import graphviz for ER diagram
try:
    from graphviz import Digraph
    _GRAPHVIZ_AVAILABLE = True
except ImportError:
    _GRAPHVIZ_AVAILABLE = False


def get_sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    """Return a list of table names in the SQLite database."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_')]
    return sorted(tables)


def get_table_preview(conn: sqlite3.Connection, table: str, n: int = 100) -> pd.DataFrame:
    """Fetch the first n rows of a table for preview."""
    query = f"SELECT * FROM {table} LIMIT {n}"
    return pd.read_sql_query(query, conn)


def infer_relationships(tables: List[str]) -> List[Tuple[str, str, str]]:
    """
    Infer relationships between tables based on common column names.

    Returns a list of (source_table, target_table, key_field) tuples
    representing directed edges in an ER diagram. The direction
    corresponds to the primary → foreign key relationship.
    """
    edges = []

    # Define a simple set of known keys for major tables
    # Each entry maps a child table to a tuple of (parent table, child_key, parent_key)
    known_relationships = {
        'COND': ('PLOT', 'PLT_CN', 'CN'),
        'SUBPLOT': ('PLOT', 'PLT_CN', 'CN'),
        'SUBP_COND': ('PLOT', 'PLT_CN', 'CN'),
        'SUBP_COND_CHNG_MTRX': ('PLOT', 'PLT_CN', 'CN'),
        'TREE': ('COND', 'CONDID', 'CONDID'),
        'SEEDLING': ('COND', 'CONDID', 'CONDID'),
        'SITETREE': ('COND', 'CONDID', 'CONDID'),
        'TREE_WOODLAND_STEMS': ('TREE', 'TRE_CN', 'CN'),
        'TREE_REGIONAL_BIOMASS': ('TREE', 'TRE_CN', 'CN'),
        'TREE_GRM_COMPONENT': ('TREE', 'TRE_CN', 'CN'),
        'TREE_GRM_MIDPT': ('TREE', 'TRE_CN', 'CN'),
        'TREE_GRM_BEGIN': ('TREE', 'TRE_CN', 'CN'),
        'TREE_GRM_ESTN': ('TREE', 'TRE_CN', 'CN'),
        'POP_PLOT_STRATUM_ASSGN': ('PLOT', 'PLT_CN', 'CN'),
    }

    for child, (parent, child_key, parent_key) in known_relationships.items():
        if child in tables and parent in tables:
            edges.append((parent, child, child_key))

    return edges


def draw_er_diagram(edges: List[Tuple[str, str, str]]) -> Digraph:
    """
    Create a Graphviz Digraph representing table relationships.
    """
    dot = Digraph(comment='FIA Database Schema', format='png')
    dot.attr('node', shape='box')

    # Collect unique nodes
    nodes = set()
    for parent, child, _ in edges:
        nodes.add(parent)
        nodes.add(child)

    # Add nodes
    for node in sorted(nodes):
        dot.node(node)

    # Add edges with key labels
    for parent, child, key in edges:
        dot.edge(parent, child, label=key)

    return dot


def main() -> None:
    st.set_page_config(page_title="FIA Database Explorer", layout="wide")
    st.title("Forest Inventory and Analysis (FIA) Database Explorer")
    st.write(
        "This interactive dashboard allows you to browse the structure and contents "
        "of a FIADB SQLite database. Upload a state or national database below to "
        "begin exploring tables, view the entity–relationship diagram, and visualize "
        "basic summaries."
    )

    uploaded_file = st.file_uploader(
        "Upload FIADB SQLite file", type=["sqlite", "db", "db3", "sqlite3"],
        help="Download a state or national FIADB SQLite file from the FIA DataMart and upload it here."
    )

    if uploaded_file is not None:
        # Save uploaded file to a temporary location
        tmp_path = os.path.join(st.experimental_get_query_params().get('tmpdir', ['/tmp'])[0], uploaded_file.name)
        with open(tmp_path, 'wb') as f:
            f.write(uploaded_file.read())
        conn = sqlite3.connect(tmp_path)
    else:
        # Create an empty in-memory database
        conn = sqlite3.connect(':memory:')

    # Extract tables
    tables = get_sqlite_tables(conn)

    st.subheader("Available Tables")
    if tables:
        st.write(f"Found **{len(tables)}** tables in the database.")
        selected_table = st.selectbox("Select a table to view", tables)
        if selected_table:
            df_preview = get_table_preview(conn, selected_table)
            st.write(f"Preview of `{selected_table}` (first 100 rows):")
            st.dataframe(df_preview)
            st.write(f"Number of columns: {len(df_preview.columns)}")
    else:
        st.warning("No tables found. Please upload a valid FIADB SQLite file.")

    # Draw ER diagram
    st.subheader("Entity–Relationship Diagram")
    if _GRAPHVIZ_AVAILABLE:
        edges = infer_relationships(tables)
        if edges:
            diagram = draw_er_diagram(edges)
            st.graphviz_chart(diagram)
        else:
            st.info(
                "Could not infer relationships automatically. Only basic table list is displayed."
            )
    else:
        st.info("Graphviz is not installed. Install graphviz to see the ER diagram.")

    # Example summary visualization
    st.subheader("Explore Tree Counts by County (Example)")
    if uploaded_file is not None and 'TREE' in tables and 'PLOT' in tables:
        try:
            # Ensure county code exists
            tree_counts = pd.read_sql_query(
                """
                SELECT p.STATECD, p.COUNTYCD, COUNT(t.PLTCN) as NUM_TREES
                FROM PLOT p
                JOIN TREE t ON p.CN = t.PLT_CN
                GROUP BY p.STATECD, p.COUNTYCD
                LIMIT 500
                """, conn
            )
            import altair as alt

            chart = (
                alt.Chart(tree_counts)
                .mark_bar()
                .encode(
                    x='NUM_TREES:Q',
                    y=alt.Y('COUNTYCD:N', sort='-x'),
                    color='STATECD:N'
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        except Exception as e:
            st.info(f"Could not produce chart: {e}")
    else:
        st.info("Upload a database containing both PLOT and TREE tables to visualize tree counts.")

    st.caption(
        "This dashboard is a prototype for exploring FIADB data. It demonstrates how "
        "to browse tables, visualize relationships, and build simple summaries. "
        "You can extend it by adding additional charts, filters, or by integrating "
        "spatial visualization libraries such as Folium or KeplerGL."
    )


if __name__ == "__main__":
    main()
