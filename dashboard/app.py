# Copyright 2026 Stanislav Burundukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DOMAIN: AEROSPACE-ANALYTICS
COMPONENT: DASHBOARD
SUBCOMPONENT: PRESENTATION (UI)

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    - Premium Streamlit interface for N-CMAPSS fleet analytics.
    - Three core views: Fleet Safety, Financial P&L, Engine Health Trends.
    - Zero business logic (delegated to logic.py).

COMPLIANCE:
    - Strict Separation of Concerns.
    - Caching for Performance (Cost Optimization).
    - Modular Monolith Design.
"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library
import logging
import sys
from pathlib import Path

# Allow execution from root without PYTHONPATH=src
sys.path.append(str(Path(__file__).parent.parent / "src"))

# 2. Third-Party
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# 3. Internal
from n_cmapss_pipeline.dashboard.logic import (
    derive_pnl_kpis,
    derive_safety_kpis,
    filter_by_dataset_and_unit,
    get_duckdb_path,
    load_data,
)

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

logger = logging.getLogger(__name__)

# Premium Dark Theme Palette
COLORS = {
    "background": "#0f172a",
    "text": "#e2e8f0",
    "accent_primary": "#3b82f6",  # Blue
    "accent_secondary": "#8b5cf6",  # Purple
    "critical": "#ef4444",  # Red
    "warning": "#f59e0b",  # Amber
    "normal": "#10b981",  # Green
    "panel": "#1e293b",
}

PLOTLY_LAYOUT = {
    "plot_bgcolor": "rgba(0,0,0,0)",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": COLORS["text"], "family": "Inter, sans-serif"},
    "xaxis": {"gridcolor": "#334155", "zerolinecolor": "#334155"},
    "yaxis": {"gridcolor": "#334155", "zerolinecolor": "#334155"},
    "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
}

# ==============================================================================
# CACHED DATA LOADERS
# ==============================================================================


@st.cache_data(ttl=300, show_spinner=False)
def get_cached_data(table_name: str) -> pl.DataFrame:
    """Loads and caches data from DuckDB to prevent re-querying on re-renders."""
    db_path = get_duckdb_path()
    try:
        return load_data(db_path, table_name)
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {e}")
        return pl.DataFrame()


# ==============================================================================
# REUSABLE COMPONENTS
# ==============================================================================


def styled_chart(fig: go.Figure) -> None:
    """Applies global dark theme to Plotly figures."""
    fig.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ==============================================================================
# PAGE ROUTERS
# ==============================================================================


def render_fleet_safety(
    df_safety: pl.DataFrame, dataset_ids: list[int], unit_ids: list[int]
) -> None:
    st.header("Fleet safety overview")

    # 1. Filter Data
    filtered = filter_by_dataset_and_unit(
        df_safety, dataset_ids=dataset_ids, unit_ids=unit_ids
    )

    if filtered.is_empty():
        st.warning("No safety data available for the selected filters.")
        return

    # 2. KPIs
    kpis = derive_safety_kpis(filtered)

    with st.container(horizontal=True):
        st.metric("Total engines monitored", kpis["total_engines"], border=True)
        st.metric("Critical alerts", kpis["critical_alerts"], border=True)
        st.metric("Warning alerts", kpis["warning_alerts"], border=True)

    # 3. Charts
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Alert Priority Distribution")
        priority_counts = filtered.get_column("alert_priority").value_counts()
        priority_counts.columns = ["Priority", "Count"]

        color_map = {
            "Critical": COLORS["critical"],
            "Warning": COLORS["warning"],
            "Normal": COLORS["normal"],
        }

        fig_bar = px.bar(
            priority_counts,
            x="Priority",
            y="Count",
            color="Priority",
            color_discrete_map=color_map,
        )
        styled_chart(fig_bar)

    with c2:
        st.subheader("Composite Alert Types")
        alert_types = filtered.get_column("alert_level").value_counts()
        fig_pie = px.pie(
            alert_types,
            values="count",
            names="alert_level",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        styled_chart(fig_pie)


def render_financial_pnl(
    df_pnl: pl.DataFrame, dataset_ids: list[int], unit_ids: list[int]
) -> None:
    st.header("Financial P&L impact")

    filtered = filter_by_dataset_and_unit(
        df_pnl, dataset_ids=dataset_ids, unit_ids=unit_ids
    )

    if filtered.is_empty():
        st.warning("No P&L data available for the selected filters.")
        return

    kpis = derive_pnl_kpis(filtered)

    with st.container(horizontal=True):
        st.metric(
            "Total excess fuel (gallons)",
            f"{kpis['total_excess_gallons']:,.0f}",
            border=True,
        )
        st.metric(
            "Total market penalty (USD)",
            f"${kpis['total_market_penalty_usd']:,.2f}",
            border=True,
        )

    st.subheader("Penalty Distribution by Engine Unit")
    fig_scatter = px.scatter(
        filtered,
        x="unit_id",
        y="market_penalty_usd",
        size="excess_gallons",
        color="dataset_id",
        hover_data=["unit_id", "market_penalty_usd", "excess_gallons"],
        color_continuous_scale="Purp",
    )
    # Cast x to categorical string to avoid numeric interpolation on discrete unit IDs
    fig_scatter.update_xaxes(type="category")
    styled_chart(fig_scatter)


def render_engine_health(
    df_fct: pl.DataFrame, dataset_ids: list[int], unit_ids: list[int]
) -> None:
    st.header("Engine health trends")
    st.caption("Deep dive into degradation metrics over operational cycles.")

    filtered = filter_by_dataset_and_unit(
        df_fct, dataset_ids=dataset_ids, unit_ids=unit_ids
    )

    if filtered.is_empty():
        st.warning("No operational data available for the selected filters.")
        return

    # Pick a specific unit to visualize if multiple are selected, to avoid spaghetti charts.
    unique_units = filtered.get_column("unit_id").unique()
    if len(unique_units) > 5:
        st.info("Displaying trends for the first 5 selected engines for clarity.")
        filtered = filtered.filter(pl.col("unit_id").is_in(unique_units[:5]))

    # Ensure cycle_num is sorted for line plots
    filtered = filtered.sort(["unit_id", "cycle_num"])

    # Cast unit_id to string for discrete color mapping
    filtered = filtered.with_columns(pl.col("unit_id").cast(pl.String).alias("unit_id_str"))

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("EGT Margin Erosion (°C)")
        fig1 = px.line(
            filtered, x="cycle_num", y="egt_margin_erosion_r", color="unit_id_str"
        )
        styled_chart(fig1)

    with c2:
        st.subheader("Stall Margin Ratio")
        fig2 = px.line(
            filtered, x="cycle_num", y="sm_hpc_ratio", color="unit_id_str"
        )
        # Add critical threshold line
        fig2.add_hline(
            y=1.0,
            line_dash="dash",
            line_color=COLORS["critical"],
            annotation_text="Stall Risk",
        )
        styled_chart(fig2)

    st.subheader("SFC Degradation (%)")
    fig3 = px.line(
        filtered, x="cycle_num", y="sfc_pct_degradation", color="unit_id_str"
    )
    styled_chart(fig3)


# ==============================================================================
# MAIN APPLICATION
# ==============================================================================


def main() -> None:
    st.set_page_config(
        page_title="N-CMAPSS Aerospace Analytics",
        page_icon=":material/flight_takeoff:",
        layout="wide",
        initial_sidebar_state="expanded",
    )


    # Load Data
    with st.spinner("Connecting to Native DuckDB..."):
        df_safety = get_cached_data("rpt_safety_alerts")
        df_pnl = get_cached_data("rpt_engine_pnl")
        df_fct = get_cached_data("fct_engine_health_per_cycle")

    if df_safety.is_empty() and df_pnl.is_empty() and df_fct.is_empty():
        st.error(
            "❌ Database connection failed or tables are empty. Please run `dbt build` first."
        )
        st.stop()

    # Sidebar Navigation & Filters
    st.sidebar.title("N-CMAPSS Platform")

    page = st.sidebar.radio(
        "Navigation",
        [
            ":material/security: Fleet safety",
            ":material/payments: Financial P&L",
            ":material/trending_down: Engine health trends",
        ],
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Global Filters")

    # Get unique IDs across datasets for filter options
    all_datasets = (
        sorted(df_safety.get_column("dataset_id").unique().to_list()) if not df_safety.is_empty() else []
    )
    all_units = (
        sorted(df_safety.get_column("unit_id").unique().to_list()) if not df_safety.is_empty() else []
    )

    selected_datasets = st.sidebar.multiselect(
        "Dataset ID", options=all_datasets, default=all_datasets
    )
    selected_units = st.sidebar.multiselect("Unit ID", options=all_units)

    st.sidebar.markdown("---")
    st.sidebar.caption("Data Architecture: Native DuckDB (Gold Layer)")
    st.sidebar.caption("Pipeline: dbt Unified Semantic Layer")

    # Router
    if "Fleet safety" in page:
        render_fleet_safety(df_safety, selected_datasets, selected_units)
    elif "Financial P&L" in page:
        render_financial_pnl(df_pnl, selected_datasets, selected_units)
    elif "Engine health trends" in page:
        render_engine_health(df_fct, selected_datasets, selected_units)


if __name__ == "__main__":
    main()
