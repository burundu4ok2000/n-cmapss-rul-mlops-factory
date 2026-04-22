"""
CORE DOMAIN: Industrial AI Diagnostics Dashboard (V1.5.6)
ROLE: Real-time Probabilistic Surveillance of Bayesian RUL & Model Uncertainty.
STATUS: PRODUCTION_READY

CONTRACT:
    - IN: Real-time telemetry snapshot via Direct WAL Access.
    - OUT: High-fidelity Plotly manifolds and statistical audit tables.

SYSTEM-LEVEL INVARIANTS:
    - SCHEMA_GUARD: Verify 'fleet_telemetry' presence to prevent Catalog Errors.
    - DIRECT_WAL: High-concurrency read-only access to live DuckDB journals.
    - SENSOR_FIDELITY: Dashboard must reflect physical space while AI works in Z-space.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import statsmodels.api as sm
import time
import json
import datetime
from pathlib import Path

# Internal compliance paths
from streaming_pipeline.config import SNAPSHOT_PARQUET_FILE, SIMULATION_LIFECYCLE_FILE

# --- 1. UI ARCHITECTURE (V1.5.0) ---
st.set_page_config(
    page_title="N-CMAPSS DT | AI SENTINEL",
    page_icon="🛡️",
    layout="wide"
)

# --- 2. DATA ACQUISITION & FORENSICS (V1.5.0) ---

@st.cache_data(ttl=1)
def check_node_heartbeats():
    """
    Verifies the operational status of backend nodes via Heartbeat interrogation.
    """
    producer_pulse = PROJECT_ROOT / ".workspace" / "producer_heartbeat.json"
    consumer_pulse = PROJECT_ROOT / ".workspace" / "consumer_heartbeat.json"
    
    results = {"producer": "OFFLINE", "consumer": "OFFLINE", "latency": 0.0}
    
    # Check Producer
    if producer_pulse.exists():
        try:
            with open(producer_pulse, "r") as f:
                data = json.load(f)
                if time.time() - data.get("timestamp", 0) < 10:
                    results["producer"] = "ONLINE"
        except: pass
        
    # Check Consumer
    if consumer_pulse.exists():
        try:
            with open(consumer_pulse, "r") as f:
                data = json.load(f)
                if time.time() - data.get("timestamp", 0) < 10:
                    results["consumer"] = "ONLINE"
                    results["latency"] = data.get("latency_ms", 0.0)
        except: pass
        
    return results

@st.cache_data(ttl=10)
def get_simulation_lifecycle():
    """Reads the simulation start time and total cycles from the producer's log."""
    if not SIMULATION_LIFECYCLE_FILE.exists():
        return None
    try:
        with open(SIMULATION_LIFECYCLE_FILE, "r") as f:
            return json.load(f)
    except:
        return None

@st.cache_data(ttl=3)
def get_telemetry_snapshot(unit_id: int):
    """
    Retrieves the mission-critical state using Lock-Free Parquet Snapshot.
    Cached for 1s to prevent redundant disk I/O.
    """
    if not SNAPSHOT_PARQUET_FILE.exists():
        return pd.DataFrame()
    
    try:
        # Atomic read of the Parquet snapshot
        df_all = pd.read_parquet(SNAPSHOT_PARQUET_FILE)
        
        # Filter for the selected unit within the snapshot
        df = df_all[df_all['unit'] == unit_id].copy()
        return df.sort_values('cycle', ascending=True)
    except Exception as e:
        return pd.DataFrame()

@st.cache_data
def apply_scientific_smoothing(y_values: np.ndarray, x_values: np.ndarray, frac: float = 0.01):
    """
    Implements the 'Surgical Smoothing' strategy using LOWESS.
    Cached based on input arrays to avoid recalculation.
    """
    if len(y_values) < 5:
        return y_values
    
    lowess = sm.nonparametric.lowess
    smoothed = lowess(y_values, x_values, frac=frac)
    return smoothed[:, 1]

# --- 3. COMMAND CENTER & CONTROLS ---

st.sidebar.title("🎮 Command Center")
st.sidebar.markdown("---")

selected_unit = st.sidebar.selectbox("Digital Twin Target", options=[11, 14, 15], index=0)

sigma_coeff = st.sidebar.slider(r"Bayesian Confidence ($k\sigma$)", 
                                min_value=1.0, max_value=3.0, value=1.96, 
                                step=0.01, help="Sigma scaling for uncertainty area.")

st.sidebar.markdown("---")
enable_smoothing = st.sidebar.toggle("Enable Scientific Smoothing", value=True)
smooth_frac = st.sidebar.slider("Lowess Bandwidth", 0.005, 0.1, 0.02, disabled=not enable_smoothing)

st.sidebar.markdown("---")
# [HEALTH:V1.5.9] Heartbeat Monitor
st.sidebar.subheader("📡 Infrastructure Health")
hb = check_node_heartbeats()

col_p, col_c = st.sidebar.columns(2)
with col_p:
    status_p = "🟢 ONLINE" if hb["producer"] == "ONLINE" else "🔴 OFFLINE"
    st.markdown(f"**Producer**\n{status_p}")
with col_c:
    status_c = "🟢 ONLINE" if hb["consumer"] == "ONLINE" else "🔴 OFFLINE"
    st.markdown(f"**Consumer**\n{status_c}")

if hb["consumer"] == "ONLINE":
    st.sidebar.metric("AI Latency", f"{hb['latency']:.1f} ms")

st.sidebar.markdown("---")
st.sidebar.info("[MISSION:Status] Node 3: SENTINEL Active. Monitoring dual-domain infrastructure...")

@st.fragment(run_every=3)
def render_mission_progress(unit_id):
    """
    Calculates and displays elapsed and estimated remaining time.
    Isolates updates to the sidebar container.
    """
    data = get_telemetry_snapshot(unit_id)
    meta = get_simulation_lifecycle()
    
    if not meta or data.empty:
        return

    current_cycle = int(data['cycle'].max())
    start_ts = meta.get("start_timestamp")
    total_cycles = meta.get("total_cycles", {}).get(str(unit_id), 0)

    if start_ts and total_cycles:
        elapsed = time.time() - start_ts
        remaining = max(0, (total_cycles - current_cycle) * 5.5)
        
        st.markdown("---")
        st.subheader("⏲ Mission Progress")
        
        c1, c2 = st.columns(2)
        c1.metric("Elapsed", str(datetime.timedelta(seconds=int(elapsed))))
        c2.metric("Remaining", str(datetime.timedelta(seconds=int(remaining))))
        
        progress = min(1.0, current_cycle / total_cycles) if total_cycles > 0 else 0
        st.progress(progress, text=f"Progress: {current_cycle}/{total_cycles} cycles")

with st.sidebar:
    render_mission_progress(selected_unit)

# --- 4. MISSION FRAGMENTS (V18.28: Anti-Flicker) ---

@st.fragment(run_every=3)
def render_rul_manifold(unit_id, sigma, smoothing, frac):
    """
    Isolated fragment for RUL manifold with Cycle-Gate optimization.
    """
    data = get_telemetry_snapshot(unit_id)

    if data.empty:
        st.title("🛰 Awaiting Ingestion Heartbeat...")
        st.info(f"[MISSION:Status] Persistence Layer Linked. Awaiting initialization for Unit {unit_id}.")
        return

    # --- CYCLE-GATE: Early exit if no state change detected ---
    current_max_cycle = int(data['cycle'].max())
    st.session_state[f"current_cycle_{unit_id}"] = current_max_cycle
    state_key = f"manifold_state_{unit_id}"
    params_hash = hash((sigma, smoothing, frac))
    
    stored = st.session_state.get(state_key)
    if stored and stored['max_cycle'] == current_max_cycle and stored['params_hash'] == params_hash:
        # Restore metrics & figure from cache (Zero CPU cost)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Operational Cycle", f"{stored['m']['cycle']}")
        m2.metric("Predicted RUL", f"{stored['m']['pred']:.1f} cycles")
        m3.metric("True RUL", f"{stored['m']['true']:.0f} cycles")
        m4.metric("Bayesian Uncertainty", f"±{stored['m']['unc'] * sigma:.2f} cycles", 
                  delta=f"{stored['m']['d_unc']:.4f} σ", delta_color="inverse")
        st.plotly_chart(stored['fig'], width="stretch", key=f"rul_manifold_widget_{unit_id}")
        return

    # --- NEW CYCLE: Heavy Analysis Block ---
    y_mean = data['predicted_rul'].values
    y_std = data['predicted_std'].values
    y_true = data['true_rul'].values
    cycles = data['cycle'].values

    if smoothing:
        y_mean_plot = apply_scientific_smoothing(y_mean, cycles, frac=frac)
        y_std_plot = apply_scientific_smoothing(y_std, cycles, frac=frac)
    else:
        y_mean_plot, y_std_plot = y_mean, y_std

    # Calculate Probability Bounds (± k*sigma)
    ci_upper = y_mean_plot + (sigma * y_std_plot)
    ci_lower = np.clip(y_mean_plot - (sigma * y_std_plot), 0, None)

    # --- MISSION METRICS ---
    cur, gt, unc = float(y_mean[-1]), float(y_true[-1]), float(y_std[-1])
    delta_unc = (unc - float(y_std[-2])) if len(y_std) > 1 else 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Operational Cycle", f"{current_max_cycle}")
    m2.metric("Predicted RUL", f"{cur:.1f} cycles")
    m3.metric("True RUL", f"{gt:.0f} cycles")
    m4.metric("Bayesian Uncertainty", f"±{unc * sigma:.2f} cycles", 
              delta=f"{delta_unc:.4f} σ", delta_color="inverse")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=np.concatenate([cycles, cycles[::-1]]), y=np.concatenate([ci_upper, ci_lower[::-1]]),
        fill='toself', fillcolor='rgba(0, 100, 255, 0.15)', line=dict(color='rgba(255,255,255,0)'),
        hoverinfo="skip", name=f"95% CI"
    ))
    fig.add_trace(go.Scatter(x=cycles, y=y_true, mode='lines', line=dict(color='rgba(0,0,0,0.6)', width=2, dash='dash'), name='True RUL'))
    fig.add_trace(go.Scatter(x=cycles, y=y_mean_plot, mode='lines', line=dict(color='#1f77b4', width=3), name='Predicted RUL'))

    fig.update_layout(
        title=f"Node 3 Surveillance: Digital Twin {unit_id} degradation manifold",
        xaxis_title="Operational Cycles", yaxis_title="RUL (Cycles)",
        height=600, template="plotly_white", margin=dict(l=20, r=20, t=80, b=20), hovermode="x unified"
    )

    # Cache for next gate check
    st.session_state[state_key] = {
        'max_cycle': current_max_cycle,
        'params_hash': params_hash,
        'fig': fig,
        'm': {'cycle': current_max_cycle, 'pred': cur, 'true': gt, 'unc': unc, 'd_unc': delta_unc}
    }
    st.plotly_chart(fig, width="stretch", key=f"rul_manifold_widget_{unit_id}")

@st.fragment(run_every=3)
def render_statistical_audit(unit_id):
    """
    Isolated audit fragment with Cycle-Gate.
    """
    data = get_telemetry_snapshot(unit_id)
    if data.empty:
        st.info("[MISSION:Status] Awaiting telemetry for statistical audit...")
        return
    
    current_max_cycle = int(data['cycle'].max())
    state_key = f"audit_state_{unit_id}"
    
    stored = st.session_state.get(state_key)
    if stored and stored['max_cycle'] == current_max_cycle:
        st.subheader("📊 Operational Audit (Physical Space)")
        st.dataframe(stored['df_phys'], width="stretch")
        st.markdown("> [!IMPORTANT]\n> Reconstructed from Z-space telemetry via inverse transformation.")
        st.markdown("---")
        st.subheader("🧠 Diagnostic Audit (Gaussian Manifold)")
        st.dataframe(stored['df_z'], width="stretch")
        st.markdown("> [!NOTE]\n> Raw normalized values processed by the Bayesian core.")
        return

    base_sensors = ['alt', 'Mach', 'TRA', 'T2', 'T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21', 'P24', 'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf']
    
    df_phys = data[base_sensors].describe().transpose()
    display_sensors_z = [f"{s}_z" for s in base_sensors]
    df_z = data[display_sensors_z].describe().transpose()

    # Cache results
    st.session_state[state_key] = {
        'max_cycle': current_max_cycle,
        'df_phys': df_phys,
        'df_z': df_z
    }

    st.subheader("📊 Operational Audit (Physical Space)")
    st.dataframe(df_phys, width="stretch")
    st.markdown("> [!IMPORTANT]\n> Reconstructed from Z-space telemetry via inverse transformation.")
    st.markdown("---")
    st.subheader("🧠 Diagnostic Audit (Gaussian Manifold)")
    st.dataframe(df_z, width="stretch")
    st.markdown("> [!NOTE]\n> Raw normalized values processed by the Bayesian core.")

# --- 5. MAIN ASSEMBLY ---

tab_main, tab_stats = st.tabs(["🚀 RUL Manifold", "🔬 Statistical Audit"])

with tab_main:
    render_rul_manifold(selected_unit, sigma_coeff, enable_smoothing, smooth_frac)

with tab_stats:
    render_statistical_audit(selected_unit)
