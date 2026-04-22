#!/bin/bash
# ==============================================================================
# INDUSTRIAL DIGITAL TWIN ORCHESTRATOR: N-CMAPSS STREAMING (V1.5.8)
# Project: N-CMAPSS RUL Agentic Factory
#
# Architectural Role:
#   Master control dispatch for the End-to-End Real-time Digital Twin.
#   Ensures a deterministic "Fresh Start" by purging legacy processes,
#   sanitizing the persistence layer, and synchronizing operational nodes.
#
# Technical Specifications:
#   - Infrastructure: Redpanda (Kafka-compatible) in Docker.
#   - Persistence: DuckDB (Sovereign workspace sink).
#   - Execution Pattern: Context-Aware Barrier Command (Node 0 -> Node 3).
#   - Isolation: Surgical process termination via absolute path filtering.
# ==============================================================================

set -o errexit
set -o nounset
set -o pipefail

# --- PATHS & CONTEXT ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJ_DIR="streaming_pipeline/streaming_pipeline"
PERSISTENCE_DIR="$BASE_DIR/.workspace/persistence"
REDPANDA_CONTAINER="redpanda-ncmapss"

# --- 1. SURGICAL UTILITIES (V18.10) ---

wait_for_infrastructure() {
    local max_retries=30
    local count=0
    echo -n "[INFO:Orchestration] Awaiting Infrastructure Heartbeat (Redpanda)..."
    until docker exec "$REDPANDA_CONTAINER" rpk cluster info > /dev/null 2>&1; do
        exit_code=$?
        count=$((count + 1))
        if [ $count -ge $max_retries ]; then
            echo -e "\n[CRITICAL:Infra] Redpanda failed to respond after $max_retries attempts (Exit: $exit_code)."
            exit 1
        fi
        echo -n "."
        sleep 1
    done
    echo " [CONNECTED]"
}

kill_surgical() {
    # Terminates processes ONLY if they belong to this project instance.
    # Uses absolute path filtering to avoid crosstalk with external tasks.
    local pattern=$1
    echo "[AUDIT:Cleanup] Searching for $pattern in $BASE_DIR context..."
    
    # 1. pgrep -af prints FULL command lines
    # 2. grep "$BASE_DIR" ensures the process originated from this specific factory
    # 3. awk extracts the PID
    # 4. xargs executes kill -9 on identified project-specific PIDs
    pgrep -af "$pattern" | grep "$BASE_DIR" | awk '{print $1}' | xargs -r kill -9 || true
}

# --- 2. MISSION CLEANUP TRAP ---
# Ensures all background operational nodes power down on exit
cleanup() {
    echo -e "\n[MISSION:Status] Initiating secure session shutdown..."
    
    # Kill background jobs started by this SPECIFIC shell session
    # (Isolated by Linux job control)
    jobs -p | xargs -r kill -9 2>/dev/null || true
    
    # Final surgical sweep to ensure no orphan nodes remain in this BASE_DIR
    kill_surgical "src/streaming_pipeline/consumer.py"
    kill_surgical "src/streaming_pipeline/producer.py"
    
    # [CLEANSE] Remove heartbeats to signal OFFLINE to any ghost dashboards
    rm -f "$BASE_DIR/.workspace"/*_heartbeat.json
    
    echo "[MISSION:Status] All project-specific nodes powered down safely."
}
trap cleanup EXIT

echo "------------------------------------------------------------------"
echo "DIGITAL TWIN DISPATCH: N-CMAPSS STREAMING (V1.5.9)"
echo "------------------------------------------------------------------"

# --- PHASE 0: SYSTEMATIC PURGE (CONTEXT-AWARE) ---
echo "[INFO:Orchestration] Phase 0: Sanitizing environment..."

# 0. Environment Guard
if ! command -v uv &> /dev/null; then
    echo "[CRITICAL:Env] 'uv' not found. Please install uv for deterministic execution."
    exit 1
fi

# 1. Surgical Process Liquidation
echo "[AUDIT:Cleanup] Purging project-specific legacy nodes..."
kill_surgical "src/streaming_pipeline/consumer.py"
kill_surgical "src/streaming_pipeline/producer.py"
kill_surgical "streamlit run.*dashboard.py"

# 2. Infrastructure Power-Cycle (Hard Purge)
echo "[AUDIT:Infrastructure] Liquidating legacy telemetry bus: $REDPANDA_CONTAINER"
docker exec "$REDPANDA_CONTAINER" rpk topic delete telemetry.live > /dev/null 2>&1 || true
docker exec "$REDPANDA_CONTAINER" rpk topic create telemetry.live > /dev/null 2>&1 || true

# 3. Persistence & Pulse Sterilization
echo "[AUDIT:Persistence] Purging analytical sink: $PERSISTENCE_DIR"
rm -rf "$PERSISTENCE_DIR"/*.db*
rm -f "$BASE_DIR/.workspace"/*_heartbeat.json
mkdir -p "$PERSISTENCE_DIR"

# Wait for Redpanda readiness
wait_for_infrastructure

# --- PHASE 1: MISSION DATA STAGING (NODE 0) ---
echo "[INFO:Orchestration] Phase 1: Executing Golden Data Staging (Node 0)..."
uv run --project "$PROJ_DIR" python "$PROJ_DIR/src/streaming_pipeline/ds02-006-preprocessing.py"

# --- PHASE 2: DIGITAL TWIN CORE DEPLOYMENT (NODES 2 & 1) ---
echo "[INFO:Orchestration] Phase 2: Launching Operational Cores..."

# 1. AI Inference Node (Node 2)
echo "[MISSION:Inference] Deploying Bayesian Engine (Background)..."
uv run --project "$PROJ_DIR" python "$PROJ_DIR/src/streaming_pipeline/consumer.py" &

sleep 2 # Stabilization delay for DuckDB initialization

# 2. Telemetry Simulation Node (Node 1)
echo "[MISSION:Streaming] Deploying Telemetry Simulator (Background)..."
uv run --project "$PROJ_DIR" python "$PROJ_DIR/src/streaming_pipeline/producer.py" &

# --- PHASE 3: SENTINEL UI DEPLOYMENT (NODE 3) ---
echo "[INFO:Orchestration] Phase 3: Launching Sentinel Dashboard (Node 3)..."
echo "------------------------------------------------------------------"
echo "MISSION ACTIVE: Monitor Digital Twin via Streamlit window."
echo "------------------------------------------------------------------"

uv run --project "$PROJ_DIR" streamlit run "$PROJ_DIR/src/streaming_pipeline/dashboard.py"
