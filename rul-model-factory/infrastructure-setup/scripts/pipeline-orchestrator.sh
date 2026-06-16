#!/bin/bash
# ==============================================================================
# CLOUD TRAINER ORCHESTRATOR: RUL FACTORY CORE (V12.0.6)
# Project: N-CMAPSS RUL Agentic Factory
#
# Architectural Role:
#   Central dispatch for the end-to-end Bayesian training lifecycle. 
#   Synchronizes infrastructure validation, data logistics, container 
#   provisioning, and high-performance compute (HPC) worker deployment.
#
# Technical Specifications:
#   - Resource Manager: Terraform (IAAS Lifecycle)
#   - Data Manager: GCS Ingestion (Atomic Provisioning)
#   - Compute Manager: Ephemeral GCE (Spot Instance Strategy)
#   - Result Manager: Artifact Harvesting (Structured recovery)
#   - Audit Manager: Git Metadata Collection (Provenance Tracking)
# ==============================================================================

set -o errexit
set -o nounset
set -o pipefail

# --- PATHS & CONTEXT ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
MOD_DIR="$SCRIPT_DIR"
TF_DIR="$BASE_DIR/infrastructure-setup/terraform/live/hpc-training-env"

# --- 1. ARGUMENT PARSING ---
FAST_FORWARD_SOURCE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--fast-forward)
            # Expected format: bayesian-YYYYMMDD-[hash] (e.g., from a prior run)
            FAST_FORWARD_SOURCE="$2" # [YOUR_PREVIOUS_RUN_ID]
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# --- 2. ENVIRONMENT BRIDGE (SSOT) ---
if [[ -f "$BASE_DIR/.env" ]]; then
    echo "[INFO:Orchestration] Loading environment from $BASE_DIR/.env"
    set -a
    source "$BASE_DIR/.env"
    set +a
else
    echo "[WARNING:Orchestration] .env file not found at $BASE_DIR/.env"
fi

# Mandatory Environment Injectors for Terraform
export TF_VAR_project_id="${GCP_PROJECT_ID:-}"
export TF_VAR_bucket_name="ncmapss-data-lake-${GCP_PROJECT_ID:-}"
export TF_VAR_fast_forward_source="$FAST_FORWARD_SOURCE"
export GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
export COMPUTE_TYPE="cpu_hpc"

# --- 3. RUN IDENTITY GENERATION ---
RUN_ID="bayesian-$(date +%Y%m%d)-$(openssl rand -hex 3)"
export RUN_ID
echo "[INFO:Orchestration] Session ID established: $RUN_ID"
if [[ -n "$FAST_FORWARD_SOURCE" ]]; then
    echo "[INFO:Orchestration] Fast-Forward Mode Active: Recycling artifacts from $FAST_FORWARD_SOURCE"
fi

# --- AUDIT TRACE COLLECTION ---
export GIT_COMMIT_HASH=$(git rev-parse HEAD 2>/dev/null || echo "initial-not-committed")
echo "[INFO:Audit] Identity Trace: $GIT_COMMIT_HASH"

echo "------------------------------------------------------------------"
echo "FACTORY DISPATCH: RUL MODEL PIPELINE (V12.2.0)"
echo "------------------------------------------------------------------"

# --- PHASE 1: INFRASTRUCTURE INTEGRITY ---
echo "[INFO:Orchestration] Phase 1: Validating Cloud Resources (Terraform)..."
cd "$TF_DIR"

# Secure dynamic initialization: backend bucket resolved from .env
BACKEND_BUCKET="ncmapss-terraform-state-${GCP_PROJECT_ID}"
terraform init -reconfigure -backend-config="bucket=${BACKEND_BUCKET}" > /dev/null

terraform apply -auto-approve

# --- PHASE 2: DATA LOGISTICS ---
echo "[INFO:Orchestration] Phase 2: Ingesting Data Assets (GCS)..."
cd "$BASE_DIR"
bash "$MOD_DIR/data-lake-ingestion.sh"

# --- PHASE 2.5: CONTAINER LOGISTICS ---
echo "[INFO:Orchestration] Phase 2.5: Provisioning Factory Image..."
bash "$MOD_DIR/image-build-publish.sh"

# --- PHASE 3: WORKER DISPATCH ---
echo "[INFO:Orchestration] Phase 3: Launching Ephemeral Training Worker..."
# Sourcing the provisioning script to capture WORKER_NAME and WORKER_ZONE
# shellcheck disable=SC1090
source "$MOD_DIR/worker-provisioning.sh"

# --- PHASE 3.5: LIVE TELEMETRY STREAM ---
echo "[INFO:Orchestration] Phase 3.5: Connecting to Factory Telemetry (Live Stream)..."
echo "[INFO:Orchestration] Streaming logs from $WORKER_NAME. Tail will close on instance termination."
echo "------------------------------------------------------------------"
gcloud compute instances tail-serial-port-output "$WORKER_NAME" \
    --project="$GCP_PROJECT_ID" \
    --zone="$WORKER_ZONE" \
    --port=1 || echo "[WARNING:Orchestration] Telemetry stream interrupted or worker terminated."
echo "------------------------------------------------------------------"

echo "------------------------------------------------------------------"
echo "MISSION DISPATCHED: Worker active. Monitor logs via GCE console."
echo "------------------------------------------------------------------"

# --- PHASE 4: ARTIFACT HARVESTING (Post-Execution Sink) ---
echo "[INFO:Orchestration] Phase 4: Harvesting Model Artifacts for $RUN_ID..."
# Passing worker name and the unique RUN_ID to ensure precise artifact recovery.
bash "$MOD_DIR/artifact-synchronization.sh" "$WORKER_NAME" "$RUN_ID"

echo "------------------------------------------------------------------"
echo "ORCHESTRATION COMPLETE: End-to-end cycle finalized."
echo "------------------------------------------------------------------"
