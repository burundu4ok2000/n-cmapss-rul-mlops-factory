#!/bin/bash
# ==============================================================================
# WORKER DISPATCH ORCHESTRATOR (V12.0.6)
# Project: N-CMAPSS RUL Agentic Factory
#
# Architectural Role:
#   Provisions high-performance GCE Compute instances for ephemeral HPC training.
#   Dynamically resolves infrastructure blueprints via metadata filtering.
#
# Technical Specifications:
#   - Runtime: GCE (Project C2D HPC Quota)
#   - Blueprint: Automated resolution of 'ephemeral-train-template-*'
#   - Lifecycle: Ephemeral (Self-terminating cluster nodes)
#   - Audit: Metadata injection for Git Integrity (V12.0.6)
# ==============================================================================

set -e

# --- 1. Environment Resolution ---
# Resolve base directory relative to the script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load .env if it exists in the root
if [[ -f "$BASE_DIR/.env" ]]; then
    set -a
    source "$BASE_DIR/.env"
    set +a
fi

if [[ -z "${GCP_PROJECT_ID:-}" ]]; then
    PROJECT_ID=$(gcloud config get-value project)
else
    PROJECT_ID="${GCP_PROJECT_ID}"
fi

# Audit Trace Propagation: Ensure the calling environment has the hash.
GIT_COMMIT_HASH="${GIT_COMMIT_HASH:-unknown-audit-fail}"

REGION="us-central1"
ZONE="${REGION}-a"
INSTANCE_NAME="ncmapss-factory-worker-$(date +%Y%m%d-%H%M)"
RUN_ID="${RUN_ID:-unknown-run-id}"

echo "[INFO:Provisioning] Resolving latest high-performance compute blueprint..."
LATEST_TEMPLATE=$(gcloud compute instance-templates list \
    --filter="name ~ ^ephemeral-train-template-" \
    --sort-by="~creationTimestamp" \
    --limit=1 \
    --format="value(name)")

if [[ -z "$LATEST_TEMPLATE" ]]; then
    echo "[INFO:Provisioning] ERROR: No compute template found with prefix 'ephemeral-train-template-'"
    exit 1
fi

echo "[INFO:Provisioning] Selected Template: ${LATEST_TEMPLATE}"
echo "[INFO:Provisioning] Initializing worker: ${INSTANCE_NAME}"

# Launching the ephemeral HPC node from the dynamically resolved template.
# We use separate commands to avoid wiping the template's metadata (startup-script).
gcloud compute instances create "${INSTANCE_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --source-instance-template="${LATEST_TEMPLATE}"

echo "[INFO:Provisioning] Injecting audit and identity metadata..."
gcloud compute instances add-metadata "${INSTANCE_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --metadata="git-commit-hash=${GIT_COMMIT_HASH},run-id=${RUN_ID}"

echo "[INFO:Lifecycle] SUCCESS: Worker ${INSTANCE_NAME} dispatched in ${ZONE}."
echo ""

# Export for master orchestrator linkage
export WORKER_NAME="${INSTANCE_NAME}"
export WORKER_ZONE="${ZONE}"
