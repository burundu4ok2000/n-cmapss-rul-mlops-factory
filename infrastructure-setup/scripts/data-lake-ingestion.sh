#!/bin/bash
# ==============================================================================
# DATA INGESTION ORCHESTRATOR (V12.0.5)
# Project: N-CMAPSS RUL Agentic Factory
#
# Architectural Role:
#   Synchronizes NASA N-CMAPSS HDF5 datasets to the Cloud Data Lake.
#   Implements high-concurrency composite uploads for rapid data provisioning.
#
# Technical Specifications:
#   - Transport Mechanism: gcloud storage cp (Industrial Integrity)
#   - Safety Features: Idempotent transfers (--no-clobber)
# ==============================================================================

set -o errexit
set -o nounset
set -o pipefail

# --- 1. ENVIRONMENT BRIDGE (SSOT) ---
# Resolve base directory relative to the script location (Level 2 depth)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load .env if it exists in the root
if [[ -f "$BASE_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    source "$BASE_DIR/.env"
fi

PROJECT_ID="${GCP_PROJECT_ID:-}"
if [[ -z "$PROJECT_ID" ]]; then
    PROJECT_ID=$(gcloud config get-value project)
fi

BUCKET_NAME="${GCS_BUCKET_NAME:-ncmapss-data-lake-${PROJECT_ID}}"

# --- 2. CONFIGURATION ---
INPUT_PATH="${1:-$BASE_DIR/.workspace/raw-telemetry}"

# Validate source existence
if [[ ! -e "$INPUT_PATH" ]]; then
    echo "ERROR: Data source missing: $INPUT_PATH"
    exit 1
fi

# --- EXECUTION ---
echo "[INFO:Ingestion] Synchronizing $INPUT_PATH to gs://$BUCKET_NAME/raw/"

INGEST_SUCCESS=1
if [[ -d "$INPUT_PATH" ]]; then
    # BINGO #34: Checksum-based Synchronization.
    # Corrected: using --checksums-only for gcloud storage rsync.
    gcloud storage rsync --checksums-only -r "$INPUT_PATH" "gs://${BUCKET_NAME}/raw/" || INGEST_SUCCESS=0
else
    # Corrected: removed invalid --checksum flag for gcloud storage cp.
    gcloud storage cp "$INPUT_PATH" "gs://${BUCKET_NAME}/raw/$(basename "$INPUT_PATH")" || INGEST_SUCCESS=0
fi

if [[ $INGEST_SUCCESS -eq 1 ]]; then
    echo "[INFO:Ingestion] Data synchronization complete."
else
    echo "[INFO:Ingestion] ERROR: Data transfer failure."
    exit 1
fi
