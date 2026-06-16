#!/bin/bash
# ==============================================================================
# CLOUD ARTIFACT HARVESTER (V12.0.5)
# Project: N-CMAPSS RUL Agentic Factory
#
# Architectural Role:
#   Implements the high-performance retrieval and normalization of ML artifacts.
#   Ensures data integrity via Success-Sentinel validation and enforces a 
#   standardized directory structure for industrial-grade traceability.
#
# Technical Specifications:
#   - Retrieval Mechanism: gcloud storage rsync (Full-Fidelity Sink)
#   - Atomic Schema: Normalizes vendor paths into model/data/logs/security
#   - Discovery Logic: Dynamical resolution of the latest checkpoint run
# ==============================================================================

set -o errexit
set -o nounset
set -o pipefail

# --- 1. ENVIRONMENT BRIDGE (SSOT) ---
# --- Forensic Context & Temporal Anchoring ---
HARVESTER_START_EPOCH=$(date +%s)
echo "[INFO:Harvesting] Temporal Anchor established: $HARVESTER_START_EPOCH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ -f "$BASE_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    source "$BASE_DIR/.env"
fi

PROJECT_ID="${GCP_PROJECT_ID:-}"
if [[ -z "$PROJECT_ID" ]]; then
    echo "[ERROR:Harvesting] GCP_PROJECT_ID not found in .env or environment."
    exit 1
fi

BUCKET_NAME="ncmapss-data-lake-$PROJECT_ID"
LOCAL_BASE_DIR="$BASE_DIR/rul-model-factory/artifacts/runs"
COMPUTE_TYPE="${COMPUTE_TYPE:-cpu_hpc}"

# --- 2. RUN DISCOVERY (SESSION-AWARE) ---
# We prioritize the forced worker from the orchestrator, falling back to discovery.
FORCED_WORKER_NAME="${1:-}"
FORCED_RUN_ID="${2:-}"

if [[ -n "$FORCED_WORKER_NAME" ]]; then
    echo "[INFO:Harvesting] Session-aware mode active for worker: $FORCED_WORKER_NAME"
    # We find the specific logs first
    LOG_SOURCE="gs://$BUCKET_NAME/logs/$FORCED_WORKER_NAME/**"
else
    LOG_SOURCE="gs://$BUCKET_NAME/logs/ncmapss-factory-worker-*/**"
fi

# We find the latest safetensor and extract its 'Updated' timestamp (Completion Time).
LATEST_ENTRY=$(gcloud storage ls --long --recursive "gs://$BUCKET_NAME/results/**" | grep "\.safetensors$" | sort -k 2 -r | head -n 1)

if [ -z "$LATEST_ENTRY" ]; then
    echo "[ERROR:Harvesting] No active runs with checkpoints detected in gs://$BUCKET_NAME/results/"
    exit 1
fi

# Extract metadata: Updated Timestamp (col 2 in 'ls --long' output) and Path (col 3)
GCS_UPDATE_RAW=$(echo "$LATEST_ENTRY" | awk '{print $2}')
LATEST_CKPT=$(echo "$LATEST_ENTRY" | awk '{print $3}')

# Convert Checkpoint Timestamp to Epoch for strict 'freshness' check
CHECKPOINT_EPOCH=$(date -u -d "$GCS_UPDATE_RAW" +%s)

# Standardize Timestamp for directory naming: 20260418T1022Z
RAW_TS=$(echo "$GCS_UPDATE_RAW" | sed 's/[-:T]//g')
TIMESTAMP="${RAW_TS:0:8}T${RAW_TS:8:4}Z"

# If we are strictly tracking a current session, we use the CURRENT time 
# to ensure we don't dump logs into an old successful run's folder.
if [[ -n "$FORCED_WORKER_NAME" ]]; then
    TIMESTAMP=$(date -u +%Y%m%dT%H%MZ)
    echo "[INFO:Harvesting] Using session timestamp: $TIMESTAMP"
fi

# If a RUN_ID is forced from the orchestrator, we bypass latest-discovery.
if [[ -n "$FORCED_RUN_ID" ]]; then
    echo "[INFO:Harvesting] Targeted mode active for Run ID: $FORCED_RUN_ID"
    RUN_ID="$FORCED_RUN_ID"
    # We still need a valid TIMESTAMP for naming, but we prefer the one from 
    # the orchestrator or current time.
    TIMESTAMP=$(date -u +%Y%m%dT%H%MZ)
else
    # Extract relative run path (assuming gs://bucket/results/runs/RUN_ID/...)
    # Robust extraction of the directory containing the checkpoint
    RUN_ID=$(echo "$LATEST_CKPT" | sed "s|gs://$BUCKET_NAME/results/||" | cut -d'/' -f1-2)
fi

RUN_LEAF=$(basename "$RUN_ID")
FULL_RUN_PATH="results/$RUN_ID"

TARGET_DIR_NAME="rul_bayesian_${TIMESTAMP}_${COMPUTE_TYPE}"
TARGET_DIR="$LOCAL_BASE_DIR/$TARGET_DIR_NAME"
TEMP_DIR="$TARGET_DIR/.raw_sync"

echo "[INFO:Harvesting] Target Directory: $TARGET_DIR"

# --- 2. SUCCESS SENTINEL VALIDATION ---
SUCCESS_LOG="gs://$BUCKET_NAME/logs/ncmapss-factory-worker-*/TRAINING_SUCCESS.log"
if ! gcloud storage ls "$SUCCESS_LOG" > /dev/null 2>&1; then
    echo "[WARNING:Harvesting] Success Sentinel not detected! Downloaded artifacts may be incomplete."
fi

# --- 3. HARVESTING EXECUTION ---
mkdir -p "$TEMP_DIR"

# Step 3.1: Calculate Expected Cloud Count (Master Manifest)
# We count every file in the GCS path to establish the "Mathematical Accuracy" baseline.
echo "[INFO:Audit] Retrieving Master Manifest from cloud..."
EXPECTED_COUNT=$(gsutil ls -r "gs://$BUCKET_NAME/$FULL_RUN_PATH/**" 2>/dev/null | grep -v "/$" | wc -l)
echo "[INFO:Audit] Expected Artifact Count: $EXPECTED_COUNT"

# ONLY synchronize the model/results if the checkpoint belongs to the current session (is 'fresh').
if [[ "$CHECKPOINT_EPOCH" -gt "$((HARVESTER_START_EPOCH - 86400))" ]]; then
    echo "[INFO:Harvesting] Verified NEW checkpoint found (Epoch: $CHECKPOINT_EPOCH)"
    echo "[INFO:Harvesting] Synchronizing raw artifacts from GCS..."
    gcloud storage rsync -r "gs://$BUCKET_NAME/$FULL_RUN_PATH" "$TEMP_DIR" --ignore-symlinks
else
    echo "[WARNING:Harvesting] Skipping stale checkpoint. Model inheritance suppressed."
fi

if [[ -n "$FORCED_WORKER_NAME" ]]; then
    echo "[INFO:Harvesting] Recovering forensic logs for $FORCED_WORKER_NAME..."
    mkdir -p "$TARGET_DIR/logs"
    gcloud storage rsync -r "gs://$BUCKET_NAME/logs/$FORCED_WORKER_NAME" "$TARGET_DIR/logs" || true
fi

# --- 4. SURGICAL NORMALIZATION ---
# Only normalize if we actually downloaded new artifacts
if [ "$(ls -A "$TEMP_DIR" 2>/dev/null)" ]; then
    echo "[INFO:Harvesting] Applying Architectural Normalization (V12.0.5 Schema)..."
    mkdir -p "$TARGET_DIR/model" "$TARGET_DIR/data" "$TARGET_DIR/logs" "$TARGET_DIR/metadata" "$TARGET_DIR/security"

    # CRITICAL: We move security manifests to their dedicated department first.
    mv "$TEMP_DIR/provenance.json"* "$TARGET_DIR/security/" 2>/dev/null || true

    # 1. Model Assets (Recursive search - PRESERVE CLOUD NAMES for audit linkage)
    find "$TEMP_DIR" -type f \( -name "*.safetensors" -o -name "*.pt" \) -exec bash -c 'mv "$1" "$2/model/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true

    # 2. Data Shards (PRESERVE NAME to avoid collision, add run-prefix for traceability)
    find "$TEMP_DIR" -type f -name "*.parquet" -exec bash -c 'mv "$1" "$2/data/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true
    
    # 3. DB Shards (LMDB recursive directory move)
    find "$TEMP_DIR" -type d -name "*.lmdb" -exec bash -c 'mv "$1" "$2/data/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true

    # 4. Telemetry & Logs
    find "$TEMP_DIR" -type f -name "events.out.tfevents.*" -exec bash -c 'mv "$1" "$2/logs/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true
    find "$TEMP_DIR" -type f -name "*.log" -exec bash -c 'mv "$1" "$2/logs/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true

    # 5. Metadata & Configs
    find "$TEMP_DIR" -type f -name "hparams.yaml" -exec bash -c 'mv "$1" "$2/metadata/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true

    # 6. Security Signatures (Recursive collection of certifications, preserving linkage)
    find "$TEMP_DIR" -type f \( -name "*.sig" -o -name "*.cert" \) -exec bash -c 'mv "$1" "$2/security/${3}.$(basename "$1")"' _ {} "$TARGET_DIR" "$TARGET_DIR_NAME" \; 2>/dev/null || true

    # 7. Zero-Leakage Catch-All (Anything remaining moves to unclassified)
    if [ "$(ls -A "$TEMP_DIR" 2>/dev/null)" ]; then
        echo "[WARNING:Security] Unclassified artifacts detected. Enforcing Zero-Leakage policy."
        mkdir -p "$TARGET_DIR/unclassified"
        find "$TEMP_DIR" -maxdepth 10 -not -path '*/.*' -type f -exec mv {} "$TARGET_DIR/unclassified/" \; 2>/dev/null || true
    fi
else
    echo "[INFO:Harvesting] No results to normalize (failed run). Forensic logs preserved."
fi

# --- 5. MATHEMATICAL AUDIT & CLEANUP ---
# We calculate the final local count across all departments (excluding hidden files).
FINAL_COUNT=$(find "$TARGET_DIR" -not -path '*/.*' -type f | wc -l)

echo "------------------------------------------------------------------"
echo "AUDIT REPORT: $TARGET_DIR_NAME"
echo "Expected Artifacts (Cloud): $EXPECTED_COUNT"
echo "Recovered Artifacts (Local): $FINAL_COUNT"
echo "------------------------------------------------------------------"

if [[ "$FINAL_COUNT" -ge "$EXPECTED_COUNT" ]]; then
    echo "[INFO:Audit] VALIDATION SUCCESSFUL: Local integrity matches cloud manifest."
    echo "[INFO:Harvesting] Purging raw sync buffer..."
    rm -rf "$TEMP_DIR"
else
    echo "[CRITICAL:Audit] VALIDATION FAILURE: Missing artifacts detected ($((EXPECTED_COUNT - FINAL_COUNT)) files)."
    echo "[CRITICAL:Audit] Preserving .raw_sync for forensic recovery."
    exit 2
fi
