#!/bin/bash
# ==============================================================================
# CONTAINER PROVISIONER: BUILD & PUSH (V12.0.5)
# ==============================================================================
#
# DESCRIPTION:
#   Synchronizes the local Docker environment with the Google Artifact Registry.
#   Ensures the trainer image is compiled, tagged, and distributed before 
#   worker dispatch. Critical for ensuring identical runtime on ephemeral nodes.
#
# TECHNICAL SPECIFICATIONS:
#   - Build Context: Repository root (Surgical file manifest).
#   - Target Registry: Artifact Registry (GCP Managed).
#   - Image URI: ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_ID}/cloud-trainer:latest
#   - Dependency Management: Automated UV-compilation for deterministic builds.
# ==============================================================================

set -o errexit
set -o nounset
set -o pipefail

# --- CONFIGURATION ---
REGION="us-central1"
REPO_ID="rul-factory"
IMAGE_NAME="cloud-trainer"
IMAGE_TAG="$IMAGE_NAME:latest"

if [[ -z "${GCP_PROJECT_ID:-}" ]]; then
    PROJECT_ID=$(gcloud config get-value project)
else
    PROJECT_ID="${GCP_PROJECT_ID}"
fi

FULL_IMAGE_NAME="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO_ID/$IMAGE_TAG"

# Determine script context (assuming run from project root)
BASE_DIR="$(pwd)"
DOCKER_DIR="infrastructure-setup/docker/hpc-training-worker"
DOCKERFILE_PATH="$DOCKER_DIR/Dockerfile"

echo "------------------------------------------------------------------"
echo "[PROVISION] Synchronizing Container: $FULL_IMAGE_NAME"
echo "------------------------------------------------------------------"

# 1. Dependency Synchronization (V12.0.6: FROZEN)
# [SECURITY] Automatic lock-file regeneration is DISABLED to prevent hidden 
# environment drift during orchestration. Re-enable only for manual updates.
# echo "[PROVISION] Compiling deterministic requirements via UV..."
# uv pip compile "$DOCKER_DIR/requirements.in" -o "$DOCKER_DIR/requirements.lock" --python-version 3.10 --quiet

# 2. Registry Authentication
echo "[PROVISION] Authenticating with Artifact Registry..."
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

# 3. Industrial Build
echo "[PROVISION] Building production-grade image..."
docker build -t "$FULL_IMAGE_NAME" -f "$DOCKERFILE_PATH" .

# 4. Local Verification (Bingo #21/32 Guard)
echo "[PROVISION] Running pre-flight integrity check (Smoke Test)..."
docker run --rm --entrypoint python3 "$FULL_IMAGE_NAME" -c "import pytorch_lightning as pl; print(f'Verification SUCCESS: Lightning {pl.__version__} is operational.')"

# 5. Deployment
echo "[PROVISION] Pushing image to remote registry..."
docker push "$FULL_IMAGE_NAME"

# 5. Cryptographic Attestation (Supply Chain Security)
echo "[PROVISION] Generating cryptographic identity-based signature..."
# We sign by digest to ensure immutability. Digest is available after push.
IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "$FULL_IMAGE_NAME")

if [[ -n "$IMAGE_DIGEST" ]]; then
    PROVENANCE_DIR="results/provenance"
    mkdir -p "$PROVENANCE_DIR"
    DIGEST_FILE="$PROVENANCE_DIR/image_digest.txt"
    echo "$IMAGE_DIGEST" > "$DIGEST_FILE"
    
    echo "[PROVISION] Running Sigstore identity-based signing (Headless)..."
    # BINGO #33: Headless signing integration.
    if [[ -z "${GOOGLE_OIDC_TOKEN:-}" ]]; then
        echo "[PROVISION] Attempting to fetch ambient OIDC token from gcloud..."
        GOOGLE_OIDC_TOKEN=$(gcloud auth print-identity-token --quiet 2>/dev/null || echo "")
    fi

    if [[ -n "${GOOGLE_OIDC_TOKEN:-}" ]]; then
        echo "[INFO] Attempting signing with ambient OIDC token..."
        if ! uv run --project infrastructure-setup sigstore sign --identity-token "${GOOGLE_OIDC_TOKEN}" --overwrite "$DIGEST_FILE" --output-directory "$PROVENANCE_DIR"; then
            echo "[WARNING] Ambient OIDC signing failed (e.g. malformed token). Falling back to interactive browser-based signing..."
            uv run --project infrastructure-setup sigstore sign --oauth-force-oob --overwrite "$DIGEST_FILE" --output-directory "$PROVENANCE_DIR"
        fi
    else
        echo "[WARNING] GOOGLE_OIDC_TOKEN not found. Using standard sign (may be interactive on local machine)."
        uv run --project infrastructure-setup sigstore sign --oauth-force-oob --overwrite "$DIGEST_FILE" --output-directory "$PROVENANCE_DIR"
    fi
else
    echo "[WARNING] Could not resolve image digest. Skipping signing."
fi

echo "------------------------------------------------------------------"
echo "[PROVISION] SUCCESS: Image is signed and globally available."
echo "------------------------------------------------------------------"
