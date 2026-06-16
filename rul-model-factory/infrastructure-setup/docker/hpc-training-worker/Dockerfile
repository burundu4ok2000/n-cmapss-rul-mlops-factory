# ==============================================================================
# CLOUD TRAINER WORKER - DOCKER MANIFEST (V12.0.5)
# Component: Industrial-Grade Bayesian RUL Training Engine
# 
# Purpose:
#   Provides a bit-perfect, reproducible execution environment for the 
#   vendor dataset (NASA N-CMAPSS) and Bayesian inference logic. 
#   Optimized for high-throughput CPU parallelization on GCE C2D instances.
#
# Compliance & Security Architecture:
#   - Zero Trust: Implements keyless signing via Sigstore/Cosign.
#   - Least Privilege: Operates under a non-root 'trainer' identity (UID 1000).
#   - Immutable Build: Uses fixed UV-resolved dependencies and CUDA runtimes.
# ==============================================================================

# CUDA 11.7.1 is selected as the baseline to ensure binary parity with 
# the vendored PyTorch 1.12.0 and Tyxe environments used in R&D.
FROM nvidia/cuda:11.7.1-runtime-ubuntu22.04

LABEL project="n-cmapss-mlops-rul-factory"
LABEL component="hpc-training-worker"
LABEL version="12.0.5"
LABEL maintainer="Stan_Buren"

# ------------------------------------------------------------------------------
# 1. RUNTIME CHARACTERISTICS
# ------------------------------------------------------------------------------
# Pinning Python 3.10 to match the validated R&D baseline. 
# Disabling byte-code generation and buffering ensures forensic-clean logs 
# in ephemeral Cloud Logging streams.
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app/rul-model-factory/src"

# ------------------------------------------------------------------------------
# 2. SYSTEM LAYER & FOUNDATIONAL LIBS
# ------------------------------------------------------------------------------
# We use a minimal footprint to reduce the attack surface. 
# LibGL/Glib are required for vendor-specific metrics visualization tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 \
    python3.10-dev \
    python3-distutils \
    curl \
    ca-certificates \
    git \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# UV: High-performance Rust-based package orchestrator. 
# Crucial for rapid container launches and reproducible dependency resolution.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# COSIGN: Official Sigstore binary for non-repudiable artifact signing.
# Enables EU AI Act-compliant data provenance within the training worker.
COPY --from=gcr.io/projectsigstore/cosign:v2.4.1 /ko-app/cosign /usr/local/bin/cosign

# ------------------------------------------------------------------------------
# 3. REPRODUCIBLE DEPENDENCY RESOLUTION
# ------------------------------------------------------------------------------
WORKDIR /app

# We leverage UV for surgical PIP installations. Using the cu116 index 
# explicitly to prevent resolution drift and ensure mathematical parity.
COPY infrastructure-setup/docker/hpc-training-worker/requirements.lock ./
RUN uv pip install --system setuptools==59.5.0 pydeprecate==0.3.1
RUN uv pip install \
    --system \
    --extra-index-url https://download.pytorch.org/whl/cu116 \
    --index-strategy unsafe-best-match \
    -r requirements.lock

# ------------------------------------------------------------------------------
# 4. IDENTITY & PERMISSIONS (BARRIER PATTERN)
# ------------------------------------------------------------------------------
# Implementation of a non-privileged worker identity. 
# The application logic never executes as ROOT to mitigate supply chain threats.
RUN useradd -m -u 1000 trainer \
    && mkdir -p /app/results \
    && chown -R trainer:trainer /app

# Inject source code with verified owner tags.
COPY --chown=trainer:trainer rul-model-factory/src /app/rul-model-factory/src

USER trainer

# ------------------------------------------------------------------------------
# 5. EXECUTION ORCHESTRATION
# ------------------------------------------------------------------------------
# Use the execution_controller as the conductor for all industrial training cycles.
ENTRYPOINT ["python3", "-m", "rul_model_factory.cloud_trainer.execution_controller"]

# Operational defaults (Scale-out settings for C2D/HPC instances).
# CUDA visibility is disabled by default to force CPU high-throughput mode.
CMD ["ncmapss.train_model", "--archi", "bigception", "--lr", "0.001", "--GPU", "-1", "--model-name", "cpu_hpc_v12_industrial"]
