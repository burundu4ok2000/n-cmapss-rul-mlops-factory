# 🛡️ N-CMAPSS Telemetry Factory: Predictive Maintenance Digital Twin

**Industrial-grade real-time streaming pipeline for aircraft engine Remaining Useful Life (RUL) estimation using Bayesian Inference.**

This project is a **uv monorepo** containing three isolated packages with 1-click orchestrators for cloud training and live inference. It implements a **Bayesian VI (Flipout)** model for RUL with uncertainty quantification, built with **Zero-Trust**, **SafeTensors**, and **Sigstore** to align with **EU AI Act** and **DORA** guidelines for high-risk AI systems.

---

## 🎯 The Industrial Challenge

Bridging the gap between academic research and safety-critical aerospace deployment requires addressing three fundamental pillars:

### 1. Operational Blindness (Flight Class Mismatch)
*   **The Challenge**: SOTA models are often trained on idealized long-haul data (**Class 3**), but fail catastrophically on high-intensity short-haul missions (**Class 1/2**), providing overly optimistic life estimates without error awareness.
*   **Our Approach**: We use **Bayesian Variational Inference (Flipout)** to provide a real-time **Uncertainty (Sigma)** signal. If the engine enters an unknown flight regime, the model's confidence drops, triggering a proactive maintenance alert.

### 2. Engineering Fragility (The Shim Layer)
*   **The Challenge**: Research code is often GPU-locked, lacks error handling, and suffers from numerical drift (NaN errors) in production.
*   **Our Approach**: An **Adaptive Shim Architecture** intercepts logic at runtime, enforcing **hardware isolation (CPU-only)** and **Global Z-Score scaling** without modifying the original research source files.

### 3. Regulatory Rigor (EU AI Act & DORA)
*   **The Challenge**: High-risk AI systems require absolute traceability and weight-tamper protection.
*   **Our Approach**: A trilateral security protocol: **SafeTensors** (binary safety) + **Sigstore** (cryptographic signing) + **Provenance** (immutable audit trails).

---

## 🚀 MVP Quick Start (Operational Node)

To launch the full stack (Data Ingestion, AI Inference, and Sentinel Dashboard):

```bash
# Full-stack orchestration (Kafka, AI Node, Dashboard)
./infrastructure-setup/scripts/streaming-pipeline-orchestrator.sh
```

<img width="1470" height="956" alt="Screenshot 2026-04-20 at 18 05 20" src="https://github.com/user-attachments/assets/a260c968-b9eb-4d94-ba18-3f075463e227" />


---

## 🛠️ Custom Model Training

If you want to train a new Bayesian model from scratch using NASA's HDF5 telemetry:

```bash
# 1. Configure environment
export GCP_PROJECT_ID="your-project-id"
export DATASET_ID="N-CMAPSS_DS02-006"

# 2. Launch the HPC Training Orchestrator
./infrastructure-setup/scripts/pipeline-orchestrator.sh
```

> [!NOTE]
> Detailed training protocols, hyperparameter tuning, and hardware isolation (Shim Layer) are documented in the [Model Factory README](./rul-model-factory/README.md).

---

## 🏗️ System Architecture

The factory operates via a 4-node deterministic lifecycle:

1.  **Node 0: Staging** — Atomic synchronization of validated telemetry artifacts.
2.  **Node 1: Streamer** — Multi-unit fleet simulator (powered by Redpanda/Kafka).
3.  **Node 2: Inference** — Bayesian "BigCeption" brain with dual-domain Z-score recovery.
4.  **Node 3: Sentinel** — Real-time surveillance dashboard for RUL manifolds and uncertainty.

---

## 🛡️ Security Hardening & IAM Governance

The factory operates under a **Zero-Trust** architecture, enforcing strict isolation between training artifacts and operational telemetry.

### 1. IAM Role Scoping (Least Privilege)
The system utilizes project-scoped service accounts with granular permissions:

| Service | IAM Role | Operational Purpose |
| :--- | :--- | :--- |
| **GCS Storage** | `roles/storage.objectAdmin` | Scoped strictly to the `ncmapss-data-lake` bucket for artifact retrieval. |
| **Cloud Logging** | `roles/logging.logWriter` | Enforces forensic audit-trail persistence without read-access to logs. |
| **Artifact Registry**| `roles/artifactregistry.reader` | Pulling verified/signed production-grade Docker images. |
| **Compute Engine** | `roles/compute.instanceAdmin.v1`| Self-termination capability to prevent zombie resource costs. |

### 2. Supply Chain Integrity (Notary Protocol)
*   **SafeTensors Isolation**: We eliminate the threat of arbitrary code execution by using `safetensors` instead of Python's `pickle`.
*   **Keyless Signing**: Every Bayesian model is signed via **Sigstore**. The system verifies the `.sig` and `.cert` files before loading weights.
*   **Non-Root Execution**: All compute workers and streaming nodes operate under a non-privileged user (UID 1000) to prevent container-breakout escalation.

---

## 🧠 Model Training Benchmarks

The factory currently hosts two primary Bayesian model variants for comparative benchmarking:

| Metric | **Standard Run (3h)** | **Deep Research Run (18h)** |
| :--- | :--- | :--- |
| **Artifact Path** | [./rul-model-factory/.../20260420T0650Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc) | [./rul-model-factory/.../20260421T1251Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260421T1251Z_cpu_hpc) |
| **Learning Rate** | `1e-4` | `3e-5` (High Precision) |
| **Bayesian Particles**| `1` | `8` (Enhanced Posterior) |
| **Pretrain Epochs** | `10` | `25` |
| **Target Hardware** | CPU HPC | CPU HPC |

<img width="1470" height="956" alt="Screenshot 2026-04-19 at 18 40 20" src="https://github.com/user-attachments/assets/df467775-d798-4e7e-84ae-edfc63e286d9" />
<img width="1470" height="956" alt="Screenshot 2026-04-19 at 19 38 31" src="https://github.com/user-attachments/assets/04946e97-78cd-4154-a16a-509ec3461435" />
<img width="1470" height="956" alt="Screenshot 2026-04-21 at 08 55 05" src="https://github.com/user-attachments/assets/e912ab6d-85c2-4f53-ac9b-d8896ca59e2e" />





> [!TIP]
> The **Deep Research Run** provides significantly more stable uncertainty quantification due to the 8-particle Flipout approximation, making it the default for high-risk diagnostic scenarios.

### Core Technology Stack
- **Persistence**: DuckDB (Sovereign DWH Layer)
- **Streaming**: Redpanda (Kafka-compatible)
- **AI/ML**: PyTorch / SafeTensors (Bayesian CNN)
- **UI**: Streamlit (Sentinel Node)

---

## 📂 Project Navigation

For detailed technical specifications, audit trails, and module-specific documentation:

*   **Streaming Pipeline Core**: [README.md](./streaming_pipeline/streaming_pipeline/README.md) (Deployment & Features)
*   **Model Architecture**: [models.py](./streaming_pipeline/streaming_pipeline/src/streaming_pipeline/models.py) (The Bayesian Brain)
*   **Research & Audits**: [./research/](./research/) (Data integrity and operational envelopes)

---

> [!NOTE]
> **Status**: This repository is currently under review for the **Data Engineer Zoom Camp**. The current state represents a functional MVP with industrial-grade logging and auditability.

© 2026 Stan_Buren. V0.1.0.
