# 🛡️ N-CMAPSS Telemetry Factory: Predictive Maintenance Digital Twin

**Industrial-grade real-time streaming pipeline for aircraft engine Remaining Useful Life (RUL) estimation using Bayesian Inference.**

This project is a **uv monorepo** containing three isolated packages with 1-click orchestrators for cloud training and live inference. It implements a **Bayesian VI (Flipout)** model for RUL with uncertainty quantification, built with **Zero-Trust**, **SafeTensors**, and **Sigstore** to align with **EU AI Act** and **DORA** guidelines for high-risk AI systems.

---

## 🚀 MVP Quick Start (Operational Node)

To launch the full stack (Data Ingestion, AI Inference, and Sentinel Dashboard):

```bash
# Full-stack orchestration (Kafka, AI Node, Dashboard)
./infrastructure-setup/scripts/streaming-pipeline-orchestrator.sh
```

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

## 🧠 Model Training Benchmarks

The factory currently hosts two primary Bayesian model variants for comparative benchmarking:

| Metric | **Standard Run (3h)** | **Deep Research Run (18h)** |
| :--- | :--- | :--- |
| **Artifact Path** | [./rul-model-factory/.../20260420T0650Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc) | [./rul-model-factory/.../20260421T1251Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260421T1251Z_cpu_hpc) |
| **Learning Rate** | `1e-4` | `3e-5` (High Precision) |
| **Bayesian Particles**| `1` | `8` (Enhanced Posterior) |
| **Pretrain Epochs** | `10` | `25` |
| **Target Hardware** | CPU HPC | CPU HPC |

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
