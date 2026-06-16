# 🛡️ Industrial Digital Twin Center: N-CMAPSS Streaming (V1.5.6)

![Sim Status: Operational](https://img.shields.io/badge/Node_1-Streaming-success)
![AI Status: Bayesian_Recovered](https://img.shields.io/badge/Node_2-Inference-blue)
![Dashboard Status: sentinel](https://img.shields.io/badge/Node_3-Sentinel-orange)

**Industrial-grade real-time Digital Twin for Bayesian RUL estimation. Transforms static HDF5 datasets into mission-critical predictive streams with millisecond latency.**

---

## 📊 Operational Status & Governance

| Vector | Control Resource | Documentation & Justification |
| :--- | :--- | :--- |
| **Orchestration** | [orchestrator.sh](../../infrastructure-setup/scripts/streaming-pipeline-orchestrator.sh) | Deterministic "Fresh Start" lifecycle manager. |
| **Node 0: Staging** | [ds02-006-preproc...](./src/streaming_pipeline/ds02-006-preprocessing.py) | Golden Data Synchronization Barrier. |
| **Node 1: Streamer** | [producer.py](./src/streaming_pipeline/producer.py) | Fleet Telemetry Simulator (CPU Affinity / Warp). |
| **Node 2: Inference** | [consumer.py](./src/streaming_pipeline/consumer.py) | Dual-Domain Bayesian Brain (Z-Score Recovery). |
| **Node 3: Sentinel** | [dashboard.py](./src/streaming_pipeline/dashboard.py) | Probabilistic Surveillance (Cycle-Gate / Smoothing). |
| **Persistence** | [.workspace/persistence/](../../.workspace/persistence/) | Sovereign DuckDB analytical sink (WAL Mirror). |

---

## ⚡ Orchestration: Mission Initialization

Starting the full Digital Twin cycle is centralized via the **Streaming Orchestrator**. This ensures deterministic process lifecycle management.

```bash
# RECOMMENDED: Automated full-stack dispatch
./infrastructure-setup/scripts/streaming-pipeline-orchestrator.sh

# MANUAL: Individual Node execution from project root
# Node 0: Data Staging Barrier 
uv run --project streaming_pipeline/streaming_pipeline python streaming_pipeline/streaming_pipeline/src/streaming_pipeline/ds02-006-preprocessing.py

# Node 2: Bayesian Inference Engine
uv run --project streaming_pipeline/streaming_pipeline python streaming_pipeline/streaming_pipeline/src/streaming_pipeline/consumer.py

# Node 1: Multi-Unit Telemetry Simulator
uv run --project streaming_pipeline/streaming_pipeline python streaming_pipeline/streaming_pipeline/src/streaming_pipeline/producer.py

# Node 3: Sentinel Dashboard
uv run --project streaming_pipeline/streaming_pipeline streamlit run streaming_pipeline/streaming_pipeline/src/streaming_pipeline/dashboard.py
```

> [!IMPORTANT]
> The system implements a **Fail-Closed** policy. If Kafka (Redpanda) or the local DuckDB persistence layer is unavailable, Node 2 will immediately power down to prevent unverified RUL dissemination.

---

## ⚙️ Industrial Core Features

The pipeline incorporates several high-fidelity engineering solutions designed for mission-critical aerospace monitoring.

### 🧬 Normalization Transformer (V18.4 Recovery)
To resolve the "0.0 RUL" prediction drift, the engine implements a **Z-Score Calibration Layer**. It autonomously maps raw physics telemetry to the Bayesian manifold expected by the training phase:
*   **Transformation**: `z = (x - μ) / σ`
*   **Precision**: Prevents model saturation, enabling RUL convergence within ±15% error margins.

### 🛡️ Atomic Shadow Mirror (V1.5.4)
The persistence layer logic uses a **Lock-Free synchronization** pattern. The dashboard interrogates the shared DuckDB state via point-in-time file snapshots:
*   **Mechanism**: `shutil.copyfile` + `journal_mode=DELETE`.
*   **Benefit**: Eliminates 'Catalog Errors' and OS-level metadata locks, maintaining a stable 15 FPS UI heartbeat.

### 🎮 Dynamic Multi-Unit Time-Warp
The simulator (Node 1) applies unit-specific undersampling to visualize full flight cycles in human-readable timeframes.
*   **Calibration**: Steps vary from 10x to 50x to target a **12-16s cycle duration**.
*   **Synchronization**: Interleaved streaming ensures all fleet units are processed in real-time coordination.

### 🛡️ Schema Guard
Mission Node 3 implements a structural validator that verifies the existence of the `fleet_telemetry` table in the shadow mirror before attempting execution, preventing startup crashes.

---

## 🔬 Architectural Synthesis (V1.5.8)

Results of the Technical Audit and Digital Twin mapping:

*   **Node 0 (Staging)**: Acts as the "Customs Office", synchronizing validated Parquet fuel from the Bayesian Factory to the operational workspace with atomic persistence.
*   **Node 1 (Producer)**: High-throughput fleet emulator. Utilizes `ThreadPoolExecutor` and `CPU Affinity` to simulate mission-critical telemetry load across multiple turbine units.
*   **Node 2 (Inference)**: The system's "Brain". Implements **Dual-Domain Processing** (Z-Score recovery) and maintains a 30-cycle sliding window for Bayesian inference.
*   **Node 3 (Sentinel)**: The "Watchtower". Uses `st.fragment` for anti-flicker UI updates and LOWESS scientific smoothing for degradation manifold visualization.

---

## 🚀 Hardening Roadmap

Strategic vector for industrial-grade optimization:

1.  **Dynamic Normalization**: Transition Z-score constants from hardcoded variables to YAML metadata artifacts for 100% training-inference parity.
2.  **Robust Orchestration**: Replace `sleep 3` startup delays with active Redpanda/Kafka health checks (`rpk status`).
3.  **Latency Surveillance**: Add real-time "Inference Time" metrics to the dashboard to monitor operational drift.
4.  **Path Portability**: Eliminate absolute path dependencies in Node 0 to ensure environment sovereignty.

---

## 🔐 Compliance & Forensics (Audit Navigator)

This section provides access to the forensic telemetry and audit-ready data interrogators.

*   **Model Architecture**: [models.py](./src/streaming_pipeline/models.py) (BigCeption Bayesian Brain).
*   **Mission Logs**: [../../.workspace/local-logs/](../../.workspace/local-logs/) (Timestamped Audit Trails).
*   **Forensic Audit Script**: [../../research/building_streaming_pipeline/asking_duck_db/check_state.py](../../research/building_streaming_pipeline/asking_duck_db/check_state.py) (Read-only database interrogation).

---
© 2026 Digital Twin Engineering Team. Operational Protocol V1.5.6.
