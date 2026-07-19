<div align="center">

# Aircraft turbine Digital Twin: Predictive Maintenance in Real Time

### *Make Bayesian CNN Research Code Face the EU AI Act*

by safely training ML in GCP using Docker on HPC 32 core instance with full Logs observability. 

Put it in a **real time telemetry** pipeline that **predicts RUL** (Remaining Useful Life) of the aircraft.

BUT you are **not allowed** to edit a single line of the original researcher code.

<br/>

<!-- ═══════════════════════ COMPLIANCE BADGES ═══════════════════════ -->

<img src="https://img.shields.io/badge/EU_AI_Act-High_Risk-red?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZmlsbD0iI2ZmZiIgZD0iTTEyIDJMNCA1djYuMDljMCA1LjA1IDMuNDEgOS43NiA4IDEwLjkxIDQuNTktMS4xNSA4LTUuODYgOC0xMC45MVY1bC04LTN6bTAgMi4xOGw2IDIuMjV2NS43NGMwIDQuMjgtMi44MyA4LjI0LTYgOS4zMy0zLjE3LTEuMDktNi01LjA1LTYtOS4zM1Y2LjQzbDYtMi4yNXoiLz48L3N2Zz4=">
<img src="https://img.shields.io/badge/Zero_Trust-Sigstore_✗_SafeTensors-black?style=for-the-badge&logo=sigstore&logoColor=white">
<img src="https://img.shields.io/badge/GCP-HPC_32_Core-blue?style=for-the-badge&logo=googlecloud&logoColor=white">
<img src="https://img.shields.io/badge/PyTorch-Bayesian_CNN-orange?style=for-the-badge&logo=pytorch&logoColor=white">

<br/>
<br/>

<!-- ═══════════════════════ TECH STACK ═══════════════════════ -->

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-Bayesian_V1-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)
![SafeTensors](https://img.shields.io/badge/SafeTensors-zero_RCE-00C7B7?style=for-the-badge)
![Apache Spark](https://img.shields.io/badge/PySpark-4.1.1-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-Lakehouse-1E90FF?style=for-the-badge&logo=apache&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Dimensional_Marts-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytical_DWH-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)

![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka_Compatible-FF5500?style=for-the-badge&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Hermetic_Builds-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-Compute_Engine-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Sigstore](https://img.shields.io/badge/Cosign-Keyless_Signing-000000?style=for-the-badge&logo=sigstore&logoColor=white)

<br/>

<b>
<a href="#-the-challenge">Challenge</a> ·
<a href="#-the-solution-adaptive-shim-architecture">Architecture</a> ·
<a href="#-system-architecture-4-node-lifecycle">Pipeline</a> ·
<a href="#-full-architecture-map">Full Map</a> ·
<a href="#-the-bayesian-engine">Bayesian Engine</a> ·
<a href="#-the-shim-layer-in-detail">Shim Layer</a> ·
<a href="#-repository-structure">Structure</a> ·
<a href="#-quick-start">Quick Start</a> ·
<a href="#-model-training-benchmarks">Benchmarks</a> ·
<a href="#-compliance--audit-trail">Compliance</a> ·
<a href="#-iam--least-privilege">IAM</a> ·
<a href="#-vs-original-research-code">vs Research</a>
</b>

</div>

<br/>

---

> ***Legenda*** Ok, company wants me to use their researcher code and build a **MLOps pipeline** from scratch. And train **ML** on **CPU** in **GCP**. But original code is written for **CUDA** and **local GPU's**. And there will be some **compliance audit** on it, so I need to implement **logs transparency**, add **Sigstore**, replace insecure .ckpt with **SafeTensors**. 

---

<br>

## 🎯 The Challenge

<div align="center">
<table>
<tr>
<td width="50%" align="center">

### 🔬 What the Researcher Gave Me

**Brilliant Bayesian algorithm.**

**But: no provenance. No audit trail. No safety.**

**It won't survive a critical infrastructure compliance audit.**

</td>
<td width="50%" align="center">

### 🏭 What Had to Be Built

| Requirement | Implementation |
|---|---|
| 🔐 Zero RCE vectors | pickle → SafeTensors |
| ✍️ Tamper-proof weights | Sigstore/Cosign keyless signing |
| 📋 Audit trail | provenance.json birth certificate |
| 🖥️ CPU-only deployment | CUDA lobotomy via metaclass shim |
| 📊 Full observability | Serial console tee → Cloud Logging |
| 🛑 Fail-closed | Crash → preserve evidence → GCS |

</td>
</tr>
</table>
</div>

<br>

## 🏗️ Architecture

<img width="1224" height="1736" alt="architecture" src="https://github.com/user-attachments/assets/f8d76748-bfc3-42f6-bb84-3eaa065f5372" />


<br>

## 📉 Dashboard

<img width="1470" height="956" alt="Screenshot 2026-04-20 at 18 05 20" src="https://github.com/user-attachments/assets/a260c968-b9eb-4d94-ba18-3f075463e227" />

<br>

## 🔬 The Bayesian Engine

### Why Bayesian?

A frequentist model says: *"Engine #7 has 42 cycles left."*  
A Bayesian model says: *"Engine #7 has 42 ± 8 cycles left. I'm 68% confident."*

When the engine enters an unknown flight regime (high-G maneuvers, unusual temperature profiles), the Bayesian uncertainty **spikes**. The maintenance crew is alerted: *"We don't know — check it."* This is the difference between a false sense of security and a genuine safety system.

### Model: BigCeption

| Component | Specification |
|---|---|
| Architecture | Bayesian InceptionNet (Conv1D + Dense Variational layers) |
| Inference Method | **Flipout** Variational Inference (8 particles) |
| Prior | Gaussian Mean-Field + **Radial** (multi-modal) |
| Input Window | 30 time-steps × 14 sensors (X_s) + 4 auxiliary (A) |
| Output | RUL (cycles) + σ (epistemic uncertainty) |
| Activation | Softplus (physical positivity: RUL ≥ 0) |
| Loss | ELBO normalized: 1/(dataset × window × features) |
| Scoring | NASA Asymmetric Penalty: 1/5 (underest.) vs 1/13 (overest.) |

### Global Z-Score Standardization

```mermaid
flowchart LR
    H5["10× .h5 files<br/>TB-scale"] --> P1["Phase 1<br/>Sequential scan<br/>Global Σx, Σx², N"]
    P1 --> P2["Phase 2<br/>Atomic μ, σ<br/>across all data"]
    P2 --> P3["Phase 3<br/>Parallel Z-score<br/>(x-μ)/σ → Parquet<br/>ProcessPoolExecutor"]
    P3 --> LMDB["LMDB<br/>low-latency I/O"]

    style H5 fill:#1a1a2e,stroke:#16213e,color:#eee
    style P1 fill:#0f3460,stroke:#1a1a8e,color:#eee
    style P2 fill:#0f3460,stroke:#1a1a8e,color:#eee
    style P3 fill:#533483,stroke:#7b2ff7,color:#eee
    style LMDB fill:#1b4332,stroke:#40916c,color:#eee
```

<br>

## 🛡️ Now we're prepared

| # | Interception Point | File | Method | What Was Adapted |
|---|---|---|---|---|
| 1 | `torch.device("cuda:0")` | `patcher.py` | Metaclass `__new__` + `sys.modules` sweep | The research code is tightly coupled to CUDA.<br>Shim intercepts the creation of device objects at the metaclass level: any call to `torch.device("cuda:X")` returns `cpu`.<br>Additionally, iterates through ALL already loaded modules and replaces their local references with `torch.device`.<br>Without this, a non-GPU HPC node crashes on the first call. |
| 2 | `torch.cuda.is_available()` | `patcher.py` | Direct assignment `= lambda: False` | Even after replacing device, many libraries (Pyro, PyTorch Lightning) check `is_available()` before initialization.<br>We force it to return `False` before any code has a chance to call the CUDA driver. |
| 3 | `pl.Trainer(gpus=...)` | `patcher.py` | Monkeypatch `__init__` | Lightning tries to use the GPU by default.<br>Shim intercepts the constructor: removes `gpus`, sets `accelerator='cpu'`, `devices=1`.<br>Adds `StabilityCallback` - detects NaN/Inf in the loss and aborts training with `sys.exit(1)`, instead of silently generating broken checkpoints. |
| 4 | `pyro.optim.ClippedAdam` → `Adam` | `patcher.py` | `sys.modules` redirection | The research code uses `pyro.optim.ClippedAdam`, a Bayesian wrapper over Adam.<br>It is unstable on CPU with large batches.<br>Shim replaces the module: when `ClippedAdam` is imported, the standard `torch.optim.Adam` is returned. |
| 5 | `DataLoader(num_workers=0)` | `patcher.py` | Monkeypatch `__init__` | The research code creates a DataLoader with `num_workers=0` (single-threaded).<br>Shim intercepts and sets `num_workers = cpu_count // 2` for parallel data loading on a 32-core HPC node. |
| 6 | `vi.VI_BNN.__init__` — Bayesian defaults | `patcher.py` | Monkeypatch `__init__` | Research defaults are weak: `num_particles=1` (one particle is essentially a regular neural network), `q_scale=0.004` (prior is too narrow).<br>Shim forces: `num_particles=8`, `q_scale=0.01`, `prior=0.2`.<br>Adds Leaky ReLU to protect against dying neurons in deep Bayesian layers.<br>LR is capped at `0.0002` for CPU safety. |
| 7 | `NCMAPSSDataModule.__init__` — paths | `patcher.py` | Monkeypatch `__init__` | Research code reads data from hardcoded local paths.<br>Shim redirects to a factory-controlled sandbox (`processed/lmdb`), synchronized with GCS. |
| 8 | `VIBnnWrapper.__init__` — batch size | `patcher.py` | Monkeypatch `__init__` | Research `batch_size=10000` causes an OOM on the CPU.<br>Shim implements "Steel Guardian": a dynamic cap of 2560 samples, regardless of the config value. |
| 9 | `VIBnnWrapper.training_step` — NaN guard | `patcher.py` | Monkeypatch methods | After each training step, Shim checks `mse/train` in callback metrics.<br>If NaN, it crashes and preserves forensic telemetry. |
| 10 | `compute_scalers` → parallel | `patcher.py` | Complete function replacement | Research code calculates the Z-score in a single thread: sequential scan of 10 HDF5 files → ~40 minutes.<br>Shim replaces `ProcessPoolExecutor`: parallel scattering across all cores → ~2 minutes. |
| 11 | `generate_parquet` → parallel | `patcher.py` | Complete function replacement | Same story: Parquet shard generation is done via `ProcessPoolExecutor` instead of a single-threaded loop. |
| 12 | `generate_lmdb` → factory version | `patcher.py` | Complete function replacement | The research LMDB generator does not support unit-based splitting.<br>Shim uses its own implementation with `RobustMinMaxAggregate`, protection against empty shards, and physical isolation of engine units. |
| 13 | `get_proportion_lists(device=...)` | `patcher.py` | Function-level patch | The research implementation of uncertainty quantification calls `y_true.device`, which can be `meta` or `None` on a CPU node.<br>Shim forces `device='cpu'` and creates tensors on the CPU. |
| 14 | `argparse` paths | `patcher.py` | `parse_args` mock injection | The research CLI accepts `--data-path`, `--out-path`, etc.<br>Shim intercepts `parse_args()` and replaces all paths with a factory-controlled sandbox: `raw_data/`, `processed/`, `results/`. |

---

<br>

## 📂 Repository Structure

```
n-cmapss-agentic-factory/
├── 📄 README.md                          ← YOU ARE HERE
├── 📄 pyproject.toml                     ← uv monorepo root (3 workspace members)
├── 📄 manifest.yaml                      ← project-wide metadata
│
├── 🧠 rul-model-factory/                 ← MLOps PIPELINE — BAYESIAN TRAINING
│   ├── pyproject.toml
│   ├── README.md
│   ├── artifacts/runs/                   ← signed & provenanced models
│   │   ├── rul_bayesian_20260420T0650Z_cpu_hpc/  (Standard, 3h)
│   │   └── rul_bayesian_20260421T1251Z_cpu_hpc/  (Deep Research, 18h)
│   └── src/rul_model_factory/
│       ├── cloud_trainer/
│       │   ├── execution_controller.py   ← State machine: train → sterilize → sign → upload
│       │   ├── core/
│       │   │   ├── vendor_patch_engine.py   ← THE SHIM: 5 monkeypatch points
│       │   │   ├── feature_engineering.py   ← HDF5 → Parquet/LMDB, GCS sync
│       │   │   └── parallel_execution.py    ← ProcessPoolExecutor workers
│       │   ├── security/
│       │   │   ├── artifact_sterilizer.py   ← pickle → SafeTensors
│       │   │   ├── cryptographic_signer.py  ← cosign sign-blob
│       │   │   └── provenance_generator.py  ← birth certificate JSON
│       │   └── logistics/
│       │       ├── path_resolver.py         ← SSOT filesystem mapping
│       │       └── artifact_uploader.py     ← GCS sync with signing gate
│       └── vendor/                      ← RESEARCH CODE (IMMUTABLE)
│           └── bayesrul/                ← github.com/arthurviens/bayesrul
│
├── 📡 streaming_pipeline/               ← STREAMING PIPELINE — REAL-TIME INFERENCE
│   ├── README.md
│   └── src/streaming_pipeline/
│       ├── ds02-006-preprocessing.py    ← Node 0: Parquet staging → local workspace
│       ├── producer.py                  ← Node 1: Fleet simulator → Redpanda
│       ├── consumer.py                  ← Node 2: Bayesian inference → DuckDB
│       ├── dashboard.py                 ← Node 3: Streamlit Sentinel dashboard
│       ├── models.py                    ← BigCeption architecture definition
│       └── config.py                    ← Pipeline configuration
│
├── 🏗️ infrastructure-setup/              ← INFRASTRUCTURE AS CODE
│   ├── terraform/
│   │   ├── live/_bootstrap/             ← GCS backend + state locking
│   │   ├── live/hpc-training-env/       ← KMS (90-day rotation) + IAM + project
│   │   └── modules/ephemeral-hpc-worker/ ← c2d-standard-32, pd-ssd, startup scripts
│   ├── docker/hpc-training-worker/      ← Dockerfile (non-root, Python 3.10-slim)
│   ├── docker/redpanda/                 ← docker-compose.yml for local streaming
│   ├── scripts/
│   │   ├── pipeline-orchestrator.sh     ← Full training: Terraform→Data→Docker→HPC→Harvest
│   │   ├── streaming-pipeline-orchestrator.sh ← Full streaming: Staging→Producer→Consumer→Dashboard
│   │   ├── worker-provisioning.sh       ← GCE instance dispatch
│   │   ├── image-build-publish.sh       ← Docker build + cosign sign
│   │   ├── artifact-synchronization.sh  ← GCS → local harvest with normalization
│   │   ├── data-lake-ingestion.sh       ← Raw HDF5 → GCS bucket
│   │   └── compliance-sync.sh           ← EU legal texts → workspace
│   └── src/infrastructure_setup/
│       ├── data_logistics/dataset_ingestion.py    ← NASA ZIP download + unzip
│       └── compliance_ops/dora_compliance_scraper.py ← DORA article scraper
│
├── 🗄️ dwh/dbt/                           ← DATA WAREHOUSE
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/stg_telemetry.sql     ← Raw → typed
│   │   ├── intermediate/int_telemetry_normalized.sql
│   │   ├── marts/fct_engine_health_per_cycle.sql  ← Fact table
│   │   └── marts/reports/               ← rpt_safety_alerts, rpt_engine_pnl
│   ├── macros/                          ← flight_distance, fuel_density, iso_corrections
│   └── seeds/fuel_prices.csv            ← Reference data
│
├── 📊 dashboard/                        ← Streamlit configuration
├── 📓 notebooks/                        ← Jupyter EDA & research notebooks
├── 📋 specs/                            ← API contracts, dataset dictionary, path specs
├── 🗺️ blueprints/                       ← Architecture Decision Records
└── 🔧 config/                           ← YAML configs: base, dev, prod, topics
```

<br>

## 🚀 Quick Start

### Local Streaming Pipeline

```bash
# One command: staging → streaming → inference → dashboard
./infrastructure-setup/scripts/streaming-pipeline-orchestrator.sh
```

| Node | Script | Role |
|---|---|---|
| 0 | `ds02-006-preprocessing.py` | Parquet staging from HDF5 |
| 1 | `producer.py` | Redpanda fleet simulator (multi-engine, time-warped) |
| 2 | `consumer.py` | Bayesian inference (30-cycle window, DuckDB sink) |
| 3 | `dashboard.py` | Streamlit Sentinel (RUL + uncertainty visualization) |

### Cloud Training (GCP HPC)

```bash
export GCP_PROJECT_ID="your-project"
export DATASET_ID="N-CMAPSS_DS02-006"

# Full cycle: Terraform → data → Docker → HPC → harvest
./infrastructure-setup/scripts/pipeline-orchestrator.sh

# Skip 18-minute preprocessing (reuse from previous run):
./infrastructure-setup/scripts/pipeline-orchestrator.sh -f bayesian-20260419-20df59
```

### Manual Artifact Recovery

```bash
./infrastructure-setup/scripts/artifact-synchronization.sh \
  "ncmapss-factory-worker-20260420-2206" \
  "runs/bayesian-20260420-0e405c"
```

<br>

## 🏆 Model Training Benchmarks

| Metric | Standard Run (3h) | Deep Research Run (18h) |
|---|---|---|
| **Artifact** | [20260420T0650Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc) | [20260421T1251Z](./rul-model-factory/artifacts/runs/rul_bayesian_20260421T1251Z_cpu_hpc) |
| **Learning Rate** | `1e-4` | `3e-5` |
| **Bayesian Particles** | `1` | `8` |
| **Pretrain Epochs** | `10` | `25` |
| **Hardware** | c2d-standard-32 (AMD Milan) | c2d-standard-32 (AMD Milan) |

<img width="1470" height="956" alt="Screenshot 2026-04-19 at 18 40 20" src="https://github.com/user-attachments/assets/df467775-d798-4e7e-84ae-edfc63e286d9" />
<img width="1470" height="956" alt="Screenshot 2026-04-19 at 19 38 31" src="https://github.com/user-attachments/assets/04946e97-78cd-4154-a16a-509ec3461435" />
<img width="1470" height="956" alt="Screenshot 2026-04-21 at 08 55 05" src="https://github.com/user-attachments/assets/e912ab6d-85c2-4f53-ac9b-d8896ca59e2e" />

<br>

## 📋 Compliance & Audit Trail

### Per-Run Artifacts

```
artifacts/runs/rul_bayesian_YYYYMMDDTHHMMZ_cpu_hpc/
├── 📊 data/
│   └── *.data.parquet              ← predictions + metrics
├── 📝 logs/
│   └── *.training.events           ← TensorBoard events
├── 📋 metadata/
│   ├── provenance.json             ← BIRTH CERTIFICATE
│   └── *.hparams.yaml              ← hyperparameters
├── 🧠 model/
│   └── *.model.safetensors         ← SAFE weights (zero-RCE)
└── 🔐 security/
    ├── *.model.sig                  ← Cosign signature
    ├── *.model.cert                 ← Cosign certificate
    ├── provenance.json.sig         ← manifest signature
    └── provenance.json.cert        ← manifest certificate
```

### provenance.json

```json
{
  "factory_version": "V12.1.0",
  "audit_timestamp": "2026-04-14T11:22:00Z",
  "identity": {
    "run_name": "rul_bayesian_20260414T1122Z_cpu_hpc",
    "git_commit": "a1b2c3d4e5f6...",
    "instance_name": "ncmapss-factory-worker-20260414-1122"
  },
  "software_context": {
    "python": "3.10",
    "torch": "2.x",
    "pytorch_lightning": "2.x",
    "safetensors": "0.x"
  },
  "hardware_context": {
    "cpu_count": 32,
    "architecture": "AMD Milan"
  },
  "data_lineage": {
    "N-CMAPSS_DS02-006.h5": "sha256:abc123..."
  }
}
```

<br>

## 🔬 Aeronautical Feature Space

Per NASA N-CMAPSS specification:

| Category | Count | Sensor Names |
|---|---|---|
| **X_s** (Measurements) | 14 | T24, T30, T48, T50, P15, P2, P21, P24, Ps30, P40, Wf, Nf, Nc, BPR |
| **X_v** (Virtual) | 14 | Efficiencies, Flow Modifiers (derived from X_s) |
| **A** (Auxiliary) | 4 | Flight Condition (Fc), Health State (hs), altitude, Mach |
| **W** (Scenario) | 4 | alt, Mach, TRA, T2 — environmental context |

**Our model input:** `[X_s, A]` — 14 physical sensors + 4 auxiliary flight condition features.

**Validation strategy:** Unit-based splitting — entire engine lifecycles are held out, not random samples. This prevents data leakage between engines in the same fleet.

**Datasets used:**
- Training: `DS02-006` (primary), expandable to all 10 subsets
- Each `.h5` file contains multiple engine units with full run-to-failure trajectories
- Flight classes: 1 (short-haul), 2 (medium-haul), 3 (long-haul) — different degradation patterns

<br>

## 🛡️ TERRAFORM IAM & Least Privilege

| Service | IAM Role | Scope |
|---|---|---|
| **GCS Storage** | `roles/storage.objectAdmin` | Bucket-scoped (not project-wide) |
| **Cloud Logging** | `roles/logging.logWriter` | Write-only (no read access) |
| **Artifact Registry** | `roles/artifactregistry.reader` | Pull signed images only |
| **Compute Engine** | `roles/compute.instanceAdmin.v1` | Self-termination only |

All containers run as **non-root UID 1000**. The application logic never gains root. Docker builds use strict `.dockerignore` denying all by default.

<br>

## 📊 vs. Original Research Code

| Feature | `bayesrul` (Research) | This Factory (Industrial) |
|---|---|---|
| **Hardware** | Hardcoded `cuda:0` | Software-Defined (CPU/GPU via shim) |
| **Environment** | Local Conda/Pip | Hermetic Docker (immutable) |
| **Serialization** | `pickle` (.ckpt) | `SafeTensors` (.safetensors) |
| **Ingestion** | Sequential (single thread) | Parallel (ProcessPoolExecutor, 32-core) |
| **Scaling** | Mini-batch normalization | Global Z-Score (cross-fleet) |
| **Config** | Ephemeral CLI args | Audited Master Config (SSOT) |
| **Provenance** | None | Cryptographically signed manifest |
| **Uncertainty** | 1 particle MFVI | 8-particle Flipout + Radial |
| **Batch Safety** | Fixed 10,000 (OOM risk) | Dynamic cap @ 2,560 |
| **Validation** | Random split | Unit-based (engine-level isolation) |
| **Audit** | Print statements | Structured logs + serial console tee |

<br>

## 📝 License & Attribution

- **Factory code:** Apache 2.0 © 2026 Stan_Buren
- **bayesrul research code:** [github.com/arthurviens/bayesrul](https://github.com/arthurviens/bayesrul) — MIT License
- **NASA N-CMAPSS dataset:** Public domain (NASA Open Data) — [PHM Datasets](https://phm-datasets.s3.amazonaws.com/NASA/17.+Turbofan+Engine+Degradation+Simulation+Data+Set+.zip)

<br>

<p align="center">
  <sub>Every pickle purged. Every artifact signed. Every provenance manifest generated.</sub>
</p>

<p align="center">
  <sub>V12.1.0 · Stan_Buren · 2026</sub>
</p>
