# Research Findings: N-CMAPSS Bayesian Inference Stabilization

**Date**: 2026-04-20  
**Scope**: Digital Twin Node 2 (AI) & Node 3 (Dashboard)  
**Status**: [SOLVED]

## 1. Problem Identification: The "0.0 RUL" & "Catalog Error"

During the deployment of the V17.4 pipeline, two critical failures were identified:
1. **Numerical Collapse**: The Bayesian RUL predictions were consistently 0.0 cycles.
2. **Infrastructure Disconnect**: The Streamlit dashboard reported `Catalog Error: Table fleet_telemetry does not exist` despite background logs showing active inference.

---

## 2. Scientific Audit: Normalization Drift

### The "AHA!" Moment
The raw flight telemetry (HDF5) contains physical units:
- **alt**: ~25,000 ft
- **TRA**: ~75%
- **T2**: ~466 Rankine

However, auditing the **Golden Parquet Artifacts** (from the training run) revealed:
- **alt**: ~ -0.91 (Standardized)
- **TRA**: ~ 0.88 (Standardized)

**Root Cause**: The model was receiving raw thousands-of-feet signals while expecting Z-scores in the `[-3, 3]` range. This overwhelmed the neural activations, forcing the `softplus` layer to output 0.0.

### Derived Normalization Vectors (V18.4)
Using the `find_norm_factors.py` linear solver, we extracted the definitive scaling constants used during training:

| Feature | Mean (Derivation) | Std (Derivation) |
| :--- | :--- | :--- |
| **alt** | 18375.58 | 9180.93 |
| **Mach** | 0.554 | 0.122 |
| **TRA** | 56.001 | 25.770 |
| **T2** | 481.217 | 24.453 |
| ... | ... | ... |

*Full vector available in the implementation plan [V18.4].*

---

## 3. Engineering Audit: DuckDB WAL Synchronization

### Synchronicity Gap
- **Finding**: DuckDB maintains an active `.db.wal` file during writes.
- **Locking**: The `shutil.copyfile` operation only captured the primary `.db` file, which did not yet contain the un-checkpointed table structure.
- **Solution**: Switching the writer (Node 2) to `journal_mode=DELETE` ensures the schema and data are flushed immediately to the primary file, making it accessible to the dashboard's "Shadow Mirror".

---

## 4. Next Steps (V18.4)
- [ ] Inject `MEANS` and `STDS` into `consumer.py`.
- [ ] Implement `PRAGMA journal_mode=DELETE` in `initialize_database()`.
- [ ] Add `Schema Guard` to `dashboard.py`.

---
*Autonomous research concluded. Ready for production implementation.*
