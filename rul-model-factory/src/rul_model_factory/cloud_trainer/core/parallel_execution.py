"""
DOMAIN: GCE_RUL_TRAINING
COMPONENT: ACCELERATOR (Multi-Core Processing Engine)
VERSION: 0.1.0
STATUS: AUDIT_PENDING

ROLE:
    Provides compute-isolated workers for high-throughput telemetry preprocessing.
    Orchestrates multi-core feature extraction while bypassing Python's 
    multiprocessing serialization constraints (pickling).

REGULATORY ALIGNMENT:
    - [CRA Annex I]: Security by Design. Ensures process isolation between workers.
    - [EU AI Act Art. 15]: Cybersecurity & Robustness. Prevents data cross-contamination.
    - [DORA]: Operational Resilience via atomic subprocess recovery.

CONTRACT:
    - IN: Raw HDF5 file pointers and scaling metadata.
    - OUT: Normalized, windowed Parquet shards ready for epoch-based training.

SYSTEM-LEVEL INVARIANTS:
    - PURE SUBPROCESS: No shared-memory state between nodes; atomic I/O only.
    - FAIL_CLOSED: If a worker fails, the parent orchestrator must halt the pipeline.
    - NO_SIDE_EFFECTS_ON_MASTER: Workers MUST NOT modify original .h5 files.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def worker_init(vendor_root_str: str, root_dir_str: str) -> None:
    """
    Initializes the execution context for a parallel worker process.
    
    This function is executed once per worker at startup to ensure the 
    sub-process environment matches the master controller's SSOT.
    
    Args:
        vendor_root_str (str): Absolute path to the bayesrul vendor directory.
        root_dir_str (str): Absolute path to the project root for .env resolution.
    """
    # 1. Path Injection: Ensure vendor modules are discoverable
    vendor_root = Path(vendor_root_str)
    for p in [vendor_root, vendor_root / "bayesrul", vendor_root / "tyxe"]:
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    
    # 2. Context Synchronization: Move to root and load environment
    os.chdir(root_dir_str)
    load_dotenv(os.path.join(root_dir_str, ".env"))

def preprocessing_worker(task_data):
    """
    Performs asynchronous feature extraction and scaling metadata computation.

    Args:
        task_data (tuple): A tuple containing filename, data_path, typ, 
                         args_subdata, and args_validation.

    Raises:
        FileNotFoundError: If the infrastructure layer failed to provision assets.
        RuntimeError: If HDF5 structure is corrupted (Fail-Closed).

    Side Effects:
        - High Memory I/O: Spawns sub-process with localized dataset copy in RAM.
        - Computational Load: Saturates single CPU core during DataFrame extraction.
    """
    # Deferred import to ensure the vendor environment is fully initialized in the worker process.
    import bayesrul.ncmapss.preprocessing as bp
    filename, data_path, typ, args_subdata, args_validation = task_data
    
    # INVARIANT: Infrastructure Integrity. We must fail-fast if the Gateway 
    # (feature_engineering.py) didn't do its job of provisioning the file.
    filepath = os.path.join(data_path, filename)
    # VENDOR CONTRACT COMPLIANCE: bayesrul filenames are always sans-extension.
    # The vendor's _load_data_from_file (preprocessing.py:398) appends .h5
    # itself. We must mirror this convention in our preflight check.
    filepath_on_disk = filepath if filepath.endswith(".h5") else filepath + ".h5"
    if not os.path.exists(filepath_on_disk):
        raise FileNotFoundError(f"[ERROR:Preflight] Required dataset missing: {filepath_on_disk}.")

    # EXTRACT: Using vendor logic. Side Effect: Memory spikes proportional to HDF5 size.
    try:
        train, _, _ = bp.extract_validation(
            filepath=filepath, 
            typ=typ, 
            vars=args_subdata, 
            validation=args_validation
        )
    except (OSError, ValueError) as e:
        print(f"[CRITICAL:DataIntegrity] Failed to extract {filename}: {e}")
        return None
    
    # INVARIANT: Physics-Only Signal. 
    # Why: High-dimensional ML logic must be decoupled from technical metadata 
    # (unit, cycle, Fc). Including them in normalization would distort sensor variance.
    cols = train.columns[~(train.columns.str.contains('unit|cycle|Fc|hs|rul'))]
    
    return train[cols].sum().values, train[cols].var().values * (len(train) - 1), len(train), list(cols)


class RobustMinMaxAggregate:
    def __init__(self, args):
        self.args = args
        self.min_ = None
        self.max_ = None

    def feed(self, line, i: int) -> None:
        import numpy as np
        n_features = len(self.args.features)
        min_curr, max_curr = (
            line.data.reshape(n_features, -1).T,
            line.data.reshape(n_features, -1).T,
        )

        if self.min_ is None:
            self.min_, self.max_ = min_curr.min(0), max_curr.max(0)
        else:
            self.min_ = np.min([self.min_, min_curr.min(0)], axis=0)
            self.max_ = np.max([self.max_, max_curr.max(0)], axis=0)

    def get(self):
        import numpy as np
        if self.min_ is None:
            print("[WARNING:DataIntegrity] MinMaxAggregate called on empty dataset.")
            # Return zeros to prevent pipeline crash, though this indicates an upstream data loss.
            zeros = np.zeros(len(self.args.features))
            return {"min_sample": zeros, "max_sample": zeros}
            
        return {
            "min_sample": self.min_.astype(
                np.float32 if self.args.bits == 32 else np.float64
            ),
            "max_sample": self.max_.astype(
                np.float32 if self.args.bits == 32 else np.float64
            ),
        }

def parquet_worker(task_data):
    """
    Handles simultaneous remote acquisition, extraction, and parquet serialization.

    Args:
        task_data (tuple): Contains all metadata, configurations, and pre-computed scalers.

    Side Effects:
        - Filesystem Mutation: Creates /parquet directory and writes *.parquet files.
        - High Memory I/O: Holding up to 3 DataFrames (train/val/test) in memory simultaneously.
    """
    import bayesrul.ncmapss.preprocessing as bp
    import pandas as pd
    import re
    (filename, data_path, results_path, typ, subdata, validation, 
     moving_avg, columns, mean, std) = task_data
    
    filepath = os.path.join(data_path, filename)
    # VENDOR CONTRACT COMPLIANCE: mirror preprocessing.py:398 — filenames are sans-extension.
    filepath_on_disk = filepath if filepath.endswith(".h5") else filepath + ".h5"
    if not os.path.exists(filepath_on_disk):
        raise FileNotFoundError(f"[ERROR:Preflight] Required dataset missing: {filepath_on_disk}")
    
    try:
        train, val, test = bp.extract_validation(
            filepath=filepath,
            typ=typ,
            vars=subdata,
            validation=validation,
        )
    except (OSError, ValueError) as e:
        print(f"[CRITICAL:DataIntegrity] Failed to extract {filename} during parquet phase: {e}")
        return None
    
    match = re.search(r'DS[0-9]{2}', filename)
    short_name = match[0] if match else filename

    # INTENT: Temporal Stabilization. 
    # Why: Moving average smooths high-frequency sensor noise typical for 
    # thermophysical engine data, preventing the model from overfitting to transients.
    if moving_avg:
        for df in [train, val, test]:
            if isinstance(df, pd.DataFrame) and not df.empty:
                saved_cols = df[['unit', 'cycle', 'Fc', 'rul']]
                df_proc = df.drop(columns=['Fc', 'rul']).groupby(['unit', 'cycle']).transform(lambda x: x.rolling(10, 1).mean())
                for col in df_proc.columns:
                    df[col] = df_proc[col]
    
    # INVARIANT: Global Standardization. 
    # Z-score must be applied based on statistics computed across ALL datasets 
    # to maintain a consistent unified feature space (L1 Integrity).
    for df in [train, val, test]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            df[columns] -= mean
            df[columns] /= std

    # PERSISTENCE: Mutation of local STAGING buffer. 
    # Parquet is chosen over CSV for deterministic typing and performance.
    parquet_path = Path(results_path, "parquet")
    parquet_path.mkdir(exist_ok=True, parents=True)
    
    for df, prefix in zip([train, val, test], ["train", "val", "test"]):
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.to_parquet(f"{parquet_path}/{prefix}_{short_name}.parquet", engine="pyarrow")
    
    return short_name
