# Copyright 2026 Stanislav Burundukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DOMAIN: MODEL-TRAINING-PROCESS
COMPONENT: CLOUD_TRAINER
SUBCOMPONENT: PARALLEL WORKERS

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - EU AI Act Machine-Readability (Parquet Format)
    - CRA Annex I (Security by Design)
    - ISO 8601 Data Continuity (Linear RUL Transformation)

PARALLEL LAYERS:
    - Preprocessing: Parallel feature extraction and Z-score stat accumulation.
    - Normalization: Global standard scaling and rolling window mean application.
    - Aggregation: Robust MinMax tracking for bit-perfect LMDB compatibility.

PERFORMANCE:
    - GIL Bypass: Leverages ProcessPoolExecutor for true multi-core utilization.
    - Memory Efficiency: Chunks large datasets to prevent system swapping.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Union

# 2. Third-Party Dependencies
import h5py
import pandas as pd
import numpy as np

# ==============================================================================
# VENDOR LOGIC
# ==============================================================================
# Column mappings dictated by the N-CMAPSS dataset and BayesRul vendor.

VARS_MAP = {
    'W': ['alt', 'Mach', 'TRA', 'T2'],
    'X_s': ['T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21', 'P24', 
            'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf'],
    'X_v': ['T40', 'P30', 'P45', 'W21', 'W22', 'W25', 'W31', 'W32', 'W48', 
            'W50', 'SmFan', 'SmLPC', 'SmHPC', 'phi'],
    'T': ['fan_eff_mod', 'fan_flow_mod', 'LPC_eff_mod', 'LPC_flow_mod',
          'HPC_eff_mod', 'HPC_flow_mod', 'HPT_eff_mod', 'HPT_flow_mod',
          'LPT_eff_mod', 'LPT_flow_mod'],
    'A': ['Fc', 'unit', 'cycle', 'hs'],
    'Y': ['rul']
}

# ==============================================================================
# WORKER INITIALIZATION
# ==============================================================================

def worker_init(vendor_root: str, root_dir: str):
    """Warm up the environment for the worker process.
    
    Injects vendor root into sys.path to allow imports in the subprocess
    and sets the FACTORY_ROOT environment anchor.
    """
    if vendor_root not in sys.path:
        sys.path.insert(0, vendor_root)
    os.environ["FACTORY_ROOT"] = root_dir

# ==============================================================================
# PREPROCESSING WORKERS
# ==============================================================================

def preprocessing_worker(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts data from HDF5 and calculates local Z-score statistics.
    
    This worker handles the initial transition from raw HDF5 to intermediate
    Parquet format while capturing statistics for global normalization.
    """
    filepath = task_data['filepath']
    selected_vars = task_data['vars']
    bits = task_data.get('bits', 32)
    output_dir = task_data['output_dir']
    
    dtype = np.float32 if bits == 32 else np.float64
    
    try:
        with h5py.File(filepath, 'r') as hdf:
            dev_data = []
            test_data = []
            all_varnames = []
            
            # W is always included in bayesrul logic
            dev_data.append(np.array(hdf.get('W_dev')))
            test_data.append(np.array(hdf.get('W_test')))
            all_varnames.extend([str(x) for x in np.array(hdf.get('W_var'), dtype='U20')])
            
            for v in selected_vars:
                if v == 'W':
                    continue  # Already added
                dev_data.append(np.array(hdf.get(f'{v}_dev')))
                test_data.append(np.array(hdf.get(f'{v}_test')))
                all_varnames.extend([str(x) for x in np.array(hdf.get(f'{v}_var'), dtype='U20')])
                
            # RUL is always included
            dev_data.append(np.array(hdf.get('Y_dev')))
            test_data.append(np.array(hdf.get('Y_test')))
            all_varnames.append('rul')
            
            dev_arr = np.concatenate(dev_data, axis=1)
            test_arr = np.concatenate(test_data, axis=1)
            
            df_dev = pd.DataFrame(dev_arr, columns=all_varnames, dtype=dtype)
            df_test = pd.DataFrame(test_arr, columns=all_varnames, dtype=dtype)
            
        # Calculate local stats for Z-score
        nosearchfor = ["unit", "cycle", "Fc", "hs", "rul"]
        scale_cols = [c for c in df_dev.columns if not any(n in c for n in nosearchfor)]
        
        local_sum = df_dev[scale_cols].sum(axis=0)
        local_count = len(df_dev)
        local_var_sum = df_dev[scale_cols].var(axis=0) * (local_count - 1) 
        
        temp_path = Path(output_dir) / f"raw_{Path(filepath).stem}.parquet"
        df_dev['split'] = 'dev'
        df_test['split'] = 'test'
        df_all = pd.concat([df_dev, df_test], axis=0)
        
        df_all.to_parquet(temp_path, engine='pyarrow')
        
        return {
            'status': 'success',
            'local_sum': local_sum.to_dict(),
            'local_var_sum': local_var_sum.to_dict(),
            'local_count': local_count,
            'temp_path': str(temp_path),
            'scale_cols': scale_cols
        }
        
    except Exception as e:
        logging.error(f"Error in preprocessing_worker for {filepath}: {str(e)}")
        return {'status': 'error', 'message': str(e)}

# ==============================================================================
# NORMALIZATION WORKERS
# ==============================================================================

def parquet_worker(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """Applies global normalization, rolling mean, and linear RUL.
    
    Transforms intermediate Parquet files into normalized datasets ready 
    for LMDB conversion. Enforces linear piece-wise RUL and unit-based
    validation splitting.
    """
    temp_path = task_data['temp_path']
    global_mean = pd.Series(task_data['global_mean'])
    global_std = pd.Series(task_data['global_std'])
    moving_avg = task_data.get('moving_avg', True)
    val_ratio = task_data.get('validation_ratio', 0.1)
    output_dir = task_data['output_dir']
    
    try:
        df = pd.read_parquet(temp_path)
        scale_cols = global_mean.index.tolist()
        
        # 1. Apply Normalization
        df[scale_cols] = (df[scale_cols] - global_mean) / global_std
        
        # 2. Moving Average (Rolling Mean)
        if moving_avg:
            exclude_cols = ['Fc', 'rul', 'unit', 'cycle', 'split', 'hs']
            cols_to_roll = [c for c in df.columns if c not in exclude_cols]
            df[cols_to_roll] = df.groupby(['unit', 'cycle'])[cols_to_roll].transform(
                lambda x: x.rolling(window=10, min_periods=1).mean()
            )
            
        # 3. Linear Piece-wise RUL
        if 'hs' in df.columns:
            def apply_linear_rul(group):
                healthy_mask = group['hs'] == 1
                if healthy_mask.any():
                    max_healthy_rul = group.loc[healthy_mask, 'rul'].min()
                    group.loc[healthy_mask, 'rul'] = max_healthy_rul
                return group
            df = df.groupby('unit', group_keys=False).apply(apply_linear_rul)
            
        # 4. Split into Train, Val, Test
        df_test = df[df['split'] == 'test'].drop(columns=['split'])
        df_train_full = df[df['split'] == 'dev'].drop(columns=['split'])
        
        units = df_train_full['unit'].unique()
        np.random.seed(42)
        np.random.shuffle(units)
        
        val_size = int(len(units) * val_ratio)
        val_units = units[:val_size]
        
        df_val = df_train_full[df_train_full['unit'].isin(val_units)]
        df_train = df_train_full[~df_train_full['unit'].isin(val_units)]
        
        # 5. Save Final Parquets
        base_name = Path(temp_path).stem.replace("raw_", "")
        parquet_dir = Path(output_dir) / "parquet"
        parquet_dir.mkdir(exist_ok=True, parents=True)
        
        paths = {}
        for name, data in [("train", df_train), ("val", df_val), ("test", df_test)]:
            p = parquet_dir / f"{name}_{base_name}.parquet"
            data.to_parquet(p, engine='pyarrow')
            paths[name] = str(p)
            
        return {'status': 'success', 'paths': paths}
        
    except Exception as e:
        logging.error(f"Error in parquet_worker for {temp_path}: {str(e)}")
        return {'status': 'error', 'message': str(e)}

# ==============================================================================
# AGGREGATORS
# ==============================================================================

class RobustMinMaxAggregate:
    """Aggregate for computing global statistics across LMDB creation.
    
    Compatible with bayesrul's protocol. Ensures that bit-perfect min/max
    samples are preserved for the vendor's normalization layers.
    """
    def __init__(self, n_features: int, bits: int = 32):
        self.n_features = n_features
        self.dtype = np.float32 if bits == 32 else np.float64
        self.min_ = None
        self.max_ = None

    def feed(self, line_data: np.ndarray, _i: int) -> None:
        """Updates global min/max stats with a flattened window of data."""
        data = line_data.reshape(-1, self.n_features)
        
        local_min = data.min(axis=0)
        local_max = data.max(axis=0)
        
        if self.min_ is None:
            self.min_ = local_min
            self.max_ = local_max
        else:
            self.min_ = np.minimum(self.min_, local_min)
            self.max_ = np.maximum(self.max_, local_max)

    def get(self) -> Dict[str, Union[np.ndarray, bytes]]:
        """Returns the accumulated statistics in vendor-compatible format."""
        return {
            "min_sample": self.min_.astype(self.dtype),
            "max_sample": self.max_.astype(self.dtype)
        }
