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
SUBCOMPONENT: BAYESRUL ADAPTER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - EU AI Act Transparency (Art. 13 - Portability Layer)
    - Zero-Trust Infrastructure (Sandbox Path Enforcement)
    - DORA Resilience (Integrity Verification)

ADAPTER LAYERS:
    - Orchestration: Full data pipeline management (HDF5 -> Parquet -> LMDB).
    - Portability: Engine execution shim utilizing runpy for non-invasive runs.
    - Verification: Environment integrity checks for critical dependencies.

PERFORMANCE:
    - Parallelization: Utilizes multi-core processing for heavy data transformations.
    - Resource Pinning: Coordinated with RuntimePatcher for hardware abstraction.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
import sys
import runpy
import logging
import importlib.util
from pathlib import Path
from typing import Dict, List, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

# 2. Internal Port & Adapter Dependencies
from ...core.ports.vendor_engine_port import VendorEnginePort
from ...core.logistics.path_resolver import PathResolver
from .patcher import surgical_interception
from ..parallel.workers import (
    worker_init, 
    preprocessing_worker, 
    parquet_worker,
    RobustMinMaxAggregate
)

# ==============================================================================
# BAYESRUL ENGINE ADAPTER
# ==============================================================================

class BayesRulEngineAdapter(VendorEnginePort):
    """Adapter for the BayesRul vendor library.
    
    Orchestrates the lifecycle of model training, from data ingestion and
    normalization to execution inside a hardened runtime context.
    """

    def __init__(self, path_resolver: PathResolver, vendor_root: str):
        """Initializes the adapter with necessary infrastructure anchors.
        
        Args:
            path_resolver: The system-wide source of truth for all paths.
            vendor_root: Path to the vendor's research codebase.
        """
        self.resolver = path_resolver
        self.vendor_root = vendor_root
        self._check_vendor_availability()

    def _check_vendor_availability(self):
        """Ensures the vendor codebase is accessible and injected into sys.path."""
        if not Path(self.vendor_root).exists():
            raise RuntimeError(f"Vendor root not found at {self.vendor_root}")
        if self.vendor_root not in sys.path:
            sys.path.insert(0, self.vendor_root)

    # ==========================================================================
    # DATA PREPARATION PIPELINE
    # ==========================================================================

    def prepare_datasets(self, dataset_id: str, config: Dict[str, Any]) -> Dict[str, str]:
        """Orchestrates the HDF5 -> Parquet -> LMDB data transformation pipeline.
        
        This multi-stage process ensures data is normalized and formatted 
        optimally for the vendor's Bayesian inference engine.
        """
        logging.info(f"Preparing datasets for {dataset_id}...")
        
        raw_data_dir = self.resolver.get_sandbox_path("raw_data")
        interim_dir = self.resolver.get_sandbox_path("interim")
        final_dir = self.resolver.get_sandbox_path("processed")
        
        hdf5_files = list(Path(raw_data_dir).glob("*.h5"))
        if not hdf5_files:
            raise FileNotFoundError(f"No HDF5 files found in {raw_data_dir}")

        # Stage 1: Parallel Preprocessing
        stats = []
        with ProcessPoolExecutor(max_workers=os.cpu_count(), 
                                 initializer=worker_init, 
                                 initargs=(self.vendor_root, str(self.resolver.root_dir))) as executor:
            tasks = [
                {
                    'filepath': str(f),
                    'vars': config.get('subdata', ['X_s', 'A']),
                    'bits': config.get('bits', 32),
                    'output_dir': str(interim_dir)
                } for f in hdf5_files
            ]
            futures = [executor.submit(preprocessing_worker, t) for t in tasks]
            
            for future in as_completed(futures):
                res = future.result()
                if res['status'] == 'error':
                    raise RuntimeError(f"Preprocessing failed: {res['message']}")
                stats.append(res)

        # Stage 2: Global Stats Aggregation
        total_count = sum(s['local_count'] for s in stats)
        scale_cols = stats[0]['scale_cols']
        
        global_sum = {col: sum(s['local_sum'][col] for s in stats) for col in scale_cols}
        global_mean = {col: val / total_count for col, val in global_sum.items()}
        
        global_var = {}
        for col in scale_cols:
            var_sum = sum(s['local_var_sum'][col] for s in stats)
            mean_diff_sum = sum(s['local_count'] * (s['local_sum'][col]/s['local_count'] - global_mean[col])**2 for s in stats)
            global_var[col] = (var_sum + mean_diff_sum) / total_count
        
        global_std = {col: max(1e-8, val**0.5) for col, val in global_var.items()}

        # Stage 3: Parallel Parquet Generation
        parquet_results = []
        with ProcessPoolExecutor(max_workers=os.cpu_count(), 
                                 initializer=worker_init, 
                                 initargs=(self.vendor_root, str(self.resolver.root_dir))) as executor:
            tasks = [
                {
                    'temp_path': s['temp_path'],
                    'global_mean': global_mean,
                    'global_std': global_std,
                    'moving_avg': config.get('moving_avg', True),
                    'validation_ratio': config.get('validation', 0.1),
                    'output_dir': str(final_dir)
                } for s in stats
            ]
            futures = [executor.submit(parquet_worker, t) for t in tasks]
            for future in as_completed(futures):
                parquet_results.append(future.result())

        # Stage 4: LMDB Conversion (Compatibility Layer)
        lmdb_dir = final_dir / "lmdb"
        lmdb_dir.mkdir(exist_ok=True)
        
        for split in ["train", "val", "test"]:
            split_parquets = []
            for res in parquet_results:
                if split in res['paths']:
                    split_parquets.append(Path(res['paths'][split]))
            
            if split_parquets:
                self._create_vendor_lmdb(lmdb_dir / f"{split}.lmdb", split_parquets, config)

        return {
            "lmdb_path": str(lmdb_dir),
            "parquet_path": str(final_dir / "parquet")
        }

    def _create_vendor_lmdb(self, output_path: Path, files: List[Path], config: Dict[str, Any]):
        """Implementation of vendor-compatible LMDB serialization logic."""
        import lmdb
        import numpy as np
        import pandas as pd
        
        bits = config.get('bits', 32)
        dtype = np.float32 if bits == 32 else np.float64
        win_length = config.get('win_length', 30)
        win_step = config.get('win_step', 10)
        features = config.get('features_list', []) 
        
        env = lmdb.open(str(output_path), map_size=1024**4)
        agg = RobustMinMaxAggregate(n_features=len(features), bits=bits)
        
        total_lines = 0
        with env.begin(write=True) as txn:
            for f in files:
                df = pd.read_parquet(f)
                for unit_id, traj in df.groupby("unit"):
                    for i in range(0, traj.shape[0] - win_length + 1, win_step):
                        sl = slice(i, i + win_length)
                        window = traj.iloc[sl]
                        
                        data = window[features].values.flatten(order='F')
                        
                        txn.put(str(total_lines).encode(), data.astype(dtype).tobytes())
                        txn.put(f"unit_id_{total_lines}".encode(), str(unit_id).encode())
                        txn.put(f"rul_{total_lines}".encode(), str(window['rul'].iloc[-1]).encode())
                        
                        agg.feed(data, total_lines)
                        total_lines += 1
            
            txn.put(b"nb_lines", str(total_lines).encode())
            txn.put(b"n_features", str(len(features)).encode())
            txn.put(b"bits", str(bits).encode())
            
            stats = agg.get()
            txn.put(b"min_sample", stats["min_sample"].tobytes())
            txn.put(b"max_sample", stats["max_sample"].tobytes())

    # ==========================================================================
    # ENGINE EXECUTION
    # ==========================================================================

    def train_model(self, method: str, hyperparams: Dict[str, Any], dataset_id: str) -> Dict[str, Any]:
        """Executes bayesrul training inside a surgical patch context.
        
        Leverages RuntimePatcher to ensure the vendor code behaves correctly
        within the factory infrastructure.
        """
        paths = {
            'data_path': str(self.resolver.get_sandbox_path("processed") / "lmdb"),
            'out_path': str(self.resolver.get_sandbox_path("results"))
        }
        
        Path(paths['out_path']).mkdir(exist_ok=True, parents=True)
        hyperparams['model'] = method
        
        logging.info(f"Starting training for {method} on {dataset_id}...")
        
        with surgical_interception(paths, hyperparams, force_cpu=hyperparams.get('force_cpu', False)):
            module_name = 'bayesrul.ncmapss.train_model'
            
            try:
                runpy.run_module(module_name, run_name='__main__')
                results_path = Path(paths['out_path'])
                return {
                    "status": "completed",
                    "artifacts_dir": str(results_path),
                    "method": method
                }
            except SystemExit as e:
                if e.code == 0:
                    return {"status": "completed", "artifacts_dir": str(Path(paths['out_path']))}
                else:
                    raise RuntimeError(f"Vendor execution exited with code {e.code}")

    # ==========================================================================
    # INTEGRITY
    # ==========================================================================

    def check_environment_integrity(self) -> bool:
        """Verifies that all required vendor and internal dependencies are available."""
        deps = ['torch', 'pytorch_lightning', 'pyro', 'tyxe', 'lmdb']
        try:
            for dep in deps:
                if importlib.util.find_spec(dep) is None:
                    logging.error(f"Integrity check failed: Missing dependency {dep}")
                    return False
            return True
        except Exception as e:
            logging.error(f"Integrity check failed: {str(e)}")
            return False
