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
SUBCOMPONENT: FEATURE_ENGINEERING

VERSION: 0.1.0
STATUS: VALIDATING
AUTHOR: Stanislav Burundukov (@Stan_Buren)
"""

# SIDE EFFECTS:

# ROLE:
#     Implements a deterministic and immutable provisioning layer for the RUL Factory.
#     Manages the transformation of raw NASA N-CMAPSS HDF5 telemetry into 
#     normalized, tensor-ready formats (Parquet/LMDB).

# REGULATORY ALIGNMENT:
#     - CRA Article 10: Ensures data integrity and lifecycle provenance.
#     - EU AI Act Section 2: Data Governance (Accuracy & Robustness Mandates).
#     - DORA: Operational Resilience via local asset isolation.

# CONTRACT:
#     - IN: Cryptographically anchored HDF5 (NASA N-CMAPSS Specification).
#     - OUT: Normalized Parquet/LMDB datasets stored in isolated .workspace/ staging.

# SYSTEM-LEVEL INVARIANTS:
#     - LOCALITY STRICTNESS: All I/O operations must strictly resolve through 
#       'logistics.path_resolver.py'. Hardcoded paths are strictly forbidden.
#     - ATOMIC PROVISIONING: Local dataset acquisition is binary (success/exit).

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

# 2. Third-Party Dependencies
import structlog

# 3. Project Internal Dependencies
from ..logistics.factory_telemetry import (
    setup_logging, 
    bind_telemetry_context, 
    track_performance
)
from .vendor_patch_engine import apply_runtime_patches
from ..logistics.path_resolver import resolve_paths

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

# NASA N-CMAPSS Specification: Standard Column Map
RAW_COLUMNS = ["unit", "cycle", "Fc", "Hs", "alt", "Mach", "TRA", "T2"]
SENSOR_COLS = ["T24", "T30", "T48", "T50", "P15", "P2", "P30", "Ps30", "P45", "P50"]

# Transformation Parameters
NORMALIZATION_STRATEGY = "Z-SCORE"
TARGET_DTYPE = "float32"
VALIDATION_SPLIT = 0.10
SUBDATA_CHANNELS = ['X_s', 'A']

# Sliding Window Configuration
WINDOW_LENGTH = 30
WINDOW_STEP = 10
SKIP_OBSERVATIONS = 10

# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# CORE PIPELINE LOGIC
# ==============================================================================

@track_performance
def execute_feature_engineering(config_obj: SimpleNamespace, dataset_name: str):
    """Orchestrates the transformation of raw HDF5 assets into training artifacts.

    This function implements the 'Isolated Execution' pattern, ensuring that 
    vendor-specific research code (bayesrul) is correctly patched and 
    executed within infrastructure-controlled paths.

    Args:
        config_obj: Validated system path mapping from the path_resolver.
        dataset_name: Identifier for the NASA dataset (e.g., N-CMAPSS_DS02-006).

    Raises:
        FileNotFoundError: If the mandatory HDF5 source asset is missing.
        RuntimeError: If the transformation pipeline fails.
    """
    log = logger.bind(component="feature_engineering", dataset=dataset_name)
    
    # 1. Presence Verification: Ensure data was provisioned by Logistics/Infrastructure
    local_file = config_obj.data_path / "ncmapss" / f"{dataset_name}.h5"
    if not local_file.exists():
        log.critical("mandatory_asset_missing", path=str(local_file))
        raise FileNotFoundError(f"Missing required telemetry asset: {local_file}")

    log.info("asset_verified", status="ready_for_transformation")

    # 2. Environment Injection: Enable access to vendored research modules
    if str(config_obj.vendor_root) not in sys.path:
        sys.path.insert(0, str(config_obj.vendor_root))

    # 3. Patch & Execute: Transform HDF5 -> Parquet -> LMDB
    with apply_runtime_patches(config_obj):
        # We import here to ensure patches are applied correctly
        import bayesrul.ncmapss.generate_files as gen_files
        import bayesrul.ncmapss.preprocessing as prep

        # Restrict vendor execution to the audited dataset only
        gen_files.ncmapss_files = [dataset_name]
        
        # Prepare parameters for the research preprocessing engine
        args = SimpleNamespace(
            data_path = str(config_obj.data_path),
            out_path = str(config_obj.out_path),
            test_path = str(config_obj.out_path / "tests"),
            validation = VALIDATION_SPLIT,
            files = [dataset_name],
            subdata = SUBDATA_CHANNELS,
            moving_avg = False, 
            win_length = WINDOW_LENGTH,  
            win_step = WINDOW_STEP, 
            skip_obs = SKIP_OBSERVATIONS, 
            bits = 32 if TARGET_DTYPE == "float32" else 64,
        )

        log.info("transformation_triggered", phase="parquet_generation")
        prep.generate_parquet(args)
        
        log.info("transformation_triggered", phase="lmdb_serialization")
        prep.generate_lmdb(args)

    log.info("feature_engineering_successful", run_id=dataset_name)


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """CLI Entry point for standalone Feature Engineering execution."""
    # 1. Initialize System Infrastructure
    config_obj = resolve_paths()
    setup_logging(config_obj.session_log)
    
    # 2. Establish Global Traceability Context
    bind_telemetry_context(
        run_context="standalone_provisioning",
        orchestrator="feature_engineering"
    )

    # 3. Execute Core Pipeline
    # TODO: In production, dataset_name should be passed via CLI arguments
    dataset_name = "N-CMAPSS_DS02-006"
    
    try:
        execute_feature_engineering(config_obj, dataset_name)
    except Exception as e:
        logger.critical("pipeline_aborted", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
