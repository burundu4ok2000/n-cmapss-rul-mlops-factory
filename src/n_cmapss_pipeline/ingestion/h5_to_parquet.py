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
DOMAIN: N-CMAPSS-TELEMETRY-PIPELINE
COMPONENT: INGESTION
SUBCOMPONENT: H5-TO-PARQUET

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Antigravity AI (Pair: Stanislav Burundukov)

GOAL:
    - High-performance, memory-efficient HDF5 to Parquet conversion.
    - Implement Hive-style partitioning (dataset/split/unit).
    - Ensure atomic writes and centralized idempotency.
    - Support parallel multi-file processing with process-level isolation.

"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library
import logging
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# 2. Third-Party
import h5py
import polars as pl
import structlog

# 3. Internal
from n_cmapss_pipeline.ingestion.schema import (
    SENSOR_GROUPS,
    VAR_GROUPS,
    TYPE_MAPPINGS,
    PARTITION_COLUMNS,
    PARQUET_FILENAME,
    TEMP_FILE_EXTENSION
)
from n_cmapss_pipeline.ingestion.h5_utils import (
    decode_var_names,
    read_group_as_frame,
    compute_sha256,
    RegistryManager
)

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

# Configure structured logging
def configure_telemetry():
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=True)
        ],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

logger = structlog.get_logger(__name__)

# ==============================================================================
# CORE PIPELINE LOGIC
# ==============================================================================

def process_file(h5_path: Path, lake_root: Path, registry: RegistryManager) -> bool:
    """Orchestrates the conversion of a single HDF5 file into partitioned Parquet.

    Args:
        h5_path: Path to the source .h5 file.
        lake_root: Root directory for the Parquet lakehouse.
        registry: Initialized RegistryManager.

    Returns:
        True if the file was processed successfully (or skipped due to idempotency).
    """
    dataset_id = h5_path.stem  # e.g., N-CMAPSS_DS02-006
    
    # Contextual logging for this specific file
    log = logger.bind(dataset=dataset_id)
    
    # 1. Idempotency Check
    current_hash = compute_sha256(h5_path)
    if registry.is_processed(h5_path.name, current_hash):
        log.info("ingestion_skipped", reason="already_processed")
        return True
    
    log.info("ingestion_started")
    
    try:
        with h5py.File(h5_path, "r") as h5_file:
            # Process both 'dev' and 'test' splits within the same H5
            for split in ["dev", "test"]:
                _process_split(h5_file, dataset_id, split, lake_root, log)
        
        # 2. Mark as processed ONLY after all splits are successfully written
        registry.mark_processed(h5_path.name, current_hash)
        log.info("ingestion_completed")
        return True
        
    except Exception as e:
        log.error("ingestion_failed", error=str(e), exc_info=True)
        return False


def _process_split(h5_file: h5py.File, dataset_id: str, split: str, lake_root: Path, log):
    """Processes a specific split (dev/test) and writes to Hive-partitioned lakehouse."""
    log = log.bind(split=split)
    log.debug("processing_split")

    # 1. Read and fuse groups
    frames = []
    for group_prefix in SENSOR_GROUPS:
        data_key = f"{group_prefix}_{split}"
        var_key = VAR_GROUPS.get(group_prefix)
        
        # Fallback for Y group which lacks a Y_var metadata group
        if group_prefix == "Y":
            columns = ["RUL"]
        elif var_key:
            columns = decode_var_names(h5_file, var_key)
        else:
            log.warning("missing_metadata_mapping", group=group_prefix)
            continue
            
        df_group = read_group_as_frame(h5_file, data_key, columns)
        frames.append(df_group)
    
    # Horizontal Fusion (Polars performs this efficiently in Rust)
    df_wide = pl.concat(frames, how="horizontal")
    
    # 2. Type Casting & Sanitization
    # Apply type mappings defined in schema
    df_wide = df_wide.with_columns([
        pl.col(col).cast(dtype) 
        for col, dtype in TYPE_MAPPINGS.items() if col in df_wide.columns
    ])
    
    # Default everything else to Float32 if not specified
    float_cols = [c for c in df_wide.columns if c not in TYPE_MAPPINGS]
    df_wide = df_wide.with_columns([
        pl.col(c).cast(TYPE_MAPPINGS.get("default_float", pl.Float32)) 
        for c in float_cols
    ])

    # 3. Hive Partitioning & Atomic Write
    # Group by 'unit' to write one Parquet file per engine
    units = df_wide["unit"].unique().to_list()
    
    for unit_id in units:
        unit_df = df_wide.filter(pl.col("unit") == unit_id)
        
        # Exclude partition columns from the data itself (Hive standard)
        # We only drop columns that actually exist in the frame to avoid errors.
        cols_to_drop = [c for c in PARTITION_COLUMNS if c in unit_df.columns]
        unit_df = unit_df.drop(cols_to_drop)
        
        # Define Path: lake/dataset={DS}/split={S}/unit={U}/data.parquet
        partition_path = (
            lake_root 
            / f"dataset={dataset_id}" 
            / f"split={split}" 
            / f"unit={unit_id}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        
        target_file = partition_path / PARQUET_FILENAME
        temp_file = partition_path / (PARQUET_FILENAME + TEMP_FILE_EXTENSION)
        
        # Atomic Write: Temp -> Parquet
        try:
            unit_df.write_parquet(temp_file, compression="zstd")
            os.replace(temp_file, target_file)
            log.debug("unit_written", unit=unit_id)
        except Exception as e:
            log.error("unit_write_failed", unit=unit_id, error=str(e))
            if temp_file.exists():
                temp_file.unlink()
            raise

# ==============================================================================
# CLI & MULTIPROCESSING ORCHESTRATION
# ==============================================================================

def main():
    """Main entrypoint for parallel ingestion."""
    configure_telemetry()
    
    # Configuration (In production, these would be in PathResolver or ENV)
    RAW_DIR = Path(".workspace/raw-telemetry")
    LAKE_ROOT = Path(".workspace/telemetry_lake")
    
    # Auto-adjust parallelism based on RAM/CPU
    # [HINT]: Limit workers if memory becomes a bottleneck
    max_workers = min(os.cpu_count() or 4, 4) 
    
    log = logger.bind(component="orchestrator")
    log.info("pipeline_initialized", max_workers=max_workers)
    
    registry = RegistryManager(LAKE_ROOT)
    h5_files = list(RAW_DIR.glob("*.h5"))
    
    if not h5_files:
        log.warning("no_source_files_found", search_path=str(RAW_DIR))
        return

    log.info("discovery_completed", files_found=len(h5_files))

    # Multi-file processing via ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_file, f, LAKE_ROOT, registry): f.name 
            for f in h5_files
        }
        
        for future in as_completed(futures):
            filename = futures[future]
            try:
                success = future.result()
                if not success:
                    log.error("file_failed", filename=filename)
            except Exception as e:
                log.error("worker_crashed", filename=filename, error=str(e))

    log.info("pipeline_finished")

if __name__ == "__main__":
    main()
