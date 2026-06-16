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
SUBCOMPONENT: H5-UTILS

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Antigravity AI (Pair: Stanislav Burundukov)

GOAL:
    - Encapsulate all h5py interactions to prevent library leakage into the main pipeline.
    - Provide secure HDF5 metadata decoding for dynamic column mapping.
    - Manage the centralized lake_registry.json for pipeline idempotency.
    - Implement SHA-256 checksums for data provenance validation.

"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library
import hashlib
import json
import fcntl
from pathlib import Path
from typing import Any

# 2. Third-Party
import h5py
import numpy as np
import polars as pl
import structlog

# 3. Internal
from n_cmapss_pipeline.ingestion.schema import LAKE_REGISTRY_FILENAME

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# HDF5 I/O UTILITIES
# ==============================================================================

def decode_var_names(h5_file: h5py.File, group_key: str) -> list[str]:
    """Decodes binary metadata from HDF5 variable groups into human-readable names.

    Args:
        h5_file: Open h5py.File handle.
        group_key: Name of the variable group (e.g., 'X_s_var').

    Returns:
        List of decoded UTF-8 strings representing column names.
    """
    if group_key not in h5_file:
        logger.warning("h5_metadata_group_missing", group=group_key)
        return []
    
    raw_names = h5_file[group_key][:]
    # H5 metadata in N-CMAPSS is often 2D (Nx1) or 1D array of binary strings.
    decoded = [name[0].decode("utf-8") if isinstance(name, np.ndarray) else name.decode("utf-8") 
               for name in raw_names]
    return decoded


def read_group_as_frame(h5_file: h5py.File, group_key: str, columns: list[str]) -> pl.DataFrame:
    """Reads a specific HDF5 dataset group and wraps it in a Polars DataFrame.

    Args:
        h5_file: Open h5py.File handle.
        group_key: Data group key (e.g., 'X_s_dev').
        columns: List of column names to assign.

    Returns:
        Polars DataFrame containing the group data.
    """
    if group_key not in h5_file:
        raise KeyError(f"Data group '{group_key}' not found in HDF5 file.")
    
    data = h5_file[group_key][:]
    
    # Defensive check: Ensure column names match the actual data width
    if data.ndim == 2 and data.shape[1] != len(columns):
        raise ValueError(
            f"Dimension mismatch in group '{group_key}': "
            f"Data has {data.shape[1]} columns, but schema has {len(columns)}."
        )
    elif data.ndim == 1 and len(columns) != 1:
        # Handle 1D arrays if they are intended to be a single column
        data = data.reshape(-1, 1)
        
    return pl.DataFrame(data, schema=columns)

# ==============================================================================
# IDEMPOTENCY & PROVENANCE
# ==============================================================================

def compute_sha256(path: Path) -> str:
    """Calculates the SHA-256 hash of a file for provenance tracking.

    Uses a chunked reading strategy to remain memory-efficient for large H5 files.

    Args:
        path: Path to the file.

    Returns:
        Hexadecimal representation of the SHA-256 hash.
    """
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class RegistryManager:
    """Centralized manager for the Lakehouse processing registry.

    Implements file-locking (fcntl) to ensure atomic updates when multiple
    processes are running parallel ingestion.
    """

    def __init__(self, lake_root: Path) -> None:
        """Initializes the registry manager.

        Args:
            lake_root: Root directory of the telemetry lakehouse.
        """
        self.path = lake_root / LAKE_REGISTRY_FILENAME
        if not self.path.exists():
            lake_root.mkdir(parents=True, exist_ok=True)
            with open(self.path, "w") as f:
                json.dump({}, f)

    def is_processed(self, filename: str, current_hash: str) -> bool:
        """Checks if a file with the given hash has already been successfully processed.

        Args:
            filename: Name of the source file.
            current_hash: Calculated SHA-256 of the source file.

        Returns:
            True if the file is in the registry with a matching hash.
        """
        registry = self._load()
        return registry.get(filename) == current_hash

    def mark_processed(self, filename: str, current_hash: str) -> None:
        """Records a successful processing event in the central registry.

        Args:
            filename: Name of the source file.
            current_hash: SHA-256 hash of the processed source.
        """
        with open(self.path, "r+") as f:
            # Advisory lock for inter-process safety
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                registry = json.load(f)
                registry[filename] = current_hash
                f.seek(0)
                json.dump(registry, f, indent=4)
                f.truncate()
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _load(self) -> dict[str, str]:
        """Loads the registry from disk with a shared lock."""
        with open(self.path, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            try:
                return json.load(f)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
