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
SUBCOMPONENT: SCHEMA

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Antigravity AI (Pair: Stanislav Burundukov)

GOAL:
    - Single authority for data types and HDF5-to-Lakehouse mappings.
    - Ensures schema consistency across parallel ingestion workers.
    - Defines Hive-style partitioning columns for exclusion from Parquet.

COMPLIANCE:
    - DORA Audit Compliance: Explicit type casting for sensors and counters.
    - DuckDB/dbt Integration: Optimized for Hive-style partition pruning.

"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

import polars as pl

# ==============================================================================
# HDF5 MAPPINGS
# ==============================================================================

# Sequence of datasets to join horizontally. 
# Order MUST be preserved for physical synchronization.
SENSOR_GROUPS = ["A", "W", "X_s", "X_v", "T", "Y"]

# Map group prefixes to their 'variable' metadata groups
VAR_GROUPS = {
    "A": "A_var",
    "W": "W_var",
    "X_s": "X_s_var",
    "X_v": "X_v_var",
    "T": "T_var",
    "Y": "Y_var"
}

# ==============================================================================
# DATA TYPES & PARTITIONING
# ==============================================================================

# Columns that derive from the folder structure (Hive Partitioning).
# [CRITICAL]: These must be removed from the physical Parquet file to avoid
# schema ambiguity and storage redundancy in DuckDB/dbt.
PARTITION_COLUMNS = ["dataset", "split", "unit"]

# Core casting logic for the Wide Record.
TYPE_MAPPINGS = {
    # Counters & IDs (Discrete)
    "unit": pl.Int32,
    "cycle": pl.Int32,
    "Fc": pl.Int32,
    "hs": pl.Int32,
    
    # Telemetry (Continuous) - Float32 is sufficient for N-CMAPSS precision.
    "default_float": pl.Float32,
}

# ==============================================================================
# FILENAME SCHEMA
# ==============================================================================

LAKE_REGISTRY_FILENAME = "lake_registry.json"
TEMP_FILE_EXTENSION = ".tmp"
PARQUET_FILENAME = "data.parquet"
