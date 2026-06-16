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
DOMAIN: AEROSPACE-ANALYTICS
COMPONENT: DASHBOARD
SUBCOMPONENT: LOGIC

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    - Pure, testable dashboard logic for N-CMAPSS metrics.
    - Resolves DuckDB connection paths.
    - Provides data loaders and filtering mechanisms for app.py.

COMPLIANCE:
    - Strict Separation of Concerns (Hexagonal Architecture).
    - Idempotent and Read-Only.
    - Strict Type Hinting.
"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library
import os
from collections.abc import Sequence
from pathlib import Path

# 2. Third-Party
import duckdb
import polars as pl

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

DEFAULT_DUCKDB_PATH = Path(
    "/home/donald_trump/developer/n-cmapss-agentic-factory/dwh/dbt/marts.duckdb"
)
ENV_VAR_DUCKDB_PATH = "DASHBOARD_DUCKDB_PATH"

ALLOWED_TABLES = frozenset(
    {
        "fct_engine_health_per_cycle",
        "rpt_engine_pnl",
        "rpt_safety_alerts",
    }
)

# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================


def get_duckdb_path() -> Path:
    """Resolves the absolute path to the local DuckDB instance."""
    path_str = os.getenv(ENV_VAR_DUCKDB_PATH, str(DEFAULT_DUCKDB_PATH))
    return Path(path_str)


# ==============================================================================
# DATA LOADERS
# ==============================================================================


def load_data(duckdb_path: Path, table_name: str) -> pl.DataFrame:
    """Loads an allowed table from the Gold layer into a Pandas DataFrame.

    Args:
        duckdb_path: The path to the duckdb database.
        table_name: The name of the table to load (must be in ALLOWED_TABLES).

    Returns:
        pl.DataFrame containing the loaded data.
    """
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table_name}' is not an approved Gold mart.")

    if not duckdb_path.exists():
        raise FileNotFoundError(
            f"Database file not found at {duckdb_path}. Please run dbt build."
        )

    # Read-only connection ensures idempotence
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        # Pushing down limits is theoretically possible, but for these mart tables (7473 rows),
        # a full pull is safe and fast.
        df = con.execute(f"SELECT * FROM {table_name}").pl()

    return df


# ==============================================================================
# FILTERS
# ==============================================================================


def filter_by_dataset_and_unit(
    dataframe: pl.DataFrame,
    *,
    dataset_ids: Sequence[int] | None = None,
    unit_ids: Sequence[int] | None = None,
) -> pl.DataFrame:
    """Filters the aerospace datasets by primary identifiers.

    Args:
        dataframe: The source dataframe containing 'dataset_id' and 'unit_id'.
        dataset_ids: Optional sequence of allowed dataset IDs.
        unit_ids: Optional sequence of allowed unit IDs.

    Returns:
        Filtered pl.DataFrame.
    """
    if dataframe.is_empty():
        return dataframe.clone()

    if dataset_ids and unit_ids:
        return dataframe.filter(
            pl.col("dataset_id").is_in(dataset_ids) & pl.col("unit_id").is_in(unit_ids)
        )
    elif dataset_ids:
        return dataframe.filter(pl.col("dataset_id").is_in(dataset_ids))
    elif unit_ids:
        return dataframe.filter(pl.col("unit_id").is_in(unit_ids))

    return dataframe.clone()


def filter_safety_alerts(
    dataframe: pl.DataFrame,
    *,
    priorities: Sequence[str] | None = None,
) -> pl.DataFrame:
    """Filters safety alerts by priority level (Critical, Warning, Normal)."""
    if dataframe.is_empty() or not priorities:
        return dataframe.clone()

    return dataframe.filter(pl.col("alert_priority").is_in(priorities))


# ==============================================================================
# INSIGHTS & AGGREGATIONS
# ==============================================================================


def derive_safety_kpis(dataframe: pl.DataFrame) -> dict[str, int]:
    """Derives key performance indicators for Fleet Safety."""
    if dataframe.is_empty():
        return {"total_engines": 0, "critical_alerts": 0, "warning_alerts": 0}

    total_engines = dataframe.height

    critical = dataframe.filter(pl.col("alert_priority") == "Critical").height
    warnings = dataframe.filter(pl.col("alert_priority") == "Warning").height

    return {
        "total_engines": total_engines,
        "critical_alerts": critical,
        "warning_alerts": warnings,
    }


def derive_pnl_kpis(dataframe: pl.DataFrame) -> dict[str, float]:
    """Derives aggregate financial penalties across the fleet."""
    if dataframe.is_empty():
        return {"total_excess_gallons": 0.0, "total_market_penalty_usd": 0.0}

    return {
        "total_excess_gallons": float(dataframe.get_column("excess_gallons").sum()),
        "total_market_penalty_usd": float(dataframe.get_column("market_penalty_usd").sum()),
    }
