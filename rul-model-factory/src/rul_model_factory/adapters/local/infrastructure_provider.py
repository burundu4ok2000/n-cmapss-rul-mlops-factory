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
COMPONENT: ADAPTER / LOCAL
SUBCOMPONENT: INFRASTRUCTURE PROVIDER

VERSION: 0.1.0
STATUS: ACTIVE
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    Concrete implementation of InfrastructurePort for the local
    development workstation (Ubuntu Linux environment).

ENVIRONMENT CONTRACT:
    - root_anchor: Resolved by walking up from __file__ until '.factory_root' sentinel
      is found. No hardcoded absolute paths, no package manager assumptions.
    - data_anchor: Project-relative ".workspace/raw-telemetry/" directory.
    - results_anchor: Project-relative "rul-model-factory/artifacts/" directory.
    - run_id: Auto-generated from current UTC datetime. No external dependencies.

SENTINEL CONTRACT:
    - A file named '.factory_root' MUST exist in the monorepo root.
    - If absent, LocalAdapter will refuse to initialize (Fail-Fast policy).
    - This file is the single source of truth for "where is home".

SIDE EFFECTS:
    - Reads system clock once at instantiation to generate run_id.
    - No network calls. No environment variable reads (pure local).

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# 2. Third-Party Dependencies
import structlog

# 3. Internal Dependencies (Core Port Contract)
from rul_model_factory.core.ports.infrastructure_port import InfrastructurePort

# ==============================================================================
# CONSTANTS
# ==============================================================================

# Sentinel file that marks the monorepo root — the single source of truth.
_SENTINEL      = ".factory_root"
_DATA_RELATIVE = ".workspace/raw-telemetry"
_RESULTS_RELATIVE = "rul-model-factory/artifacts"
_RUN_ID_PREFIX = "local"

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# PRIVATE HELPER FUNCTIONS
# ==============================================================================

def _find_project_root(max_depth: int = 10) -> Path:
    """Walks up the directory tree from this file until the sentinel is found.

    Args:
        max_depth: Maximum number of parent directories to search before 
                  aborting. Prevents infinite loops or excessive scanning.

    Returns:
        Absolute Path to the directory containing '.factory_root'.

    Raises:
        FileNotFoundError: If the sentinel is not found within max_depth.
    """
    current = Path(__file__).resolve()
    for i, directory in enumerate([current, *current.parents]):
        if i > max_depth:
            break
        if (directory / _SENTINEL).exists():
            return directory
            
    event = "project_root_sentinel_missing"
    message = (
        f"Industrial Integrity: Project root sentinel '{_SENTINEL}' not found "
        f"within {max_depth} levels. Searched from: {current}"
    )
    logger.error(event, msg=message, search_origin=current, max_depth=max_depth)
    raise FileNotFoundError(message)

# ==============================================================================
# ADAPTER IMPLEMENTATION
# ==============================================================================

class LocalAdapter(InfrastructurePort):
    """
    LOCAL INFRASTRUCTURE ADAPTER: Provides filesystem anchors for the local
    development workstation running Ubuntu.

    Implements InfrastructurePort. The Core (PathResolver) receives this object
    and extracts anchors without knowing it is running on a local machine.
    """

    def __init__(self) -> None:
        """Initializes the local adapter and resolves environment coordinates.

        Resolves the project root by walking up the directory tree until the 
        '.factory_root' sentinel is found. Generates a unique session ID.
        
        Raises:
            RuntimeError: If the project root cannot be determined.
        """
        try:
            # 1. RESOLVE PROJECT ROOT
            # Walk up from this file until we find the '.factory_root' sentinel.
            # This approach is immune to package restructuring or manager changes.
            self._root = _find_project_root()

            # 2. GENERATE SESSION IDENTITY
            # Local runs use a 'local-YYYYMMDDTHHmmZ' format for easy sorting.
            _ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ")
            self._run_id = f"{_RUN_ID_PREFIX}-{_ts}"

        except Exception as e:
            # Industrial Guard: Log the failure before the factory crashes.
            logger.error("local_adapter_init_failed", error=str(e))
            raise RuntimeError(f"Failed to initialize LocalAdapter: {e}") from e

        # Audit: Record successful local infrastructure binding.
        logger.info(
            "local_adapter_initialized",
            run_id=self._run_id,
            root_resolved=True
        )

    @property
    def root_anchor(self) -> Path:
        """Absolute path to the project root on the local workstation."""
        return self._root

    @property
    def data_anchor(self) -> Path:
        """Absolute path to the local raw telemetry directory."""
        return self._root / _DATA_RELATIVE

    @property
    def results_anchor(self) -> Path:
        """Absolute path to the local artifact storage root."""
        return self._root / _RESULTS_RELATIVE

    @property
    def run_id(self) -> str:
        """Auto-generated session ID based on UTC timestamp."""
        return self._run_id

    @property
    def telemetry_sink(self) -> Any:
        """Returns a LoggerFactory configured for the local console.

        This is the default telemetry sink for workstation development, providing 
        immediate, human-readable feedback in the terminal without infrastructure 
        overhead or cloud authentication.

        Returns:
            A PrintLoggerFactory for standard output logging.
        """
        # Local development uses the simple Print factory for real-time debugging
        return structlog.PrintLoggerFactory()
