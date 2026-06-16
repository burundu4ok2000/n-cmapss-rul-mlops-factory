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
COMPONENT: CORE
SUBCOMPONENT: PORTS / INFRASTRUCTURE PORT

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    Formal contract (Port) for infrastructure access.
    Decouples the Core from any specific execution environment
    (Docker, Local, Cloud, Edge).

COMPLIANCE:
    - Dependency Inversion Principle (SOLID/DIP): Core depends on this
      abstraction, never on concrete adapters.
    - EU AI Act Art. 13: Transparency. Any adapter implementing this port
      is a formally declared execution context.

CONTRACT:
    - IN:  Nothing. The port is a pure declaration of requirements.
    - OUT: Filesystem anchors and a session identity consumed by PathResolver.

INVARIANTS:
    - All properties MUST return resolved, absolute Path objects.
    - run_id MUST be a non-empty, filesystem-safe string.
    - No property may perform I/O or network calls.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

# 2. Third-Party Dependencies

# ==============================================================================
# PORT DEFINITION
# ==============================================================================

class InfrastructurePort(ABC):
    """
    HEXAGONAL PORT: Formal contract for infrastructure access.

    Any adapter (LocalAdapter, GcpHpcAdapter, etc.) MUST implement
    all abstract properties. Python will raise a TypeError at instantiation
    time if any property is missing — enforcing the contract at runtime.

    Usage (in __main__.py / Composition Root):
        adapter = LocalAdapter()                            # implements this port
        resolver = PathResolver(adapter)                    # core receives the port, not the adapter
    """

    @property
    @abstractmethod
    def root_anchor(self) -> Path:
        """Absolute path to the filesystem root of the execution environment.

        Examples:
            - Docker worker: Path("/app")
            - Local Dell:    Path("{{ROOT}}")
        """

    @property
    @abstractmethod
    def data_anchor(self) -> Path:
        """Absolute path to the directory containing raw N-CMAPSS telemetry (.h5 files).

        Examples:
            - Docker worker: Path("/app/data")
            - Local:         Path("{{ROOT}}/.workspace/raw-telemetry")
        """

    @property
    @abstractmethod
    def results_anchor(self) -> Path:
        """Absolute path to the root directory for all session output artifacts.

        The PathResolver will create run-specific subdirectories (runs/{run_id}/)
        under this anchor. The adapter provides the base; the Core owns the schema.

        Examples:
            - Docker worker: Path("/app/results")
            - Local:         Path("{{ROOT}}/rul-model-factory/artifacts")
        """

    @property
    @abstractmethod
    def secrets_anchor(self) -> Path:
        """Absolute path to the directory containing sensitive credentials (keys, tokens).

        Required for cryptographic signing and authenticated cloud ingestion.

        Examples:
            - Local:         Path("{{ROOT}}/.secrets")
            - Docker:        Path("/run/secrets")
        """

    @property
    @abstractmethod
    def run_id(self) -> str:
        """Unique, filesystem-safe identifier for the current training session.
        # APPROVED. Stan Buren, 25.04.2026. 
        Format: "rul-factory-<timestamp>-<run_id>-<model_name>-<infrastructure_type>"

        # TODO: add example when the code is ready
        Examples:
            - GCP worker:     "..."
            - Local:          "..."
        """

    @property
    @abstractmethod
    def telemetry_sink(self) -> Any:
        """Provides the logger factory appropriate for this execution environment.

        Returns:
            A concrete structlog LoggerFactory (e.g., PrintLoggerFactory, stdlib.LoggerFactory).
        """
