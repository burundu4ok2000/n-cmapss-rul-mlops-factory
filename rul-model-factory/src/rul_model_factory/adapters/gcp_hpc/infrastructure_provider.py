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
COMPONENT: ADAPTER / GCP_HPC
SUBCOMPONENT: INFRASTRUCTURE PROVIDER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    Concrete implementation of InfrastructurePort for ephemeral GCP HPC
    worker nodes executing inside the Docker training container.

ENVIRONMENT CONTRACT:
    - root_anchor:    Fixed Docker WORKDIR "/app". Mandated by Dockerfile.
    - data_anchor:    "/app/data" — mounted from /tmp/data on the host worker.
    - results_anchor: "/app/results" — mounted from /tmp/data/results on the host.
    - run_id:         Read from RUN_ID environment variable. Set by startup.sh.tftpl
                      before the container is launched. FAIL-CLOSED if missing.

COMPLIANCE:
    - Zero-Trust: run_id is NOT generated locally. It is injected by the
      Terraform-managed orchestration layer, ensuring auditability.
    - EU AI Act Art. 12: Logs and session IDs are traceable to the
      infrastructure provisioning event (startup.sh.tftpl).

SIDE EFFECTS:
    - Reads os.environ at instantiation. Will raise RuntimeError if RUN_ID
      is absent (Fail-Closed policy).

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
from pathlib import Path
from typing import Any

# 2. Third-Party Dependencies
import structlog

# 3. Internal Dependencies (Core Port Contract)
from rul_model_factory.core.ports.infrastructure_port import InfrastructurePort

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# CONSTANTS
# ==============================================================================

# Docker filesystem topology — defined by Dockerfile and startup.sh.tftpl.
# [CRITICAL]: Do not change unless the Terraform/Docker layer is updated.
_DOCKER_ROOT     = Path("/app")
_DOCKER_DATA     = Path("/app/data")
_DOCKER_RESULTS  = Path("/app/results")
_DOCKER_SECRETS  = Path("/run/secrets")

# Environment variable injected by startup.sh.tftpl before container launch.
_ENV_RUN_ID = "RUN_ID"

# ==============================================================================
# ADAPTER IMPLEMENTATION
# ==============================================================================

class GcpHpcAdapter(InfrastructurePort):
    """
    GCP HPC INFRASTRUCTURE ADAPTER: Provides filesystem anchors for ephemeral
    Docker-based training workers on Google Cloud Compute Engine.

    Implements InfrastructurePort. The Core (PathResolver) receives this object
    and extracts anchors without knowing it is running inside a Docker container.

    Raises:
        RuntimeError: At instantiation if RUN_ID environment variable is not set.
            This is a Fail-Closed policy: an unidentified session must not proceed.
    """

    def __init__(self) -> None:
        """Reads RUN_ID from the environment. Raises if the variable is absent."""
        run_id = os.environ.get(_ENV_RUN_ID)
        if not run_id:
            event = "infrastructure_integrity_failure"
            message = (
                f"Industrial Composition: Fail-Closed. Environment variable '{_ENV_RUN_ID}' "
                f"is not set. Check startup.sh.tftpl."
            )
            logger.error(event, msg=message, missing_env=_ENV_RUN_ID)
            raise RuntimeError(message)
            
        self._run_id = run_id

        # Audit: Record successful cloud infrastructure binding.
        logger.info(
            "gcp_hpc_adapter_initialized",
            run_id=self._run_id,
            docker_root=str(_DOCKER_ROOT)
        )

    @property
    def root_anchor(self) -> Path:
        """Absolute path to the Docker container root (/app)."""
        return _DOCKER_ROOT

    @property
    def data_anchor(self) -> Path:
        """Absolute path to the data mount point inside the container (/app/data)."""
        return _DOCKER_DATA

    @property
    def results_anchor(self) -> Path:
        """Absolute path to the results mount point inside the container (/app/results)."""
        return _DOCKER_RESULTS

    @property
    def secrets_anchor(self) -> Path:
        """Absolute path to the secrets mount point inside the container (/run/secrets)."""
        return _DOCKER_SECRETS

    @property
    def run_id(self) -> str:

        """Session ID injected by the Terraform orchestration layer via RUN_ID env var."""
        return self._run_id

    @property
    def telemetry_sink(self) -> Any:
        """Connects the factory to the Google Cloud Logging infrastructure.

        Establishes a Standard Library Bridge: initializes the GCP Logging 
        client which hijacks Python's root logger. We then return a stdlib 
        factory to allow structlog entries to flow into the GCP stream.

        Returns:
            A LoggerFactory configured for GCP Cloud Logging or a Fail-Safe 
            fallback to standard output.
        """
        try:
            from google.cloud import logging
            
            client = logging.Client()
            # This call wires the GCP handler into Python's logging module
            client.setup_logging()
            
            # Audit: Confirm successful hijack of the root logger by GCP.
            logger.info(
                "gcp_logging_client_connected",
                project=client.project
            )
            
            # Since GCP uses 'logging', we use the stdlib factory as a bridge
            return structlog.stdlib.LoggerFactory()

        except (ImportError, Exception) as e:
            # Fail-Safe: Graceful degradation to local console logging
            logger.warning(
                "gcp_logging_connection_failed", 
                reason=str(e),
                fallback="PrintLoggerFactory"
            )
            return structlog.PrintLoggerFactory()
