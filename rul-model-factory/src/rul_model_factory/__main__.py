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
COMPONENT: COMPOSITION ROOT
SUBCOMPONENT: __main__ (Entry Point)

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    The single point where the Hexagonal Architecture is "assembled".
    This is the ONLY file in the project that is allowed to know about
    both the Core and the Adapters simultaneously.

    It reads the FACTORY_MODE environment variable and wires the correct
    adapter into the Core (PathResolver), then hands off to the training
    orchestration layer.

COMPLIANCE:
    - Composition Root Pattern: Dependency injection happens here and only here.
    - EU AI Act Art. 13: The selected adapter is logged for traceability.

ENVIRONMENT VARIABLES:
    - FACTORY_MODE: Controls which infrastructure adapter is loaded.
        "local"  -> LocalAdapter     (default, for development)
        "cloud"  -> GcpHpcAdapter    (requires RUN_ID to be set by startup.sh)

USAGE:
    uv run --project rul-model-factory -m rul_model_factory
    uv run --project rul-model-factory -m rul_model_factory  # FACTORY_MODE=cloud

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
import sys

# 2. Third-Party Dependencies
import structlog

# ==============================================================================
# CONSTANTS
# ==============================================================================

ENV_FACTORY_MODE = "FACTORY_MODE"
MODE_LOCAL       = "local"
MODE_CLOUD       = "cloud"

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

class CompositionError(Exception):
    """Raised when the Hexagonal Architecture assembly fails at the root."""
    pass

# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# COMPOSITION ROOT
# ==============================================================================

def _build_adapter():
    """Selects and instantiates the correct infrastructure adapter.

    Reads FACTORY_MODE from the environment and returns the matching adapter.
    This function contains the ONLY if/else in the entire codebase that
    branches on the execution environment.

    Returns:
        A concrete InfrastructurePort implementation.

    Raises:
        ValueError: If FACTORY_MODE contains an unrecognized value.
    """
    mode = os.environ.get(ENV_FACTORY_MODE, MODE_LOCAL).lower()
    
    # Audit: Record the infrastructure mode requested by the environment.
    logger.info("factory_mode_selected", mode=mode)

    if mode == MODE_LOCAL:
        from rul_model_factory.adapters.local.infrastructure_provider import LocalAdapter
        return LocalAdapter()

    elif mode == MODE_CLOUD:
        from rul_model_factory.adapters.gcp_hpc.infrastructure_provider import GcpHpcAdapter
        return GcpHpcAdapter()

    else:
        event = "factory_composition_failed"
        message = (
            f"Industrial Composition: Unknown FACTORY_MODE='{mode}'. "
            f"Valid values: '{MODE_LOCAL}', '{MODE_CLOUD}'."
        )
        logger.error(event, msg=message, mode=mode)
        raise CompositionError(message)


def main() -> None:
    """Factory entry point. Assembles the Hexagonal Architecture and launches training."""
    from rul_model_factory.core.logistics.path_resolver import PathResolver
    from rul_model_factory.core.logistics.factory_telemetry import setup_logging

    # 1. WIRE: Select adapter and inject into Core.
    adapter  = _build_adapter()
    resolver = PathResolver(adapter)

    # 2. TELEMETRY: Initialize the audit stream based on resolver coordinates.
    setup_logging(resolver, sink=adapter.telemetry_sink)

    # 3. LOGISTICS: Provision output directories.
    resolver.prepare_environment()

    # 4. LAUNCH: Hand off to the training orchestration layer.
    # [TODO]: Wire TrainingEngine here once core/training/ is implemented.
    logger.info(
        "factory_initialized",
        run_id=resolver.run_id,
        root=resolver.root,            # Automatically masked by structlog processor
        results=resolver.results_path, # Automatically masked by structlog processor
        training_engine="not_wired",
    )


# ==============================================================================
# ENTRYPOINT
# ==============================================================================

if __name__ == "__main__":
    main()
