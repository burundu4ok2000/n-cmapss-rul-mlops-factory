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
SUBCOMPONENT: PATH RESOLVER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - Zero-Trust Infrastructure Pathing (Docker Anchor Isolation)
    - EU AI Act Data Provenance (Audit Log Mapping)

LOGISTICS LAYERS:
    - Environment Detection: Differentiates between local development and Docker-HPC clusters.
    - Session Isolation: Creates run-specific artifact sandboxes using RUN_ID.
    - State Management: Enforces lifecycle transitions via FactoryStateController.

RESILIENCE:
    - Defensive Directory Provisioning: Enforces infrastructure integrity via prepare_environment().
    - Anchor Stability: Uses immutable root anchors to prevent relative path drift.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
from pathlib import Path

# 2. Third-Party Dependencies
from dotenv import load_dotenv
import structlog

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

# ==============================================================================
# VENDOR LOGIC
# ==============================================================================
# Everything in this section is dictated by the vendor's repository structure.
# Do not change unless the vendor updates their package.

VENDOR_RELATIVE_PATH = "rul-model-factory/src/rul_model_factory/vendor"
VENDOR_RESULTS_SUBDIR = "results"
RAW_DATA_SUBDIR = "ncmapss"
BEST_MODELS_SUBDIR = "best_models"

# ==============================================================================
# OUR LOGIC
# ==============================================================================
# Current "working mess" that keeps the factory running during the transition.

# --- Filesystem Taxonomy ---
PARQUET_SUBDIR = "parquet"
LMDB_SUBDIR = "lmdb"
RUNS_SUBDIR = "runs"
LOGS_SUBDIR = "local-logs"
GLOBAL_LOG_FILENAME = "factory_global.json.log"
SESSION_LOG_FILENAME = "factory_session.audit.log"
SESSION_LOGS_SUBDIR = "logs"

# --- Infrastructure Anchors ---
DOCKER_ROOT_PATH = "/app"
LOCAL_WORKSPACE_PATH = ".workspace/raw-telemetry"
ARTIFACTS_RELATIVE_PATH = "rul-model-factory/artifacts"

# --- Environment Variable SSOT ---
ENV_RUN_ID = "RUN_ID"
ENV_GCP_PROJECT = "GCP_PROJECT_ID"
ENV_FAST_FORWARD = "FAST_FORWARD_SOURCE"

# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

class FactoryStateControllerError(Exception):
    """Raised when an illegal state transition is attempted in the Controller."""
    pass

class PathResolverError(Exception):
    """Raised when filesystem resolution or access fails in the Resolver."""
    pass

# ==============================================================================
# CLASSES
# ==============================================================================

class FactoryStateController:
    """
    STRICT STATE MACHINE: Enforces the lifecycle of the RUL Factory.
    Prevents illegal transitions (e.g., training before data ingestion).
    """

    # The single source of truth for allowed transitions
    _TRANSITIONS = {
        "PENDING": {"INGEST_COMPLETE"},
        "INGEST_COMPLETE": {"FETCH_COMPLETE", "TRAIN_COMPLETE", "ABORT"},
        "FETCH_COMPLETE": {"TRAIN_COMPLETE", "FETCH_COMPLETE"},
        "TRAIN_COMPLETE": {"SAVE_TO_GCS", "ABORT"},
        "SAVE_TO_GCS": {"PROMOTE_TO_PROD", "ABORT"},
        "PROMOTE_TO_PROD": {"PENDING", "ABORT"},
        "ABORT": set()
    }

    _CURRENT_STATE = "PENDING"

    @classmethod
    def transition(cls, event: str) -> None:
        """Attempt to move the factory to a new state."""
        if event not in cls._TRANSITIONS[cls._CURRENT_STATE]:
            raise FactoryStateControllerError(
                f"ILLEGAL TRANSITION: Cannot move from '{cls._CURRENT_STATE}' "
                f"to '{event}'"
            )
        cls._CURRENT_STATE = event
        logger.info("factory_state_transition", new_state=event)

    @classmethod
    def get_state(cls) -> str:
        """Return the current immutable state."""
        return cls._CURRENT_STATE


class PathResolver:
    """
    INDUSTRIAL PATH RESOLVER: The single source of truth for the RUL Factory.
    Encapsulates environment detection (Docker/Local) and session isolation.
    """

    def __init__(self, run_id: str = None):
        """Initializes the factory map for a specific execution context.

        Args:
            run_id: Unique identifier for the training run. 
                Defaults to ENV_RUN_ID or 'local-session'.
        """
        self.run_id = run_id or os.getenv(ENV_RUN_ID, "local-session")
        
        # 1. Environment Detection (One-time check)
        self.is_docker = Path(DOCKER_ROOT_PATH).exists()
        
        # 2. Anchor Resolution
        if self.is_docker:
            self._project_root = Path(DOCKER_ROOT_PATH)
        else:
            self._project_root = Path(__file__).resolve().parents[5]
        
        # 3. Security & Context Handshake
        load_dotenv(self._project_root / ".env")
        
        logger.info("infrastructure_context_initialized", 
                    is_docker=self.is_docker, 
                    run_id=self.run_id,
                    root=str(self._project_root))

    @property
    def root(self) -> Path:
        """The absolute base directory of the project/container."""
        return self._project_root

    @property
    def vendor_root(self) -> Path:
        """Path to the vendor-provided research codebase."""
        return self.root / VENDOR_RELATIVE_PATH

    @property
    def data_path(self) -> Path:
        """Base directory for all raw telemetry and intermediate datasets."""
        if self.is_docker:
            return self.root / "data"
        return self.root / LOCAL_WORKSPACE_PATH

    @property
    def raw_telemetry_dir(self) -> Path:
        """Specific directory containing the N-CMAPSS HDF5 datasets."""
        return self.data_path / RAW_DATA_SUBDIR

    @property
    def artifacts_root(self) -> Path:
        """Base directory for all generated outputs (logs, results, models)."""
        if self.is_docker:
            return self.root / "results"
        return self.root / ARTIFACTS_RELATIVE_PATH

    @property
    def results_path(self) -> Path:
        """Isolated sandbox directory for the current session's artifacts."""
        return self.artifacts_root / RUNS_SUBDIR / self.run_id

    @property
    def logs_path(self) -> Path:
        """Centralized log storage for global factory audit trails."""
        return self.artifacts_root / LOGS_SUBDIR

    @property
    def session_log_dir(self) -> Path:
        """Specific log directory for the active execution session."""
        return self.results_path / SESSION_LOGS_SUBDIR

    @property
    def best_models_root(self) -> Path:
        """The industrial anchor point for all Pareto-optimal model configs."""
        return self.vendor_root / VENDOR_RESULTS_SUBDIR / RAW_DATA_SUBDIR / BEST_MODELS_SUBDIR

    def get_best_model_config(self, model_family: str, model_id: str = "000") -> Path:
        """Resolves the path to a specific Pareto-optimal model configuration.

        Args:
            model_family: Name of the model architecture (e.g., 'DEEP_ENSEMBLE').
            model_id: Specific config index (e.g., '003'). Defaults to '000'.

        Returns:
            Absolute Path to the JSON configuration file.
        """
        config_path = self.best_models_root / model_family / f"{model_id}.json"
        if not config_path.exists():
            logger.error("model_config_not_found", path=str(config_path))
            raise PathResolverError(f"Industrial Registry: Configuration {model_id} for {model_family} not found at {config_path}")
        return config_path

    def prepare_environment(self) -> None:
        """Enforces the existence of the industrial infrastructure (directories).
        
        This method must be called before any I/O operations to guarantee
        that results and log sinks are available on the filesystem.
        """
        dirs_to_create = [
            self.results_path,
            self.logs_path,
            self.session_log_dir
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)
            logger.debug("directory_verified", path=str(d))

    # ==========================================================================
    # LEGACY COMPATIBILITY (v.12.x.x)
    # ==========================================================================

    @property
    def project_id(self) -> str: return os.getenv(ENV_GCP_PROJECT)
    @property
    def global_logs(self) -> Path: return self.logs_path / GLOBAL_LOG_FILENAME
    @property
    def session_log(self) -> Path: return self.session_log_dir / SESSION_LOG_FILENAME
    
    # Aliases for legacy coordination logic
    @property
    def data_dir(self) -> Path: return self.data_path
    @property
    def results_dir(self) -> Path: return self.results_path
    @property
    def out_path(self) -> Path: return self.results_path

# ==============================================================================
# LEGACY COMPATIBILITY FUNCTIONS (v.12.x.x)
# ==============================================================================

def resolve_paths(run_id: str = None) -> PathResolver:
    """LEGACY SHIM: Resolves canonical locations and prepares the environment.

    Maintains backward compatibility with legacy scripts while internally
    leveraging the new class-based PathResolver architecture.

    Args:
        run_id: Optional session identifier.

    Returns:
        An initialized PathResolver instance.
    """
    resolver = PathResolver(run_id=run_id)
    resolver.prepare_environment()
    return resolver
