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
SUBCOMPONENT: PATH RESOLVER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    - Translator between vendor's internal path schema and the Factory's schema.
    - Provider of all canonical paths consumed by the training pipeline.
    - Completely environment-agnostic: receives all filesystem anchors
      via dependency injection (InfrastructurePort).

COMPLIANCE:
    - Dependency Inversion Principle (SOLID/DIP): depends on InfrastructurePort
      abstraction, never on concrete adapters or os.environ.
    - EU AI Act Data Provenance: all session paths derive from a validated run_id
      supplied by the adapter layer.

LOGISTICS LAYERS:
    - Vendor Translation: Maps vendor's internal directory schema (results/ncmapss/
      best_models/) to abstract method calls consumable by the Core.
    - Session Isolation: Creates run-specific artifact sandboxes under results_anchor.

RESILIENCE:
    - Defensive Directory Provisioning: Enforces infrastructure integrity via
      prepare_environment().
    - Anchor Stability: Anchors are immutable after construction; no runtime
      path mutation is permitted.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from enum import Enum
from importlib import resources
from pathlib import Path

# 2. Third-Party Dependencies
import structlog

# 3. Internal Dependencies (Core Contract)
from rul_model_factory.core.ports.infrastructure_port import InfrastructurePort
from rul_model_factory.core.logistics.factory_telemetry import mask_path

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

# Vendor package root — resolved via importlib, independent of filesystem layout.
# [INVARIANT]: Do not change. This path is determined by the Python package
# structure, not by the execution environment.
_VENDOR_ROOT                    = Path(resources.files("rul_model_factory.vendor"))

# --- Vendor Name ---
_VENDOR_NAME                    = "bayesrul"

# --- Vendor Internal Schema ---
# [CRITICAL]: These subdirectory names are hardcoded inside bayesrul source.
# Only change if the vendor updates their package structure.
_VENDOR_NCMAPSS_SUBDIR          = "ncmapss"
_VENDOR_RESULTS_SUBDIR          = "results"
_VENDOR_BESTMODELS_SUBDIR       = "best_models"
VENDOR_BEST_MODEL_ID            = "000"  # Canonical index for Pareto-optimal config

# --- Factory Filesystem Schema ---
# The canonical directory taxonomy for all Factory-generated artifacts.
# These are OUR rules. Adapters must respect this schema.
PARQUET_SUBDIR                  = "parquet"
LMDB_SUBDIR                     = "lmdb"
RUNS_SUBDIR                     = "runs"
LOGS_SUBDIR                     = "local-logs"
SESSION_LOGS_SUBDIR             = "logs"

# --- Audit Filenames ---
GLOBAL_LOG_FILENAME             = "factory_global.json.log"
SESSION_LOG_FILENAME            = "factory_session.audit.log"

# ==============================================================================
# DOMAIN TYPES & REGISTRIES
# ==============================================================================

# [TEMPORARY]: In the future (v0.2.0), we will move this to 
# `rul_model_factory.core.domain.models` to avoid circular imports 

class ModelArchitecture(str, Enum):
    """Registry of supported Bayesian neural network architectures."""
    DEEP_ENSEMBLE = "DEEP_ENSEMBLE"
    FLIPOUT       = "FLIPOUT"
    LRT           = "LRT"
    MC_DROPOUT    = "MC_DROPOUT"
    RADIAL        = "RADIAL"


# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

# [TODO]: Integrate with a centralized 'core.errors' module in v0.2.0.
# Currently maintained as a local placeholder for logistics-specific failures.
class PathResolverError(Exception):
    """Raised when filesystem resolution or access fails in the Resolver."""
    pass

# ==============================================================================
# CLASSES
# ==============================================================================

class PathResolver:
    """
    INDUSTRIAL PATH RESOLVER: The single source of truth for all canonical paths
    in the RUL Factory.

    Receives all filesystem anchors via an InfrastructurePort. Has zero knowledge
    of the execution environment (Docker, Local, Cloud). This is the Core.

    Usage:
        adapter  = LocalAdapter()     # or GcpHpcAdapter()
        resolver = PathResolver(adapter)
        config   = resolver.get_best_model_config("DEEP_ENSEMBLE")
    """

    # APPROVED. Stan Buren, 24.04.2026. 
    def __init__(self, infra: InfrastructurePort) -> None:
        """Initializes the factory logistics hub using injected infrastructure anchors.

        Performs a Zero-Trust integrity validation of all provided coordinates
        before allowing the state to be assigned.

        Args:
            infra: Concrete implementation of InfrastructurePort providing
                   the physical filesystem anchors.
        """
        # Guard Clause: Granular integrity check for diagnostic feedback.
        anchors = {
            "root_anchor":    infra.root_anchor,
            "data_anchor":    infra.data_anchor,
            "results_anchor": infra.results_anchor,
            "secrets_anchor": infra.secrets_anchor,
            "run_id":         infra.run_id,
        }
        missing = [name for name, val in anchors.items() if not val]

        if missing:
            self._report_incident(
                event="infrastructure_integrity_failure",
                message=(
                    f"Industrial Integrity: Received incomplete infrastructure anchors. "
                    f"Missing: {', '.join(missing)}. "
                    "The factory cannot safely initialize without full coordinates."
                ),
                missing_anchors=missing
            )

        # Fail-Fast: State assignment proceeds only if integrity check passes.
        self._root           = infra.root_anchor
        self._data_anchor    = infra.data_anchor
        self._results_anchor = infra.results_anchor
        self._secrets_anchor = infra.secrets_anchor
        self.run_id          = infra.run_id

        # Log Secure Initialization.
        logger.info(
            "path_resolver_initialized",
            run_id=self.run_id,
            anchors_validated=True
        )
        # Masked paths prevent metadata leakage in production/audit logs.
        logger.debug(
            "infrastructure_anchors_details",
            root=self._mask_path(self._root),
            data=self._mask_path(self._data_anchor),
            results=self._mask_path(self._results_anchor),
            secrets=self._mask_path(self._secrets_anchor),
        )

    # ==========================================================================
    # INFRASTRUCTURE ANCHORS (Injected by Adapter)
    # ==========================================================================

    @property
    def root(self) -> Path:
        """The absolute base directory of the current execution environment.

        Returns:
            Path object pointing to the project or container root.
        """
        return self._root

    @property
    def data_path(self) -> Path:
        """The absolute path to the raw N-CMAPSS telemetry storage.

        Returns:
            Path object containing source .h5 datasets.
        """
        return self._data_anchor

    @property
    def raw_telemetry_dir(self) -> Path:
        """The specific subdirectory containing raw HDF5 telemetry files.

        Maps the generic data_anchor to the vendor-specific directory schema.

        Returns:
            Path object resolved to '{{DATA}}/ncmapss/'.
        """
        return self._data_anchor / _VENDOR_NCMAPSS_SUBDIR

    @property
    def artifacts_root(self) -> Path:
        """The absolute base directory for all factory-generated output sinks.

        Returns:
            Path object pointing to the results anchor.
        """
        return self._results_anchor

    @property
    def results_path(self) -> Path:
        """The isolated session sandbox for current run-specific artifacts.

        Returns:
            Path object resolved to '{{RESULTS}}/runs/{{RUN_ID}}/'.
        """
        return self._results_anchor / RUNS_SUBDIR / self.run_id

    @property
    def secrets_path(self) -> Path:
        """The absolute path to the directory containing sensitive credentials.

        Returns:
            Path object pointing to the secrets anchor.
        """
        return self._secrets_anchor

    def get_secret_path(self, secret_filename: str) -> Path:
        """Resolves the path to a specific credential or secret file.

        Args:
            secret_filename: Name of the secret file (e.g., 'cosign.key').

        Returns:
            Absolute Path to the secret file.
        """
        secret_path = self.secrets_path / secret_filename

        if secret_path.exists():
            # Audit: Record successful secret resolution (masked path).
            logger.debug(
                "secret_resolved",
                filename=secret_filename,
                path=self._mask_path(secret_path)
            )
            return secret_path

        # Incident Reporting: Missing critical credential
        self._report_incident(
            event="secret_not_found",
            message=f"Security Integrity: Secret file '{secret_filename}' not found.",
            path=secret_path
        )

    @property
    def logs_path(self) -> Path:
        """The centralized global directory for factory-wide audit trails.

        Returns:
            Path object resolved to '{{RESULTS}}/local-logs/'.
        """
        return self._results_anchor / LOGS_SUBDIR

    @property
    def session_log_dir(self) -> Path:
        """The dedicated log directory scoped to the current execution session.

        Returns:
            Path object resolved to '{{RESULTS_PATH}}/logs/'.
        """
        return self.results_path / SESSION_LOGS_SUBDIR

    # ==========================================================================
    # VENDOR TRANSLATION (Vendor Path Schema -> Factory Access Methods)
    # ==========================================================================

    @property
    def vendor_root(self) -> Path:
        """The absolute path to the internal vendor package (bayesrul).

        Resolved dynamically via importlib to ensure environment-independence.

        Returns:
            Path object pointing to the vendor library source.
        """
        return _VENDOR_ROOT

    @property
    def best_models_root(self) -> Path:
        """The vendor's central model registry for Pareto-optimal weights.

        Returns:
            Path object following the vendor's internal directory schema.
        """
        return (
            _VENDOR_ROOT
            / _VENDOR_NAME
            / _VENDOR_RESULTS_SUBDIR
            / _VENDOR_NCMAPSS_SUBDIR
            / _VENDOR_BESTMODELS_SUBDIR
        )

    def get_best_model_config(
        self, 
        model_family: ModelArchitecture, 
        model_id: str = VENDOR_BEST_MODEL_ID
    ) -> Path:
        """Resolves the path to a specific Pareto-optimal model configuration.

        Args:
            model_family: Member of ModelArchitecture enum.
            model_id: Specific config index (e.g., '003'). Defaults to vendor's '000'.

        Returns:
            Absolute Path to the JSON configuration file.
        """
        config_name = f"{model_id}.json"
        config_path = self.get_model_family_dir(model_family) / config_name

        if config_path.exists():
            # Audit: Record successful model configuration resolution.
            logger.info(
                "model_config_resolved",
                architecture=model_family,
                config_id=model_id,
                path=config_path # Automatically masked
            )
            return config_path

        # Incident Reporting: Only reached if the config is missing
        self._report_incident(
            event="model_config_not_found",
            message=(
                f"Industrial Registry: Configuration '{model_id}' "
                f"for family '{model_family}' not found."
            ),
            path=config_path,
            architecture=model_family
        )

    def get_model_family_dir(self, model_family: ModelArchitecture) -> Path:
        """Resolves the absolute path to a specific model family directory.
        
        Args:
            model_family: Member of ModelArchitecture enum.
            
        Returns:
            Absolute Path to the family-specific configuration registry.
        """
        family_path = self.best_models_root / model_family

        if family_path.is_dir():
            # Diagnostic: Record successful model family directory resolution.
            logger.debug(
                "model_family_dir_resolved",
                architecture=model_family,
                path=family_path # Automatically masked
            )
            return family_path

        # Incident Reporting: Missing architecture directory in vendor tree
        self._report_incident(
            event="model_family_missing",
            message=(
                f"Industrial Registry: Model family directory '{model_family}' "
                f"not found or is not a directory."
            ),
            path=family_path,
            architecture=model_family
        )

    # ==========================================================================
    # AUDIT / ERROR HANDLING
    # ==========================================================================

    def prepare_environment(self) -> None:
        """Provisions all required output directories before any I/O operations.

        Must be called once after construction. Guarantees that results and
        log sinks exist on the filesystem before the pipeline writes to them.
        """
        dirs_to_create = [
            self.results_path,
            self.logs_path,
            self.session_log_dir,
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)
            logger.debug("directory_provisioned", path=self._mask_path(d))

        # Audit: Record completion of filesystem provisioning.
        logger.info(
            "factory_environment_provisioned",
            directories_created=len(dirs_to_create),
            session_sandbox=self._mask_path(self.results_path)
        )

    def _report_incident(
        self, 
        event: str, 
        message: str, 
        path: Path = None, 
        **kwargs
    ) -> None:
        """Centralized structured reporting for all PathResolver failures.

        [TODO]: Redirect this to a global 'IncidentManager' in future iterations.
        
        Sanitizes any Path objects in metadata (and the main path argument)
        before logging to prevent host-level information leakage. Always 
        terminates with an exception.

        Args:
            event:   Structured log event key (e.g., 'model_config_not_found').
            message: Human-readable failure description.
            path:    Optional Path that caused the failure (will be masked).
            **kwargs: Arbitrary audit metadata attached to the log record.
        """
        # Append masked path to message for exception clarity if provided.
        full_message = message
        if path:
            masked_path = self._mask_path(path)
            full_message = f"{message} [Target: {masked_path}]"
            kwargs["incident_path"] = path

        sanitized_kwargs = {
            k: (self._mask_path(v) if isinstance(v, Path) else v)
            for k, v in kwargs.items()
        }
        logger.error(event, msg=full_message, **sanitized_kwargs)
        raise PathResolverError(full_message)

    def mask_path(self, path: Path) -> str:
        """Instance-scoped wrapper around the shared mask_path utility.

        Binds the current project root so callers don't need to pass
        it explicitly. See factory_telemetry.mask_path for full docs.
        """
        return mask_path(path, root=self._root)

    # ==========================================================================
    # AUDIT FILENAMES
    # ==========================================================================

    @property
    def global_log(self) -> Path:
        """The absolute path to the global factory-wide audit log file.

        Returns:
            Path object resolved to '{{LOGS_PATH}}/factory_global.json.log'.
        """
        return self.logs_path / GLOBAL_LOG_FILENAME

    @property
    def session_log(self) -> Path:
        """The absolute path to the current session's structured audit file.

        Returns:
            Path object resolved to '{{SESSION_LOG_DIR}}/factory_session.audit.log'.
        """
        return self.session_log_dir / SESSION_LOG_FILENAME

    # ==========================================================================
    # LEGACY COMPATIBILITY (v.12.x.x)
    # ==========================================================================
    # [NOTE]: These aliases exist solely to avoid breaking the _old_cloud_trainer
    # scripts during the migration window. They will be removed in v0.3.0.

    @property
    def data_dir(self) -> Path: 
        """[DEPRECATED]: Alias for data_path (v12.x compatibility)."""
        return self.data_path
    
    @property
    def results_dir(self) -> Path: 
        """[DEPRECATED]: Alias for results_path (v12.x compatibility)."""
        return self.results_path
    
    @property
    def out_path(self) -> Path: 
        """[DEPRECATED]: Alias for results_path (v12.x compatibility)."""
        return self.results_path

# ==============================================================================
# LEGACY COMPATIBILITY FUNCTIONS (v.12.x.x)
# ==============================================================================
# [NOTE]: resolve_paths() is deprecated. Use PathResolver(adapter) directly
# via __main__.py composition root. Will be removed in v0.3.0.

def resolve_paths(infra: InfrastructurePort) -> PathResolver:
    """LEGACY ENTRYPOINT: Initializes the Resolver and provisions the environment.

    [DEPRECATED]: This shim is provided for compatibility with old cloud workers.
    New code should instantiate PathResolver directly via Composition Root.

    Args:
        infra: A concrete implementation of the InfrastructurePort.

    Returns:
        An initialized and filesystem-ready PathResolver instance.
    """
    resolver = PathResolver(infra)
    resolver.prepare_environment()
    return resolver
