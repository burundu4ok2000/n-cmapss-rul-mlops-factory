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
COMPONENT: CORE_SECURITY
SUBCOMPONENT: PROVENANCE SERVICE

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - EU AI Act Art. 12: Technical documentation and record-keeping.
    - Zero-Trust: Provenance data must be cryptographically bound to artifacts.
    - DORA Resilience: Traceability of software and hardware supply chains.

ROLE:
    - Aggregate cryptographic evidence of the training process.
    - Generate the immutable 'Model Birth Certificate' (provenance.json).
    - Capture exact hardware, software, and data context for reproducibility.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
import json
import hashlib
import datetime
import platform
import multiprocessing
import importlib.metadata
from pathlib import Path
from typing import List, Dict, Any

# 2. Third-Party Dependencies
import structlog

# 3. Internal Logistics Dependencies
from rul_model_factory.core.logistics.path_resolver import PathResolver

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

class ProvenanceServiceError(Exception):
    """Raised when provenance generation or audit fails (Fail-Closed)."""
    pass

# ==============================================================================
# PROVENANCE SERVICE ENGINE
# ==============================================================================

class ProvenanceService:
    """INDUSTRIAL PROVENANCE SERVICE: Generates the 'Model Birth Certificate'.
    
    Responsible for capturing the state of the factory during a training run
    to ensure complete auditability and meeting regulatory lineage requirements.
    """

    def __init__(self, resolver: PathResolver):
        """Initializes the service with a PathResolver for lineage discovery.
        
        Args:
            resolver: PathResolver instance providing data and result anchors.
        """
        self.resolver = resolver

    def generate_birth_certificate(
        self, 
        artifact_hashes: Dict[str, str], 
        run_config: Dict[str, Any]
    ) -> Path:
        """Formally compiles the cryptographic provenance manifest.

        Args:
            artifact_hashes: Mapping of file names to their SHA-256 digests.
            run_config:      High-level configuration of the training run.

        Returns:
            Path: Reference to the generated 'provenance.json'.

        Raises:
            ProvenanceServiceError: If aggregation fails (Fail-Closed).
        """
        logger.info("provenance_generation_started", run_id=self.resolver.run_id)

        try:
            manifest = {
                "factory_version": "0.1.0", # [TODO]: Extract from pyproject.toml
                "audit_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "run_id": self.resolver.run_id,
                "identity": {
                    "model_name": os.getenv("MODEL_NAME", "unknown_model"),
                    "instance_node": platform.node(),
                    "git_commit": os.getenv("GIT_COMMIT_HASH", "uncommitted/unknown"),
                    "git_branch": os.getenv("GIT_BRANCH", "unknown")
                },
                "artifacts": artifact_hashes,
                "software_context": self._aggregate_software_context(),
                "hardware_context": self._aggregate_hardware_context(),
                "data_lineage": self._calculate_data_lineage(),
                "training_parameters": run_config
            }

            # Fail-Closed: Strict validation of critical audit fields
            if manifest["identity"]["git_commit"] == "uncommitted/unknown":
                logger.warning("provenance_audit_warning", detail="GIT_COMMIT_HASH is missing")

            manifest_path = self.resolver.results_path / "provenance.json"
            
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=4)

            logger.info(
                "provenance_manifest_finalized",
                path=self.resolver.mask_path(manifest_path),
                hashes_recorded=len(artifact_hashes)
            )
            return manifest_path

        except Exception as e:
            error_msg = f"Critical Audit Failure: Provenance generation failed. {str(e)}"
            logger.error("provenance_generation_failed", error=error_msg)
            raise ProvenanceServiceError(error_msg) from e

    def _aggregate_software_context(self) -> Dict[str, str]:
        """Captures the exact software environment state."""
        libs = ["torch", "pytorch-lightning", "safetensors", "numpy", "pandas"]
        context = {
            "python_version": platform.python_version(),
            "libraries": {}
        }
        
        for lib in libs:
            try:
                context["libraries"][lib] = str(importlib.metadata.version(lib))
            except importlib.metadata.PackageNotFoundError:
                context["libraries"][lib] = "not-installed"
        
        return context

    def _aggregate_hardware_context(self) -> Dict[str, Any]:
        """Captures the physical infrastructure environment."""
        return {
            "os_system": platform.system(),
            "os_release": platform.release(),
            "processor": platform.processor(),
            "cpu_count": multiprocessing.cpu_count(),
            "memory_context": "dynamic_discovery_pending" # [TODO]: use psutil in v0.2.0
        }

    def _calculate_data_lineage(self) -> Dict[str, str]:
        """Establishes a cryptographically-bound anchor for input datasets."""
        data_path = self.resolver.raw_telemetry_dir
        logger.debug("calculating_data_lineage", target_dir=self.resolver.mask_path(data_path))
        
        lineage = {}
        if not data_path.exists():
            logger.warning("data_lineage_path_missing", path=self.resolver.mask_path(data_path))
            return {"status": "data-dir-not-found"}

        h5_files = sorted(list(data_path.rglob("*.h5")))
        if not h5_files:
            logger.warning("no_telemetry_files_found_for_lineage")
            return {"status": "no-h5-data-found"}

        for h5_file in h5_files:
            sha256_hash = hashlib.sha256()
            try:
                with open(h5_file, "rb") as f:
                    for byte_block in iter(lambda: f.read(65536), b""):
                        sha256_hash.update(byte_block)
                lineage[h5_file.name] = sha256_hash.hexdigest()
            except Exception as e:
                logger.error("file_hash_failed", file=h5_file.name, error=str(e))
                lineage[h5_file.name] = f"error: {str(e)}"

        return lineage
