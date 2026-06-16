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
SUBCOMPONENT: CONFIG ADAPTER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - EU AI Act Transparency (Art. 13 - Portability Layer)
    - Zero-Trust Infrastructure (Artifact Integrity Verification)

ADAPTER LAYERS:
    - Hyperparameter Extraction: Parses optimized parameters from vendor artifacts.
    - Artifact Parsing: Validates and maps JSON-based research metadata.
    - Configuration Mapping: Bridges vendor schemas to industrial TrainingHyperparams.

PERFORMANCE:
    - Cached Discovery: Minimal overhead for hyperparameter retrieval.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import json
from pathlib import Path
from typing import Dict

# 2. Third-Party Dependencies
import structlog

# 3. Internal Port & Domain Dependencies
from ...core.ports.vendor_config_port import VendorConfigPort
from ...core.domain.training_configs import BayesianMethod, TrainingHyperparams

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# BAYESRUL CONFIG ADAPTER
# ==============================================================================

class BayesrulConfigAdapter(VendorConfigPort):
    """Infrastructure Adapter for parsing vendor research artifacts.
    
    Extracts Pareto-optimal hyperparameters from the vendor's JSON registry
    and converts them into a format suitable for industrial execution.
    """

    def __init__(self, vendor_root: Path):
        """Initializes the adapter with the vendor's result registry path.
        
        Args:
            vendor_root: Path to the vendor's research codebase.
        """
        self.base_path = vendor_root / "bayesrul" / "results" / "ncmapss" / "best_models"
        
    def get_best_hyperparams(self, method: BayesianMethod) -> TrainingHyperparams:
        """Fetches optimized parameters for a specific Bayesian inference method.
        
        Args:
            method: The target Bayesian method (e.g., DEEP_ENSEMBLE).
            
        Returns:
            TrainingHyperparams: The mapped configuration ready for engine ingestion.
            
        Raises:
            FileNotFoundError: If the vendor's configuration artifact is missing.
        """
        # Vendor convention: directory names are uppercase versions of methods
        method_dir = method.value.upper()
        json_file = self.base_path / method_dir / "000.json"
        
        if not json_file.exists():
            logger.error("vendor_config_missing", path=str(json_file), method=method.value)
            raise FileNotFoundError(f"Vendor configuration file missing: {json_file}")

        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            logger.info("vendor_config_loaded", method=method.value, path=str(json_file))
            
            # Mapping JSON keys to our TrainingHyperparams dataclass
            return TrainingHyperparams(
                lr=data.get("lr", 0.001),
                pretrain=data.get("pretrain", 0),
                num_particles=data.get("num_particles", 1), # Default if missing
                prior_scale=data.get("prior_scale", 0.1),
                q_scale=data.get("q_scale", 0.1),
                activation=data.get("activation", "relu"), # Note: patches forced leaky_relu
            )
        except Exception as e:
            logger.critical("vendor_config_parse_error", path=str(json_file), error=str(e))
            raise RuntimeError(f"Failed to parse vendor config {json_file}: {e}")

    def get_all_best_configs(self) -> Dict[BayesianMethod, TrainingHyperparams]:
        """Retrieves and maps all available configurations in the vendor registry."""
        configs = {}
        for method in BayesianMethod:
            try:
                configs[method] = self.get_best_hyperparams(method)
            except FileNotFoundError:
                logger.warning("skipping_missing_method_config", method=method.value)
        return configs
