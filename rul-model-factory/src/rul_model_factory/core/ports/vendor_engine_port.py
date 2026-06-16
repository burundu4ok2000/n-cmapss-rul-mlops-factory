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
COMPONENT: PORTS
SUBCOMPONENT: VENDOR_ENGINE_PORT

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - CRA Article 10: Lifecycle integrity and supply chain transparency.
    - EU AI Act Section 2: Accountability via clean abstraction of vendor components.

ROLE:
    Defines the abstract interface for interacting with external research codebases
    (BayesRul). This port isolates the Core Domain from the specifics of 
    monkeypatching, vendor I/O conventions, and third-party ML framework quirks.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from abc import ABC, abstractmethod

# 2. Internal Domain Dependencies
from ..domain.training_configs import (
    BayesianMethod, 
    TrainingHyperparams, 
    PreprocessingConfig
)

# ==============================================================================
# VENDOR ENGINE PORT
# ==============================================================================

class VendorEnginePort(ABC):
    """Interface for the industrial execution engine.
    
    Ensures that any vendor-specific research logic is wrapped in a 
    deterministic, patched environment. Controls the entire data preparation
    and model training lifecycle.
    """

    @abstractmethod
    def prepare_datasets(
        self, 
        dataset_id: str, 
        config: PreprocessingConfig
    ) -> None:
        """Orchestrates the feature engineering pipeline.
        
        Transitions data through the lifecycle: 
        HDF5 (Raw) -> Parquet (Normalized) -> LMDB (Tensor-ready).
        
        Args:
            dataset_id: The identifier of the N-CMAPSS dataset (e.g., 'DS02').
            config: Transformation and windowing parameters.
        """
        pass

    @abstractmethod
    def train_model(
        self, 
        method: BayesianMethod, 
        hyperparams: TrainingHyperparams,
        dataset_id: str
    ) -> None:
        """Executes a complete Bayesian model training cycle.
        
        Args:
            method: The Bayesian inference method to utilize.
            hyperparams: Validated training hyperparameters.
            dataset_id: The target dataset for training.
        """
        pass

    @abstractmethod
    def check_environment_integrity(self) -> bool:
        """Verifies that required vendor modules are correctly injected and accessible.
        
        Checks for dependencies such as bayesrul, tyxe, and pytorch-lightning.
        
        Returns:
            bool: True if the runtime environment is safe for execution.
        """
        pass
