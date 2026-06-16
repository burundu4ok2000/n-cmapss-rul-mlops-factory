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
SUBCOMPONENT: VENDOR_CONFIG_PORT

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - CRA Article 10: Lifecycle integrity and supply chain transparency.
    - EU AI Act Section 2: Accountability via clean abstraction of vendor components.

ROLE:
    Defines the abstract interface for hyperparameter discovery. 
    This port decouples the Core Domain from the specifics of the vendor's 
    internal JSON registry and research artifact structures.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from abc import ABC, abstractmethod
from typing import Dict

# 2. Internal Domain Dependencies
from ..domain.training_configs import BayesianMethod, TrainingHyperparams

# ==============================================================================
# VENDOR CONFIG PORT
# ==============================================================================

class VendorConfigPort(ABC):
    """Interface for retrieving hyperparameters from vendor research artifacts.
    
    Provides a deterministic abstraction for accessing the vendor's 'best_models'
    registry without exposing the underlying filesystem or schema details.
    """
    
    @abstractmethod
    def get_best_hyperparams(self, method: BayesianMethod) -> TrainingHyperparams:
        """Fetches and parses optimized hyperparameters for a Bayesian method.
        
        Args:
            method: The target inference architecture to lookup.
            
        Returns:
            TrainingHyperparams: The mapped configuration.
        """
        pass

    @abstractmethod
    def get_all_best_configs(self) -> Dict[BayesianMethod, TrainingHyperparams]:
        """Retrieves best configurations for all supported Bayesian methods."""
        pass
