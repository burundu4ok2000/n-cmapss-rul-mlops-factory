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
COMPONENT: TRAINING_CONFIGS
VERSION: 0.1.0

ROLE:
    Central source of truth for all training hyperparameters and transformation 
    constants. Encapsulates the 'Absolute Certainty' and 'Steel Guardian' 
    industrial stability thresholds.

REGULATORY ALIGNMENT:
    - [EU AI Act Art. 15]: Cybersecurity & Robustness. Enforces stability caps.
    - [CRA]: Reliability of ML components via deterministic configuration.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import structlog

logger = structlog.get_logger(__name__)

# ==============================================================================
# ENUMS & TYPES
# ==============================================================================

class BayesianMethod(str, Enum):
    """Supported Bayesian inference methods for the RUL engine."""
    FLIPOUT = "flipout"
    LRT = "lrt"
    RADIAL = "radial"
    MC_DROPOUT = "mc_dropout"

# ==============================================================================
# HYPERPARAMETER SCHEMAS
# ==============================================================================

@dataclass(frozen=True)
class TrainingHyperparams:
    """Industrial-grade training hyperparameters with stability defaults."""
    lr: float
    pretrain: int
    num_particles: int = 1
    prior_scale: float = 0.1
    q_scale: float = 0.1
    activation: str = "leaky_relu"
    batch_size: int = 512
    max_epochs: int = 150
    log_every_n_steps: int = 10
    clip_norm: float = 1.0
    automatic_optimization: bool = False  # V12.2.13 Lead Shield default

@dataclass(frozen=True)
class PreprocessingConfig:
    """N-CMAPSS specific transformation parameters."""
    window_length: int = 30
    window_step: int = 10
    skip_observations: int = 10
    validation_split: float = 0.10
    subdata_channels: List[str] = field(default_factory=lambda: ["X_s", "A"])
    target_dtype: str = "float32"
    moving_average: bool = False

# ==============================================================================
# FALLBACK CONFIGURATION MAP (Legacy V12.2.7 Standards)
# ==============================================================================

# [DEPRECATED] Use VendorConfigPort to load these dynamically from bayesrul artifacts.
# These remain as emergency fallbacks if the filesystem is unavailable.
FALLBACK_HYPERPARAM_MAP: Dict[BayesianMethod, TrainingHyperparams] = {
    BayesianMethod.FLIPOUT: TrainingHyperparams(
        lr=0.00003, 
        pretrain=25, 
        num_particles=8, 
        prior_scale=0.2, 
        q_scale=0.01
    ),
    BayesianMethod.LRT: TrainingHyperparams(
        lr=0.0002, 
        pretrain=10
    ),
    BayesianMethod.RADIAL: TrainingHyperparams(
        lr=0.001, 
        pretrain=5
    ),
    BayesianMethod.MC_DROPOUT: TrainingHyperparams(
        lr=0.001, 
        pretrain=0
    ),
}

# ==============================================================================
# STABILITY THRESHOLDS (Steel Guardian & Lead Shield)
# ==============================================================================

# Hard caps for CPU-based execution to prevent driver/memory instability
CPU_LR_CAP = 0.0002
CPU_BATCH_SIZE_CAP = 2560

# ==============================================================================
# DYNAMIC LOADER
# ==============================================================================

def get_config_for_method(
    method: BayesianMethod, 
    vendor_port: Optional[Any] = None
) -> TrainingHyperparams:
    """
    Retrieves the hyperparameters for a specific method.
    If vendor_port is provided, it prioritizes dynamic loading from research artifacts.
    """
    if vendor_port:
        try:
            return vendor_port.get_best_hyperparams(method)
        except Exception as e:
            logger.warning(
                "vendor_config_load_failed", 
                method=method.value, 
                error=str(e),
                action="falling_back_to_legacy_map"
            )
            
    return FALLBACK_HYPERPARAM_MAP.get(method, FALLBACK_HYPERPARAM_MAP[BayesianMethod.FLIPOUT])

def apply_stability_caps(params: TrainingHyperparams, is_cpu_mode: bool = False) -> TrainingHyperparams:
    """
    Implements the 'Steel Guardian' logic: enforces safety caps on hyperparameters 
    when running on resource-constrained hardware (CPU).
    """
    if not is_cpu_mode:
        return params
        
    capped_lr = min(params.lr, CPU_LR_CAP)
    capped_batch_size = min(params.batch_size, CPU_BATCH_SIZE_CAP)
    
    if capped_lr < params.lr or capped_batch_size < params.batch_size:
        logger.warning(
            "steel_guardian_active", 
            message="Capping hyperparameters for CPU safety",
            original_lr=params.lr, 
            capped_lr=capped_lr,
            original_batch_size=params.batch_size,
            capped_batch_size=capped_batch_size
        )
        
    # Since dataclass is frozen, we must create a new instance
    from dataclasses import replace
    return replace(params, lr=capped_lr, batch_size=capped_batch_size)
