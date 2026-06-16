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
SUBCOMPONENT: RUNTIME PATCHER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - Zero-Trust Execution (Hardware Abstraction Isolation)
    - EU AI Act Machine-Readability (Logistics Shimming)
    - Numerical Stability (NaN Fail-Fast Invariants)

PATCHING LAYERS:
    - Hardware: GPU Lobotomy logic to enforce CPU-only execution in secured environments.
    - Logistics: Argparse shimming to redirect vendor I/O into factory-controlled sandboxes.
    - Stability: Numerical monitoring layer that triggers immediate abort on NaN/Inf detection.

ARCHITECTURE:
    - Surgical Interception: Uses unittest.mock to modify behavior without source code changes.
    - Context Isolation: Patches are only active within the lifecycle of the Patcher context.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import sys
import argparse
import logging
from contextlib import contextmanager
from unittest.mock import patch
from typing import Dict, Any, Optional

# 2. Third-Party Dependencies
import torch
import pytorch_lightning as pl

# ==============================================================================
# RUNTIME PATCHER ENGINE
# ==============================================================================

class RuntimePatcher:
    """Surgical interception engine for vendor code.
    
    Ensures zero-trust execution and resource pinning by intercepting
    core library calls (torch, lightning, argparse) and injecting
    factory-controlled parameters.
    """

    def __init__(self, 
                 paths: Dict[str, str], 
                 hyperparams: Optional[Dict[str, Any]] = None,
                 force_cpu: bool = False):
        """Initializes the patcher with target paths and hardware constraints.
        
        Args:
            paths: Dictionary mapping vendor path names to factory sandbox locations.
            hyperparams: Configuration overrides for the vendor model.
            force_cpu: If True, activates 'GPU Lobotomy' to block CUDA access.
        """
        self.paths = paths
        self.hyperparams = hyperparams or {}
        self.force_cpu = force_cpu

    @contextmanager
    def apply(self):
        """Applies all patches and yields control back to the execution port.
        
        This method uses a nested stack of mocks to ensure that vendor 
        imports and subsequent executions are fully intercepted.
        """
        logging.info("Applying Runtime Patches to BayesRul engine...")
        
        try:
            with patch('argparse.ArgumentParser.parse_args', side_effect=self._mock_parse_args):
                with patch('torch.device', side_effect=self._mock_torch_device):
                    with patch('pytorch_lightning.Trainer', side_effect=self._mock_pl_trainer):
                        yield
        finally:
            logging.info("Runtime Patches deactivated.")

    def _mock_parse_args(self, *_args, **_kwargs):
        """Injects path and model parameters directly into the vendor's argparse."""
        namespace = argparse.Namespace()
        
        # Mapping Factory state to Vendor expected arguments
        namespace.data_path = self.paths.get('data_path', 'data/ncmapss')
        namespace.out_path = self.paths.get('out_path', 'results/ncmapss')
        namespace.model = self.hyperparams.get('model', 'lrt')
        namespace.GPU = 0 if not self.force_cpu else -1
        namespace.bits = self.hyperparams.get('bits', 32)
        namespace.moving_avg = self.hyperparams.get('moving_avg', True)
        namespace.win_length = self.hyperparams.get('win_length', 30)
        namespace.win_step = self.hyperparams.get('win_step', 10)
        namespace.validation = self.hyperparams.get('validation', 0.1)
        namespace.files = self.paths.get('files', [])
        namespace.subdata = self.hyperparams.get('subdata', ['X_s', 'A'])
        
        # Optimization specific (Optuna shimming)
        namespace.study_name = self.hyperparams.get('study_name', 'factory_study')
        namespace.sampler = 'random'
        
        return namespace

    def _mock_torch_device(self, device_str: str):
        """Enforces CPU usage if 'force_cpu' is enabled (GPU Lobotomy)."""
        if self.force_cpu:
            logging.warning("HardwareAbstraction: Redirecting torch.device to 'cpu' (GPU Lobotomy active)")
            return torch.device('cpu')
        return torch.device(device_str)

    def _mock_pl_trainer(self, *args, **kwargs):
        """Intercepts Lightning Trainer to enforce limits and add stability callbacks."""
        if self.force_cpu:
            kwargs['gpus'] = None
            if 'accelerator' in kwargs:
                kwargs['accelerator'] = 'cpu'
            logging.info("HardwareAbstraction: pl.Trainer configured for CPU execution")

        # Stability Layer: Detect NaNs and Inf early
        class StabilityCallback(pl.Callback):
            def on_train_batch_end(self, trainer, _pl_module, outputs, _batch, _batch_idx):
                loss = outputs.get('loss') if isinstance(outputs, dict) else outputs
                if loss is not None and (torch.isnan(loss) or torch.isinf(loss)):
                    logging.critical(f"NumericalStability: NaN/Inf detected at step {trainer.global_step}. ABORTING.")
                    sys.exit(1)

        callbacks = kwargs.get('callbacks', [])
        callbacks.append(StabilityCallback())
        kwargs['callbacks'] = callbacks

        return pl.Trainer(*args, **kwargs)

# ==============================================================================
# CONVENIENCE HELPERS
# ==============================================================================

@contextmanager
def surgical_interception(paths: Dict[str, str], 
                          hyperparams: Dict[str, Any], 
                          force_cpu: bool = False):
    """Convenience wrapper for the RuntimePatcher.
    
    Provides a clean interface for starting a patched vendor execution session.
    """
    patcher = RuntimePatcher(paths, hyperparams, force_cpu)
    with patcher.apply():
        yield
