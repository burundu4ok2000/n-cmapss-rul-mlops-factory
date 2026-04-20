"""
CORE DOMAIN: Vendor Patching & Execution Shim (V12.1.5)
ROLE: Runtime interception and adaptation of the research code.

This is the 'Hacker Department'. It contains all monkeypatching and 
environment shims required to enforce industrial constraints on the 
original 'bayesrul' research codebase without modifying its source.
"""

import os
import sys
import runpy
import argparse
import multiprocessing
import h5py
import numpy as np
from pathlib import Path
from functools import wraps
from unittest.mock import patch
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

import torch
import pytorch_lightning as pl
from unittest.mock import patch

# --- End Global Hardware Abstraction ---

# Relative import for internal workers
from .parallel_execution import (
    preprocessing_worker, parquet_worker, worker_init, RobustMinMaxAggregate
)

def apply_runtime_patches(paths):
    """
    Implements the 'Surgical Interception' strategy for runtime path decoupling.
    
    Patches 'argparse' to redirect hardcoded vendor I/O defaults to 
    infrastructure-managed secure directories. This ensures environment 
    portability across local development and Cloud Compute instances.

    Args:
        paths (SimpleNamespace): Formal mapping of redirected filesystem targets.
    """
    original_add_argument = argparse.ArgumentParser.add_argument

    def patched_add_argument(self, *args, **kwargs):
        # Intercept and redirect data path CLI argument.
        if '--data-path' in args or 'data_path' in args:
            if 'default' in kwargs:
                old_val = kwargs['default']
                kwargs['default'] = str(paths.data_path)
                print(f"[INFO:Orchestration] Redirected data_path: {old_val} -> {kwargs['default']}")
        
        # Intercept and redirect output storage path CLI argument.
        if any(opt in args for opt in ['--out-path', 'out_path', '--out-dir', 'out_dir']):
            if 'default' in kwargs:
                old_val = kwargs['default']
                kwargs['default'] = str(paths.results_path)
                print(f"[INFO:Orchestration] Redirected out_path:  {old_val} -> {kwargs['default']}")
        
        # V12.2.8: Inject mission-critical arguments that are missing in vendor's train_model.py
        if '--fit-context' in args or '--fit_context' in args:
             # Already present (e.g. from our own interceptor or if vendor adds it later)
             pass
        elif '--model-name' in args: # Use model-name as an anchor to inject new args
             original_add_argument(self, '--fit-context', type=str, default='flipout')
             original_add_argument(self, '--fit_context', type=str, default='flipout')
                
        return original_add_argument(self, *args, **kwargs)

    # Return the patch object for use in a context manager
    return patch('argparse.ArgumentParser.add_argument', patched_add_argument)

def execute_vendor_module(module_name: str, paths):
    """
    Orchestrates the patched execution of a vendor submodule.
    
    Args:
        module_name (str): Dot-notation identifier for the target bayesrul 
            execution module (e.g., 'ncmapss.train_model').
        paths (SimpleNamespace): System-wide path context.
    """
    # --- Aggressive Global Hardware Abstraction (V12.2.8) ---
    # We patch torch.device and is_available IMMEDIATELY to prevent driver initialization.
    torch.cuda.is_available = lambda: False
    original_device = torch.device

    class DeviceMeta(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, original_device)

    class patched_device(metaclass=DeviceMeta):
        def __new__(cls, *args, **kwargs):
            if args and isinstance(args[0], str) and 'cuda' in args[0]:
                print(f"[INFO:Orchestration] Global GPU-to-CPU Redirection: {args[0]} -> cpu")
                return original_device('cpu')
            return original_device(*args, **kwargs)

    torch.device = patched_device
    # Aggressively patch already-loaded modules that might have local device refs
    for mod_name, mod in list(sys.modules.items()):
        if mod and (mod_name.startswith('bayesrul') or mod_name.startswith('rul_model_factory')):
            if hasattr(mod, 'device') and mod.device is original_device:
                setattr(mod, 'device', patched_device)
            if hasattr(mod, 'torch') and hasattr(mod.torch, 'device'):
                mod.torch.device = patched_device

    # 1. Detection of CPU/GPU mode via CLI telemetry
    is_cpu_mode = False
    for i, arg in enumerate(sys.argv):
        if arg == '--GPU' and i + 1 < len(sys.argv):
            try:
                if int(sys.argv[i+1]) < 0:
                    is_cpu_mode = True
                    print("[INFO:Orchestration] Computational backend redirection: Forcing CPU-mode.")
            except ValueError: pass

    # Hardware Isolation for DORA audits
    if is_cpu_mode:
        os.environ["CUDA_VISIBLE_DEVICES"] = ""

    # --- Numerical Stability Patches (V12.2.5) ---
    # --- Numerical Stability Patches (V12.2.8) ---
    import pyro.optim
    original_clipped_adam = pyro.optim.ClippedAdam
    
    @wraps(original_clipped_adam)
    def patched_clipped_adam(params, *args, **kwargs):
        # Enforce strict gradient clipping (1.0) regardless of vendor default (15.0)
        # In Pyro, params is often a dict containing 'clip_norm'
        if isinstance(params, dict) and 'clip_norm' in params:
            old_clip = params['clip_norm']
            params['clip_norm'] = 1.0
            print(f"[INFO:Stability] Neutered aggressive clip_norm: {old_clip} -> 1.0")
        return original_clipped_adam(params, *args, **kwargs)
    
    pyro.optim.ClippedAdam = patched_clipped_adam

    # --- Lightning Trainer Patching ---
    # --- Lightning Trainer Patching ---
    original_init = pl.Trainer.__init__
    @wraps(original_init)
    def transparent_init(self, *args, **kwargs):
        # V12.2.9: Removed 'terminate_on_nan' as it is deprecated in PL 1.7+
        # We enforce industrial defaults for stability instead
        kwargs['max_epochs'] = kwargs.get('max_epochs', 150)
        kwargs['log_every_n_steps'] = 10
        
        if is_cpu_mode:
            kwargs['gpus'] = None
            kwargs['devices'] = 1 # Correct convention for PL 1.7 CPU mode
            kwargs['accelerator'] = 'cpu'
            print("[INFO:Orchestration] Safe CPU forced in Lightning Trainer.")
        return original_init(self, *args, **kwargs)
    
    pl.Trainer.__init__ = transparent_init

    # --- DataLoader Patching ---
    # --- DataLoader Patching ---
    from torch.utils.data import DataLoader as OriginalLoader
    original_loader_init = OriginalLoader.__init__
    @wraps(original_loader_init)
    def patched_loader_init(self, *args, **kwargs):
        if kwargs.get('num_workers', 0) == 0:
            kwargs['num_workers'] = multiprocessing.cpu_count() // 2
            print(f"[INFO:Orchestration] High-throughput I/O initialization: n={kwargs['num_workers']}")
        return original_loader_init(self, *args, **kwargs)
    OriginalLoader.__init__ = patched_loader_init

    # --- Vendor Injection (Turbo & Stability) ---
    import bayesrul.ncmapss.preprocessing as bp
    import bayesrul.inference.vi_bnn as vi
    # Capture original references
    _original_generate_lmdb = bp.generate_lmdb
    _original_vi_init = vi.VI_BNN.__init__

    # --- Master Configuration Map (V12.2.7 - Derived from vendor best_models) ---
    MASTER_CONFIG_MAP = {
        'flipout': {'lr': 0.0001, 'pretrain': 10},
        'lrt': {'lr': 0.0002, 'pretrain': 10},
        'radial': {'lr': 0.001, 'pretrain': 5},
        'mc_dropout': {'lr': 0.001, 'pretrain': 0}, # Success path of April 14th
    }

    @wraps(_original_vi_init)
    def patched_vi_init(self, *args, **kwargs):
        # Intercept hyperparams to enforce V12.2.8 'Absolute Certainty' logic
        # Signature: def __init__(self, args, data, hyp, GPU)
        # So in *args: args[0]=cli_args, args[1]=data, args[2]=hyp
        if len(args) > 2:
            user_args = args[0]
            hyp = args[2]
            
            # Identify the Bayesian method (fit_context)
            # Prioritize CLI argument if we successfully injected/parsed it
            method = getattr(user_args, 'fit_context', 'flipout')
            if method == 'null' or not method: method = 'flipout'
            
            config = MASTER_CONFIG_MAP.get(method.lower(), MASTER_CONFIG_MAP['flipout'])

            if hyp and 'lr' in hyp:
                old_lr = hyp['lr']
                # Apply Golden LR only if user hasn't overridden it via CLI
                # Vendor default for LR is 0.002 in train_model.py:101
                if user_args.lr == 0.0001 or user_args.lr == 0.002 or user_args.lr == 0.001: 
                    hyp['lr'] = config['lr']
                    print(f"[INFO:Stability] V12.2.11 Applied {method.upper()} Golden LR: {old_lr} -> {hyp['lr']}")
                else:
                    # Steel Guardian: Ultra-Hard cap for CPU mode (1e-4)
                    hyp['lr'] = user_args.lr
                    if is_cpu_mode and hyp['lr'] > 0.0002:
                        hyp['lr'] = 0.0002
                        print(f"[WARNING:Stability] Steel Guardian: Capping User LR {user_args.lr} -> 0.0002 for absolute CPU safety")
                    else:
                        print(f"[INFO:Stability] Using User-Defined LR: {hyp['lr']}")
            
            # Synchronize fit_context inside the dict
            hyp['fit_context'] = method

            # Force Master Pretraining (5 epochs) for all Bayesian modes
            if hyp and hyp.get('pretrain', 0) == 0 and config['pretrain'] > 0:
                # Prioritize CLI --pretrain if user provided one
                if hasattr(user_args, 'pretrain') and user_args.pretrain > 0:
                    hyp['pretrain'] = user_args.pretrain
                else:
                    hyp['pretrain'] = config['pretrain']
                print(f"[INFO:Stability] V12.2.12 Applied {method.upper()} Master Pretraining: {hyp['pretrain']} epochs")

            # Force Master Activation (Leaky ReLU for stability)
            if hyp and hyp.get('activation') != 'leaky_relu':
                old_act = hyp.get('activation')
                hyp['activation'] = 'leaky_relu'
                print(f"[INFO:Stability] V12.2.12 Applied Master Activation: {old_act} -> leaky_relu")
        
        return _original_vi_init(self, *args, **kwargs)
    vi.VI_BNN.__init__ = patched_vi_init

    # --- DataModule Stabilization (V12.2.12) ---
    import bayesrul.ncmapss.dataset as ds
    _original_dm_init = ds.NCMAPSSDataModule.__init__
    @wraps(_original_dm_init)
    def patched_dm_init(self, data_path, batch_size, all_dset=False):
        if is_cpu_mode and batch_size > 2560:
            print(f"[WARNING:Stability] Steel Guardian: Capping Batch Size {batch_size} -> 2560")
            batch_size = 2560
        return _original_dm_init(self, data_path, batch_size, all_dset=all_dset)
    ds.NCMAPSSDataModule.__init__ = patched_dm_init
    
    # --- Telemetry Stabilization (V12.2.11) ---
    import bayesrul.lightning_wrappers.bayesian as bw
    
    # V12.2.13: Lead Shield - Disable automatic optimization
    _original_vibnn_init = bw.VIBnnWrapper.__init__
    @wraps(_original_vibnn_init)
    def patched_vibnn_init(self, *args, **kwargs):
        _original_vibnn_init(self, *args, **kwargs)
        self.automatic_optimization = False
        print("[INFO:Stability] V12.2.13 Lead Shield: Automatic Optimization DISABLED")
    bw.VIBnnWrapper.__init__ = patched_vibnn_init

    _original_training_step = bw.VIBnnWrapper.training_step
    
    @wraps(_original_training_step)
    def patched_training_step(self, *args, **kwargs):
        # Execute original logic (logs metrics and performs manual optimization)
        _original_training_step(self, *args, **kwargs)
        
        # Pull the logged MSE to satisfy Lightning's telemetry requirement
        mse = self.trainer.callback_metrics.get('mse/train')
        
        if mse is not None and torch.as_tensor(mse).isnan():
            print("[CRITICAL:Stability] Lead Shield: NaN detected. Divergence captured.")
            # Trigger graceful exit to allow artifact harvesting
            sys.exit(1)
            
        return {"loss": mse if mse is not None else torch.tensor(0.0)}

    bw.VIBnnWrapper.training_step = patched_training_step

    def patched_compute_scalers(args, typ, arg=""):
        print(f"[INFO:Performance] Parallel Scaler Active (Cores: {multiprocessing.cpu_count()})")
        # Use paths.data_path resolved by the resolver
        tasks = [(f, paths.data_path, typ, args.subdata, args.validation) for f in args.files]
        with ProcessPoolExecutor(
            max_workers=multiprocessing.cpu_count(),
            initializer=worker_init,
            initargs=(str(paths.vendor_root), str(paths.root_dir))
        ) as executor:
            raw_results = list(tqdm(executor.map(preprocessing_worker, tasks), total=len(args.files)))
        
        # Layer 2 Aggregation: Filter out results from corrupted assets
        results = [r for r in raw_results if r is not None]
        
        if not results:
            # Audit Trail: If no data survives, the job has no objective value.
            print("[CRITICAL:DataIntegrity] ALL datasets failed integrity check. Aborting.")
            sys.exit(1)
        
        # Dynamic Update: Ensure subsequent phases only process validated files
        # Each successful worker result contains short_name at index 3 (via list(cols) returning task data)
        # Wait, the return is: return sum, var_sum, len, list(cols)
        # We need to correlate filenames back. Let's improve the list(cols) part or trust the order.
        # Actually, let's just use the filenames we sent in 'tasks'.
        valid_files = [args.files[i] for i, r in enumerate(raw_results) if r is not None]
        if len(valid_files) < len(args.files):
            skipped = set(args.files) - set(valid_files)
            print(f"[WARNING:DataIntegrity] Dropping corrupted datasets from pipeline: {list(skipped)}")
            args.files = valid_files

        # Layer 3: Feature Alignment Guard
        reference_cols = results[0][3]
        for idx, (s, v_sum, n, cols) in enumerate(results):
            if cols != reference_cols:
                print(f"[CRITICAL:DataIntegrity] Feature mismatch in dataset at index {idx}!")
                print(f"Reference: {reference_cols}")
                print(f"Current:   {cols}")
                raise RuntimeError("Distributed feature misalignment detected. Aborting pipeline.")

        global_sum = results[0][0] * 0
        global_var_sum = results[0][1] * 0
        total_count = sum(r[2] for r in results)
        for s, v_sum, n, _ in results:
            global_sum += s
            global_var_sum += v_sum
        mean = global_sum / total_count
        std = np.sqrt(np.maximum(global_var_sum / total_count, 1e-9))
        print(f"[INFO:Orchestration] Statistical Convergence Achieved. Features: {len(reference_cols)}, Samples: {total_count}")
        return reference_cols, mean, std

    def patched_generate_parquet(args):
        # ------------------------------------------------------------------------------
        # BINGO: Auto-Discovery of Data Assets
        # The vendor code hardcoded 5 files in generate_files.py. We dynamically 
        # discover what Terraform actually provisioned into the container volume.
        # ------------------------------------------------------------------------------
        available_files = []
        for search_dir in [paths.data_path, paths.data_path / "ncmapss"]:
            if search_dir.exists():
                for f in search_dir.glob("*.h5"):
                    # Layer 2 Security: Verify HDF5 Header Integrity before scheduling
                    if h5py.is_hdf5(f):
                        available_files.append(f.stem)
                    else:
                        print(f"[CRITICAL:DataIntegrity] Dataset {f.name} is CORRUPTED. Skipping asset.")
                    # We dynamically re-map paths.data_path to where the files actually are
                    paths.data_path = search_dir
        
        if available_files:
            # Sort to maintain determinism
            args.files = sorted(list(set(available_files)))
            print(f"[INFO:Orchestration] Auto-discovered {len(args.files)} datasets for processing: {args.files}")
        else:
            print(f"[WARNING:Preflight] No .h5 datasets found dynamically. Defaulting to vendor list.")

        typ = np.float32 if args.bits == 32 else np.float64
        columns, mean, std = bp.compute_scalers(args, typ)
        
        # --- Surgical Fast-Forward Guard (V12.2.1) ---
        ff_source = os.getenv("FAST_FORWARD_SOURCE")
        # Primary check: session-specific directory
        parquet_dir = paths.results_path / "parquet"
        # Fallback check: root results directory (legacy/fallback)
        fallback_dir = Path("/app/results/parquet")
        
        found_parquet = False
        if parquet_dir.exists() and any(parquet_dir.glob("*.parquet")):
            found_parquet = True
        elif fallback_dir.exists() and any(fallback_dir.glob("*.parquet")):
            parquet_dir = fallback_dir
            found_parquet = True

        if ff_source and found_parquet:
            print(f"[INFO:Security] Fast-Forward Active: Recycling Parquet from {parquet_dir}")
        else:
            if ff_source:
                print(f"[WARNING:Security] Fast-Forward requested but NO artifacts found at {parquet_dir}")
            
            print(f"[INFO:Performance] Phase 2: Generating Parquets (Parallel on {multiprocessing.cpu_count()} cores)...")
            tasks = []
            for filename in args.files:
                tasks.append((
                    filename, str(paths.data_path), str(paths.results_path), 
                    typ, args.subdata, args.validation, args.moving_avg, 
                    columns, mean, std
                ))
            with ProcessPoolExecutor(
                max_workers=multiprocessing.cpu_count(),
                initializer=worker_init,
                initargs=(str(paths.vendor_root), str(paths.root_dir))
            ) as executor:
                list(tqdm(executor.map(parquet_worker, tasks), total=len(tasks)))
        
        # ----------------------------------------------------------------------
        # PATH HIJACK: Synchronizing Staging Buffers for LMDB Phase
        # ----------------------------------------------------------------------
        args.out_path = str(paths.results_path)
        args.data_path = str(paths.data_path)
        print(f"[INFO:Orchestration] Hijacked args.out_path: {args.out_path}")

    def patched_generate_lmdb(args, datasets=["train", "val", "test"]):
        import shutil
        lmdb_dir = Path(args.out_path) / "lmdb"
        # Fallback check (legacy or root results)
        fallback_dir = Path("/app/results/lmdb")
        
        # --- Surgical Fast-Forward Guard (V12.2.1) ---
        ff_source = os.getenv("FAST_FORWARD_SOURCE")
        found_lmdb = False
        if lmdb_dir.exists() and any(lmdb_dir.glob("*.lmdb")):
            found_lmdb = True
        elif fallback_dir.exists() and any(fallback_dir.glob("*.lmdb")):
            lmdb_dir = fallback_dir
            found_lmdb = True

        if ff_source and found_lmdb:
            print(f"[INFO:Security] Fast-Forward Active: Recycling LMDB from {lmdb_dir}")
            return
        
        if ff_source:
             print(f"[WARNING:Security] Fast-Forward requested but NO LMDB found at {lmdb_dir}")

        if lmdb_dir.exists():
            print(f"[INFO:Orchestration] Cleaning existing LMDB staging buffer: {lmdb_dir}")
            shutil.rmtree(lmdb_dir)
        lmdb_dir.mkdir(parents=True, exist_ok=True)
        # Call ORIGINAL vendor logic via frozen reference.
        return _original_generate_lmdb(args, datasets=datasets)

    # --- Step 2: Aggressive Global Injection ---
    # We must replace references in ALL loaded modules because some might have 
    # done 'from .preprocessing import ...' before we arrived.
    target_fns = {
        'compute_scalers': patched_compute_scalers,
        'generate_parquet': patched_generate_parquet,
        'generate_lmdb': patched_generate_lmdb,
        'generate_unittest_subsample': lambda *a, **k: None,
        'MinMaxAggregate': RobustMinMaxAggregate
    }

    for mod_name, mod in list(sys.modules.items()):
        if mod and (mod_name.startswith('bayesrul') or mod_name.startswith('rul_model_factory')):
            for fn_name, fn_obj in target_fns.items():
                if hasattr(mod, fn_name):
                    setattr(mod, fn_name, fn_obj)
    
    # Ensure the base module is definitely patched
    for fn_name, fn_obj in target_fns.items():
        setattr(bp, fn_name, fn_obj)

    # --- Metrics Device Patching ---
    import bayesrul.utils.metrics
    from torch.distributions import Normal
    def patched_get_proportion_lists(y_pred, y_std, y_true, num_bins, prop_type='interval'):
        target_dev = 'cpu' if is_cpu_mode else y_true.device
        exp_proportions = torch.linspace(0, 1, num_bins, device=target_dev)
        residuals = y_pred - y_true
        normalized_residuals = (residuals.flatten() / y_std.flatten()).reshape(-1, 1)
        dist = Normal(torch.tensor([0.0], device=target_dev), torch.tensor([1.0], device=target_dev))
        if prop_type == 'interval':
            gaussian_lower_bound = dist.icdf(0.5 - exp_proportions / 2.0)
            gaussian_upper_bound = dist.icdf(0.5 + exp_proportions / 2.0)
            above_lower = (normalized_residuals >= gaussian_lower_bound)
            below_upper = (normalized_residuals <= gaussian_upper_bound)
            observed_proportions = (above_lower & below_upper).float().mean(dim=0)
        elif prop_type == 'quantile':
            observed_proportions = (normalized_residuals <= dist.icdf(exp_proportions)).float().mean(dim=0)
        else: raise NotImplementedError
        return exp_proportions, observed_proportions

    bayesrul.utils.metrics.get_proportion_lists = patched_get_proportion_lists

    # --- EXECUTION ---
    print(f"[AUDIT:Orchestration] Initializing secure barrier for vendor module: {module_name}")
    full_module_path = f"bayesrul.{module_name}"
    
    # --- PHASE HANDOVER: Path Synchronization ---
    # The training module expects the LMDB database in the data_path.
    # We redirect it to the results_path for the duration of the execution.
    # We use a try-finally block to ensure convergence with the provenance 
    # generation phase (which requires the original .h5 data_path).
    original_data_path = paths.data_path
    
    try:
        if module_name == "ncmapss.train_model":
            print(f"[INFO:Orchestration] Phase Handover: Shifting data_path to {paths.results_path}")
            paths.data_path = paths.results_path

        with apply_runtime_patches(paths):
            runpy.run_module(full_module_path, run_name="__main__", alter_sys=True)
    finally:
        # Restore for post-execution compliance hashing (.h5 files)
        paths.data_path = original_data_path

