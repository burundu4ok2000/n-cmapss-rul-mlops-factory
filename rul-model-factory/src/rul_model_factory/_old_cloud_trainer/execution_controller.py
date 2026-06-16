"""
CORE DOMAIN: Execution Orchestrator & Compliance Controller (V12.1.0)
ROLE: The Conductor. High-level pipeline management.

This is the central entry point of the Cloud Trainer. It implements a 
clean Facade pattern, delegating specialized tasks to domain-specific 
sub-modules (Security, Logistics, Core).
"""

import sys
import os
from pathlib import Path

# --- GLOBAL ARCHITECTURAL BARRIER (V12.0.6 Pattern) ---
# We MUST inject vendor paths before any domain imports to ensure that 
# libraries like torch and pytorch_lightning are loaded within the 
# validated 2022 research environment.
BASE_DIR = Path(__file__).resolve().parent
# VENDOR_ROOT is at rul_model_factory/vendor/, while we are in cloud_trainer/
VENDOR_ROOT = BASE_DIR.parent / "vendor"

for p in [VENDOR_ROOT, VENDOR_ROOT / "bayesrul", VENDOR_ROOT / "tyxe"]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# --- Domain Imports (Safe behind the barrier) ---
try:
    from .logistics.path_resolver import resolve_paths
    from .core.vendor_patch_engine import execute_vendor_module
    from .security.artifact_sterilizer import secure_checkpoints_conversion
    from .security.provenance_generator import generate_provenance_manifest
    from .logistics.artifact_uploader import upload_results_to_gcs
except ImportError as e:
    print(f"[CRITICAL:Orchestration] Environment integrity breach: {e}")
    print(f"[DEBUG:Forensics] sys.path contains: {sys.path}")
    # Attempt to log torch status if available
    try:
        import torch
        print(f"[DEBUG:Forensics] Torch version: {torch.__version__}")
        print(f"[DEBUG:Forensics] CUDA available: {torch.cuda.is_available()}")
    except Exception:
        print("[DEBUG:Forensics] Torch not importable.")
    raise

def wrap_ml_module(module_name: str):
    """
    Executes an industrial AI training cycle within a secured orchestration barrier.
    """
    # 1. Logistics: Synchronize the Factory Map
    paths = resolve_paths()

    try:
        # 2. Core Execution: Patch & Run the researcher's code
        execute_vendor_module(module_name, paths)
        
        # 3. Security & Compliance (Post-Success Handshake)
        # We ensure artifacts are sterile and lineage is captured.
        secure_checkpoints_conversion(paths.results_path)
        generate_provenance_manifest(paths.results_path, paths.data_path)
        
        # 4. Logistics: Commitment to the Data Lake
        upload_results_to_gcs(paths.results_path)
        
        print(f"[AUDIT:Success] Pipeline completed for module: {module_name}")

    except Exception as e:
        # FAIL-CLOSED: Infrastructure failure must terminate execution.
        # We attempt to preserve any forensic telemetry before exit.
        if isinstance(e, SystemExit):
            # Normal termination for CLI tools, still need to finalize
            exit_code = e.code if hasattr(e, 'code') else 0
            if exit_code == 0:
                secure_checkpoints_conversion(paths.results_path)
                generate_provenance_manifest(paths.results_path, paths.data_path)
                upload_results_to_gcs(paths.results_path)
                return

        import traceback
        print(f"[AUDIT:Failure] Pipeline crashed: {e}")
        traceback.print_exc()
        try:
            secure_checkpoints_conversion(paths.results_path)
            generate_provenance_manifest(paths.results_path, paths.data_path)
            upload_results_to_gcs(paths.results_path)
        except Exception as forensic_error:
            print(f"[AUDIT:Critical] Forensic capture failed: {forensic_error}")
        
        # Re-raise for the system shell to catch the error
        raise e

if __name__ == "__main__":
    # Industrial Standard: Command Line Interface for the Orchestrator
    if len(sys.argv) > 1:
        target_module = sys.argv[1]
        # Remove the orchestrator-specific argument to keep sys.argv clean for patches
        sys.argv.pop(1)
        wrap_ml_module(target_module)
    else:
        print("Usage: python execution_controller.py <submodule_path> [args]")
