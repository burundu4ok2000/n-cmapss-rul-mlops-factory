"""
LOGISTICS DOMAIN: Path Resolution & Environment Context (V12.1.0)
ROLE: Centralized map of the Industrial Engine Factory filesystem.

This module is the SOLE source of truth for filesystem locations. 
It ensures bit-perfect parity between local, containerized, and HPC 
execution environments.
"""

import os
from pathlib import Path
from types import SimpleNamespace
from dotenv import load_dotenv

# --- Core Anchors ---
# Current Depth: rul-model-factory/src/rul_model_factory/cloud_trainer/logistics/path_resolver.py
# parents[0]=logistics, parents[1]=cloud_trainer, parents[2]=rul_model_factory, 
# parents[3]=src, parents[4]=rul-model-factory, parents[5]=root
ROOT_DIR = Path(__file__).resolve().parents[5]

# --- Functional Nodes ---
# We prioritize /app paths if running inside the hardened HSE-HPC container
DOCKER_APP_ROOT = Path("/app")
IF_DOCKER = DOCKER_APP_ROOT.exists()

CURRENT_PROJECT_ROOT = DOCKER_APP_ROOT if IF_DOCKER else ROOT_DIR

VENDOR_ROOT = CURRENT_PROJECT_ROOT / "rul-model-factory" / "src" / "rul_model_factory" / "vendor"
ARTIFACTS_DIR = CURRENT_PROJECT_ROOT / "results" if IF_DOCKER else CURRENT_PROJECT_ROOT / "rul-model-factory" / "artifacts"
DATA_DIR = CURRENT_PROJECT_ROOT / "data" if IF_DOCKER else CURRENT_PROJECT_ROOT / ".workspace" / "raw-telemetry"
LOGS_DIR = ARTIFACTS_DIR / "local-logs"

# Ensure .env is loaded from the root for all execution contexts
load_dotenv(ROOT_DIR / ".env")

def resolve_paths() -> SimpleNamespace:
    """
    Resolves canonical locations for raw telemetry assets and execution artifacts.

    Returns:
        SimpleNamespace: Access-optimized container for system paths.
    """
    # Resolve isolation level: use RUN_ID if available, otherwise fallback to session-local
    run_id = os.getenv("RUN_ID", "local-session")
    run_artifacts_dir = ARTIFACTS_DIR / "runs" / run_id
    
    # Enforce directory existence for artifacts and logs
    run_artifacts_dir.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Internal session logging structure
    session_log_dir = run_artifacts_dir / "logs"
    session_log_dir.mkdir(parents=True, exist_ok=True)
    
    return SimpleNamespace(
        root_dir=ROOT_DIR,
        vendor_root=VENDOR_ROOT,
        data_path=DATA_DIR,
        results_path=run_artifacts_dir,
        logs_path=LOGS_DIR,
        # Helper aliases for legacy and coordination logic
        data_dir=DATA_DIR,
        results_dir=run_artifacts_dir,
        out_path=run_artifacts_dir,
        # Project Identification
        project_id=os.getenv("GCP_PROJECT_ID"),
        # Standardized log targets
        global_logs=LOGS_DIR / "factory_global.json.log",
        session_log=session_log_dir / "factory_session.audit.log"
    )
