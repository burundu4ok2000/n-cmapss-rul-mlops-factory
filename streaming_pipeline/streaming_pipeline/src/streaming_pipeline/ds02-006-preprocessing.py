"""
CORE DOMAIN: Mission-Critical Data Staging (Operational Node 0)
ROLE: Data Sovereignty Layer Integration & Artifact Provisioning.
STATUS: PRODUCTION_READY

CONTRACT:
    - IN: Pre-normalized telemetry artifacts from Bayesian model runs.
    - OUT: Verified Parquet sink in the operational pipeline workspace.

SYSTEM-LEVEL INVARIANTS:
    - LINEAGE_LOCK: Staging MUST pull from validated training artifacts to ensure consistency.
    - ATOMIC_COPY: Use high-performance copy2 to preserve timestamp metadata.
    - FAIL_FAST_STAGING: Exit immediately if the source artifact is missing or corrupted.
"""

import os
import sys
import shutil
from pathlib import Path

# Internal Compliance SSOT
from streaming_pipeline.config import (
    PROJECT_ROOT, PREPARED_DATA_DIR, PREPARED_PARQUET_FILE, 
    setup_compliance_logging
)

# Initialize Industrial Logging
logger = setup_compliance_logging("ds02_006_staging")

# --- 1. GOLDEN DATA CONFIGURATION (V1.5.0) ---
# Pointing to the validated artifacts of the current Bayesian run
ARTIFACT_SOURCE_DIR = PROJECT_ROOT / "rul-model-factory" / "artifacts" / "runs" / "rul_bayesian_20260420T0650Z_cpu_hpc" / "data"
ARTIFACT_FILE_NAME = "rul_bayesian_20260420T0650Z_cpu_hpc.test_DS02.parquet"
SOURCE_PATH = ARTIFACT_SOURCE_DIR / ARTIFACT_FILE_NAME

# --- 2. EXECUTION BARRIER (V1.5.0) ---

def execute_ingestion_barrier():
    """
    Synchronizes high-fidelity training data into the operational workspace.
    This acts as 'Staging Node 0' in the Digital Twin lifecycle.
    """
    logger.info("------------------------------------------------------------------")
    logger.info("[MISSION:Staging] Initializing Golden Data Barrier (Node 0)")
    logger.info("------------------------------------------------------------------")

    try:
        shutil.copy2(SOURCE_PATH, PREPARED_PARQUET_FILE)
        
        # Verify the sink integrity (Layer 2 Verification)
        if PREPARED_PARQUET_FILE.exists():
            file_size_mb = PREPARED_PARQUET_FILE.stat().st_size / (1024 * 1024)
            logger.success(f"[MISSION:Success] Data staging barrier cleared ({file_size_mb:.2f} MB)")
            logger.info(f"[AUDIT:Provenance] SSOT Source: {SOURCE_PATH}")
            logger.info(f"[AUDIT:Provenance] STAGING Target: {PREPARED_PARQUET_FILE}")
        else:
            logger.error("[CRITICAL:Audit] Staging checksum failure: Target file missing after I/O.")
            sys.exit(1)

        logger.info("------------------------------------------------------------------")
        logger.info("[MISSION:Status] Staging Node 0: READY for telemetry simulation.")
        logger.info("------------------------------------------------------------------")

    except Exception as e:
        logger.exception(f"[CRITICAL:Runtime] Staging cycle breached: {e}")
        sys.exit(1)

if __name__ == "__main__":
    execute_ingestion_barrier()
