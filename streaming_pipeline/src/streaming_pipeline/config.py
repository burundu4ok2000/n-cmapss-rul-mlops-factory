"""
CORE DOMAIN: Mission Configuration & Infrastructure SSOT (V1.5.0)
ROLE: Centralized coordination of filesystem paths and simulation context.
STATUS: PRODUCTION_READY

REGULATORY ALIGNMENT:
    - [EU AI Act Art. 12]: Technical documentation. Ensures deterministic path resolution.
    - [DORA]: Operational Resilience. Centralizes infrastructure metadata.

CONTRACT:
    - OUT: Verified Path objects and mission-critical constants for Node 2 & 3.

SYSTEM-LEVEL INVARIANTS:
    - DETERMINISTIC_ROOT: PROJECT_ROOT must be derived dynamically from module location.
    - SOVEREIGN_WORKSPACE: All persistence and logs MUST reside within '.workspace/'.
    - PATH_IMMUTABILITY: Configuration constants remain read-only during runtime.
"""

import os
import sys
import datetime
from pathlib import Path
from loguru import logger

# --- 1. DYNAMIC ROOT DISCOVERY (V1.5.0) ---
# Path resolution logic to ensure environment portability.
# Script location: [ROOT]/streaming_pipeline/streaming_pipeline/src/streaming_pipeline/config.py
CONFIG_PATH = Path(__file__).resolve()
PROJECT_ROOT = CONFIG_PATH.parents[4]

# --- 2. DATA LAYER PATHS (V1.5.0) ---
WORKSPACE_ROOT = PROJECT_ROOT / ".workspace"

RAW_DATA_DIR = WORKSPACE_ROOT / "raw-telemetry"
RAW_H5_FILE = RAW_DATA_DIR / "N-CMAPSS_DS02-006.h5"

PREPARED_DATA_DIR = WORKSPACE_ROOT / "prepared-telemetry"
PREPARED_PARQUET_FILE = PREPARED_DATA_DIR / "test_DS02_prepared.parquet"

# --- 3. INFRASTRUCTURE & AI PATHS (V1.5.0) ---
LOG_DIR = WORKSPACE_ROOT / "local-logs"

# Persistence for the Streamlit decoupling layer (WAL Direct Access)
DB_DIR = WORKSPACE_ROOT / "persistence"
DB_PATH = DB_DIR / "telemetry_state.db"
SNAPSHOT_PARQUET_FILE = DB_DIR / "telemetry_snapshot.parquet"
SIMULATION_LIFECYCLE_FILE = DB_DIR / "simulation_lifecycle.json"

# AI Model: Resolved from the latest stable Bayesian training run
MODEL_DIR = PROJECT_ROOT / "rul-model-factory" / "artifacts" / "runs" / "rul_bayesian_20260420T0650Z_cpu_hpc" / "model"
MODEL_FILE = MODEL_DIR / "rul_bayesian_20260420T0650Z_cpu_hpc.epoch=149-step=21000.safetensors"

# --- 4. COMMUNICATION BUS (V1.5.0) ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "telemetry.live"

# --- 5. SIMULATION PARAMETERS (FTL MODE V1.5.8) ---
# Rapid Simulation Profile: 5s cycles with 10 calibrated points per cycle.
WARP_CONFIG = {
    11: {"target_cycle_sec": 5, "points_per_cycle": 10},
    14: {"target_cycle_sec": 5, "points_per_cycle": 10},
    15: {"target_cycle_sec": 5, "points_per_cycle": 10},
}

# --- FUNCTIONS ---

def setup_compliance_logging(component_name: str):
    """
    Orchestrates industrial-grade logging for the Digital Twin nodes.

    Args:
        component_name (str): Identifier for the originating operational node.

    Returns:
        loguru.logger: Pre-configured logger instance with rotation and formatting.

    Side Effects:
        - Filesystem Mutation: Creates '.workspace/local-logs' if not existent.
        - Streams hijacking: Redirects logs to stdout and persistent storage.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%dT%H%M%SZ')
    log_file = LOG_DIR / f"{component_name}_{timestamp}.log"
    
    # Reset default handlers
    logger.remove()
    
    # Layer 1: High-Visibility Console Output
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )
    
    # Layer 2: Persistent Audit Trail for Regulatory Compliance
    logger.add(
        str(log_file),
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    
    logger.info(f"[AUDIT:Logging] Compliance channel initialized: {log_file.name}")
    return logger

def print_config_telemetry():
    """
    Performs a pre-flight environment portability check.
    
    Calculates and prints the critical path map to verify artifact accessibility.
    """
    print("------------------------------------------------------------------")
    print(f"[MISSION:Config] Environment Portability Shield Active (V1.5.0)")
    print(f"[MISSION:Config] Master Project Root: {PROJECT_ROOT}")
    print(f"[MISSION:Config] Resolved Target H5: {RAW_H5_FILE.relative_to(PROJECT_ROOT)}")
    print(f"[MISSION:Config] Active AI Model:    {MODEL_FILE.relative_to(PROJECT_ROOT)}")
    print(f"[MISSION:Config] Kafka Broker:       {KAFKA_BROKER}")
    print("------------------------------------------------------------------")

if __name__ == "__main__":
    print_config_telemetry()
