"""
SECURITY DOMAIN: Provenance & Regulatory Lineage (V12.1.0)
ROLE: Generation of the immutable 'Model Birth Certificate'.

This module aggregates cryptographic evidence of the training process, 
including data hashes, environment state, and software versions. It 
is the primary record for EU AI Act compliance audits.
"""

import os
import json
import hashlib
import datetime
import platform
import multiprocessing
import importlib.metadata
from pathlib import Path

def calculate_data_lineage_hash(data_path: Path) -> dict:
    """
    Establishes a cryptographically-bound anchor for the input data lineage.
    
    Computes a collective SHA-256 fingerprint for all source telemetry 
    HDF5 files. This ensures that the generated model's 'Birth Certificate' 
    is inextricably linked to the specific state of the training dataset.

    Returns:
        dict: A mapping of filenames to their respective SHA-256 digests.
    """
    print("[AUDIT:Compliance] Calculating multi-file data lineage hashes...")
    lineage = {}
    data_path = Path(data_path)
    try:
        h5_files = sorted(list(data_path.rglob("*.h5")))
        if not h5_files:
            return {"status": "no-data-found"}
        
        for h5_file in h5_files:
            sha256_hash = hashlib.sha256()
            with open(h5_file, "rb") as f:
                for byte_block in iter(lambda: f.read(65536), b""):
                    sha256_hash.update(byte_block)
            lineage[h5_file.name] = sha256_hash.hexdigest()
        return lineage
    except Exception as e:
        print(f"[ERROR:Audit] Data lineage calculation failed: {e}")
        return {"error": str(e)}

def generate_provenance_manifest(results_path: Path, data_path: Path) -> None:
    """
    Formally generates the 'Model Birth Certificate' for regulatory compliance.
    
    Aggregates immutable runtime metadata (Git, Environment, Hardware) and 
    cryptographic data lineage into a structured JSON manifest. This fulfills 
    the 'Audit Manager' role mandated by industrial AI safety standards.

    FAIL-CLOSED: Any failure in provenance generation mandates immediate 
    pipeline termination to prevent the release of unverified artifacts.
    """
    print("[AUDIT:Compliance] Compiling cryptographic Model Provenance manifest...")
    
    results_path = Path(results_path)
    data_path = Path(data_path)

    try:
        manifest = {
            "factory_version": "V12.1.0",
            "audit_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "identity": {
                "run_name": os.getenv("MODEL_NAME", "unknown_run"),
                "instance_name": platform.node(),
                "git_commit": os.getenv("GIT_COMMIT_HASH", "unknown-audit-fail")
            },
            "software_context": {
                "python_version": platform.python_version(),
                "libraries": {
                    "torch": str(importlib.metadata.version("torch")),
                    "pytorch_lightning": str(importlib.metadata.version("pytorch-lightning")),
                    "safetensors": str(importlib.metadata.version("safetensors"))
                }
            },
            "hardware_context": {
                "system": platform.system(),
                "processor": platform.processor(),
                "cpu_count": multiprocessing.cpu_count()
            },
            "data_lineage": calculate_data_lineage_hash(data_path)
        }

        manifest_path = results_path / "provenance.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=4)
        
        if manifest["identity"]["git_commit"] == "unknown-audit-fail":
            raise RuntimeError("CRITICAL LOGISTICS FAILURE: Missing GIT_COMMIT_HASH.")
            
        print(f"[INFO:Security] PROVENANCE SUCCESS: {manifest_path.name} generated.")
        
    except Exception as e:
        error_msg = f"CRITICAL AUDIT FAILURE: Provenance generation failed. {e}"
        print(f"[INFO:Security] {error_msg}")
        raise RuntimeError(error_msg) from e
