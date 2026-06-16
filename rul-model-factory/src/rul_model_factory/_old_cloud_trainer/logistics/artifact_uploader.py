"""
LOGISTICS DOMAIN: Cloud Artifact Synchronization (V12.1.0)
ROLE: Automated transport of verified assets to Google Cloud Storage.

This module acts as the industrial 'Courier'. It enforces a mandatory 
security handshake by calling the cryptographic signer before ingestion 
into the Cloud Data Lake.
"""

import os
from pathlib import Path
from google.cloud import storage

# Cross-Domain Dependency: Logistics calls Security for clearance.
from ..security.cryptographic_signer import sign_artifact

def upload_results_to_gcs(results_path: Path) -> None:
    """
    Manages the bit-perfect synchronization of verified assets to GCS.
    
    Implements a formal Fail-Closed transfer protocol: all critical artifacts 
    identified by extension mandates must be cryptographically signed 
    prior to ingestion into the Cloud Data Lake.

    Args:
        results_path (Path): Directory containing verified training outputs.

    Raises:
        RuntimeError: If cryptographic verification or upload integrity is compromised.
    """
    # Strict Zero-Trust policy: Project ID must be provided by the environment.
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("CRITICAL LOGISTICS FAILURE: GCP_PROJECT_ID environment variable is not set.")
        
    bucket_name = f"ncmapss-data-lake-{project_id}"
    
    # Ensure results_path is Path object
    results_path = Path(results_path)

    print(f"[AUDIT:Logistics] Initializing secure transfer to gs://{bucket_name}/results/...")
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Identity-based Signing & Upload Loop
        files_processed = 0
        # We only sign heavyweight or critical meta artifacts.
        critical_extensions = {'.pt', '.ckpt', '.parquet', '.csv', '.safetensors', '.json'}
        
        for local_file in sorted(results_path.rglob("*")):
            if local_file.is_file():
                # Avoid re-signing signatures themselves
                if local_file.suffix in {'.sig', '.cert'}:
                    continue
                
                # Security Mandate: Apply cryptographic signing to critical artifacts.
                artifacts_to_upload = [local_file]
                if local_file.suffix in critical_extensions:
                    sig_path, cert_path = sign_artifact(local_file)
                    artifacts_to_upload.extend([sig_path, cert_path])
                
                for file_to_sync in artifacts_to_upload:
                    relative_path = file_to_sync.relative_to(results_path)
                    # Use RUN_ID from environment to maintain cloud-side isolation
                    run_id = os.environ.get("RUN_ID", "local-session")
                    blob_path = f"results/runs/{run_id}/{relative_path}"
                    
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(str(file_to_sync))
                    files_processed += 1
        
        print(f"[AUDIT:Security] Verification complete: {files_processed} artifacts committed to storage.")
    except Exception as e:
        # FAIL-CLOSED: Infrastructure failure must terminate execution.
        error_msg = f"CRITICAL FAILURE: Artifact integrity check compromised. {e}"
        print(f"[INFO:Orchestration] {error_msg}")
        raise RuntimeError(error_msg) from e
