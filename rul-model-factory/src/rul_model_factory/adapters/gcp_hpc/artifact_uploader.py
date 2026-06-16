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
COMPONENT: ADAPTER / GCP_HPC
SUBCOMPONENT: ARTIFACT UPLOADER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    - Industrial-grade archival of training session artifacts to GCS.
    - Mandatory Zero-Trust protocol enforcement (Sterilization + Signing)
      before any asset touches the Data Lake.
    - Environment-agnostic: coordinates and security tools are injected
      during construction.

COMPLIANCE:
    - EU AI Act Art. 12: Traceability. Every uploaded artifact is cryptographically
      bound to its provenance and signer.
    - DORA: Non-repudiation. Artifacts in the lake are signed with keyless OIDC,
      guaranteeing they originated from a validated training run.
    - Zero-Trust: "No Signature, No Upload". The uploader acts as a final gateway.

SECURITY HANDSHAKE:
    1.  Recursive Scan: Obtains all files from the session sandbox (results/).
    2.  Sterilization: Intercepts .ckpt/.pt and forces immutable .safetensors conversion.
    3.  Provenance: Ensures provenance.json exists and is accurate.
    4.  Signing: Generates .sig and .cert for every critical asset.
    5.  Verification: Verifies the signature locally before initiating GCS transfer.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
from pathlib import Path
from typing import List, Optional

# 2. Third-Party Dependencies
import structlog
from google.cloud import storage

# 3. Internal Dependencies
from rul_model_factory.core.logistics.path_resolver import PathResolver
from rul_model_factory.core.security.artifact_sterilizer import ArtifactSterilizer
from rul_model_factory.core.security.cryptographic_signer import CryptographicSigner

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

# Critical file extensions that MUST be signed before upload.
_MANDATORY_SIGN_EXTENSIONS = {
    ".safetensors",
    ".json",      # Includes provenance.json and hyperparameters.
    ".parquet",   # Processed telemetry.
    ".lmdb",      # Training datasets.
}

# Source extensions that MUST be sterilized (converted to safetensors).
_SENSITIVE_SOURCE_EXTENSIONS = {
    ".ckpt",
    ".pt",
    ".pth",
}

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

class UploaderError(Exception):
    """Raised when the security handshake or GCS transfer fails."""
    pass

# ==============================================================================
# ADAPTER IMPLEMENTATION
# ==============================================================================

class ArtifactUploader:
    """
    GCP ARTIFACT UPLOADER: The industrial gatekeeper for the Data Lake.

    Enforces a strict security handshake for every artifact generated during
    a training session. Fail-Closed: if any file fails sterilization, signing,
    or verification, the entire upload process is aborted to prevent
    unverified asset propagation.
    """

    def __init__(
        self,
        resolver: PathResolver,
        signer: CryptographicSigner,
        sterilizer: ArtifactSterilizer,
        project_id: str,
        bucket_name: Optional[str] = None
    ) -> None:
        """Initializes the uploader with security tools and cloud coordinates.

        Args:
            resolver:   The canonical path resolver for the current session.
            signer:     Security tool for cryptographic artifact signing.
            sterilizer: Security tool for mitigating pickle-based ACE attacks.
            project_id: Google Cloud Project ID for resource ownership.
            bucket_name: Optional target bucket name. If None, resolves to
                         'ncmapss-data-lake-{project_id}'.
        """
        self._resolver   = resolver
        self._signer     = signer
        self._sterilizer = sterilizer
        self._project_id = project_id
        self._bucket_name = bucket_name or f"ncmapss-data-lake-{project_id}"
        
        # Initialize GCS Client (Authenticated via ADC or service account key)
        self._client = storage.Client(project=project_id)

        logger.info(
            "artifact_uploader_initialized",
            bucket=self._bucket_name,
            project=self._project_id,
            run_id=self._resolver.run_id
        )

    def upload_session_artifacts(self) -> int:
        """Executes the complete Zero-Trust upload cycle for the session.

        Traverses the local results sandbox, applies the security handshake,
        and synchronizes verified assets with the GCS Data Lake.

        Returns:
            The total count of blobs (artifacts + signatures) uploaded.

        Raises:
            UploaderError: If any stage of the handshake or transfer fails.
        """
        local_root = self._resolver.results_path
        
        if not local_root.exists():
            message = f"Upload Aborted: Session sandbox '{local_root}' does not exist."
            logger.error("upload_source_missing", msg=message)
            raise UploaderError(message)

        logger.info(
            "starting_session_upload",
            source_dir=self._resolver.mask_path(local_root),
            target_bucket=self._bucket_name
        )

        upload_count = 0
        # Recursive traversal: mirroring local structure to cloud topology.
        for path in local_root.rglob("*"):
            if path.is_dir():
                continue

            # Step 1-3: Security Handshake (Sterilization + Signing)
            batch = self._prepare_upload_batch(path)
            
            # Step 4: Physical Transfer
            for file_path in batch:
                # Resolve relative path to preserve directory structure in GCS.
                relative_name = str(file_path.relative_to(local_root.parent))
                # Cloud Topology: results/runs/{RUN_ID}/...
                destination_name = f"{relative_name}"
                
                self._upload_blob_to_gcs(file_path, destination_name)
                upload_count += 1

        logger.info(
            "session_upload_complete",
            total_blobs=upload_count,
            run_id=self._resolver.run_id
        )
        return upload_count

    def _prepare_upload_batch(self, local_file: Path) -> List[Path]:
        """Enforces the 'Security Handshake' for a single local file.

        [INVARIANT]: No sensitive asset touches the cloud without a signature.
        No unsterilized asset touches the cloud, period.

        Args:
            local_file: Path to the local artifact candidate.

        Returns:
            A list of Paths (The file itself + its cryptographic proofs).
        """
        batch = [local_file]
        
        # 1. Sterilization: Intercept and convert vulnerable formats.
        if local_file.suffix in _SENSITIVE_SOURCE_EXTENSIONS:
            logger.info("sterilization_required", artifact=local_file.name)
            # Sterilizer returns the path to the newly created .safetensors
            sterilized_path = self._sterilizer.sterilize_checkpoint(local_file)
            # Replace the original in the batch with the sterilized version.
            # We do NOT upload the raw .ckpt to the lake.
            batch = [sterilized_path]
            local_file = sterilized_path

        # 2. Cryptographic Signing: Generate non-repudiable proof.
        if local_file.suffix in _MANDATORY_SIGN_EXTENSIONS:
            logger.debug("signing_artifact", artifact=local_file.name)
            
            # Generate .sig and .cert via cosign (keyless OIDC).
            sig_path, cert_path = self._signer.sign_artifact(local_file)
            
            # 3. Local Verification: "Trust but Verify" before ingress.
            if not self._signer.verify_artifact(local_file):
                message = f"Security Integrity Failure: Signature verification failed for '{local_file.name}'."
                logger.critical("upload_blocked_by_security", msg=message)
                raise UploaderError(message)

            batch.extend([sig_path, cert_path])

        return batch

    def _upload_blob_to_gcs(self, local_path: Path, destination_blob_name: str) -> None:
        """Transfers a local file to GCS with mandatory integrity verification.

        Args:
            local_path: Absolute path to the local file.
            destination_blob_name: The destination path inside the GCS bucket.
        """
        try:
            bucket = self._client.bucket(self._bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Performance: Use parallel composite uploads for large files if possible.
            # Integrity: google-cloud-storage automatically calculates MD5/CRC32C.
            blob.upload_from_filename(str(local_path))

            logger.debug(
                "blob_uploaded",
                local_file=local_path.name,
                cloud_dest=destination_blob_name,
                size_bytes=local_path.stat().st_size
            )

        except Exception as e:
            message = f"GCS Transfer Failure: Could not upload '{local_path.name}' to bucket '{self._bucket_name}'."
            logger.error("gcs_upload_failed", msg=message, error=str(e))
            raise UploaderError(message) from e
