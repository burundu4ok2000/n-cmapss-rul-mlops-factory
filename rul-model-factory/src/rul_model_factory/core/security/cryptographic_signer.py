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
COMPONENT: CORE SECURITY
SUBCOMPONENT: CRYPTOGRAPHIC SIGNER

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - EU AI Act Art. 12: Record-keeping and traceability.
    - Zero-Trust Architecture: No artifact is trusted without valid proof.
    - DORA Resilience: Critical proof of integrity for ML supply chains.

SECURITY LAYERS:
    - Non-Repudiation: Cryptographic binding of artifacts to the training run.
    - Identity: Support for OIDC-based Workload Identity (Sigstore).
    - Auditability: Comprehensive logging of all signing and verification events.

GOAL:
    - Ensure non-repudiation of ML artifacts (weights, provenance).
    - Provide cryptographic proof of integrity via Sigstore/Cosign.
    - Abstract the complexities of OIDC and key-based signing protocols.

RESILIENCE:
    - Fail-Closed: Immediate abort of the pipeline on signing failure.
    - Dependency Hardening: Verification of Sigstore binary availability.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import os
import subprocess
from pathlib import Path
from typing import List, Dict

# 2. Third-Party Dependencies
import structlog

# 3. Internal Logistics Dependencies
from rul_model_factory.core.logistics.path_resolver import PathResolver

# ==============================================================================
# SYSTEM SETUP: LOGGING
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# EXCEPTIONS
# ==============================================================================

class CryptographicSignerError(Exception):
    """Raised when cryptographic signing or verification fails (Fail-Closed)."""
    pass

# ==============================================================================
# CRYPTOGRAPHIC SIGNER ENGINE
# ==============================================================================

class CryptographicSigner:
    """
    INDUSTRIAL CRYPTOGRAPHIC SIGNER: Interface for Sigstore/Cosign.
    
    Enforces industrial-grade safety invariants by ensuring that every 
    subprocess call is isolated, logged, and compliant with Zero-Trust protocols.
    """

    def __init__(self, resolver: PathResolver):
        """Initializes the signer with a PathResolver for secure secret discovery.
        
        Args:
            resolver: PathResolver instance used to locate keys and mask paths.
        """
        self.resolver = resolver

    # --- Signing Logic ---

    def sign_artifact(self, file_path: Path) -> List[Path]:
        """
        Generates cryptographic proof of artifact integrity.

        Uses OIDC-based Workload Identity by default (keyless signing).
        If a static key is required, it must be provided via PathResolver.

        Args:
            file_path: Path to the artifact pending signature.

        Returns:
            List[Path]: A pair of [signature_path, certificate_path].

        Raises:
            CryptographicSignerError: If signing fails (Fail-Closed).
        """
        if not file_path.exists():
            raise CryptographicSignerError(f"Artifact not found: {file_path}")

        sig_path = file_path.with_suffix(file_path.suffix + ".sig")
        cert_path = file_path.with_suffix(file_path.suffix + ".cert")

        # 1. Command Construction
        cmd = [
            "cosign", "sign-blob",
            "--output-signature", str(sig_path),
            "--output-certificate", str(cert_path),
            str(file_path)
        ]

        # 2. Key Discovery (Static vs Keyless)
        try:
            key_path = self.resolver.get_secret_path("cosign.key")
            cmd.extend(["--key", str(key_path)])
            logger.info("using_static_signing_key", key=self.resolver._mask_path(key_path))
        except Exception:
            # Fallback to Keyless/OIDC mode
            logger.info("using_oidc_keyless_signing")

        # 3. Environment Hardening
        env = os.environ.copy()
        env["COSIGN_YES"] = "true"
        env["COSIGN_EXPERIMENTAL"] = "1" # Required for --output-certificate

        logger.info(
            "signing_artifact_started",
            artifact=self.resolver.mask_path(file_path),
            sig_target=self.resolver.mask_path(sig_path)
        )

        # 4. Execution
        result = self._execute_cosign_command(cmd, env)

        if result.returncode != 0:
            error_msg = f"Cosign signing failed: {result.stderr}"
            logger.error("signing_failed", error=error_msg)
            raise CryptographicSignerError(error_msg)

        logger.info(
            "artifact_signed_successfully",
            artifact=self.resolver.mask_path(file_path),
            sig_path=self.resolver.mask_path(sig_path),
            cert_path=self.resolver.mask_path(cert_path)
        )

        return [sig_path, cert_path]

    # --- Verification Logic ---

    def verify_artifact(self, file_path: Path, sig_path: Path, cert_path: Path) -> bool:
        """
        Verifies the integrity and authenticity of an artifact signature.

        Args:
            file_path: Path to the artifact to verify.
            sig_path:  Path to the .sig file.
            cert_path: Path to the .cert file.

        Returns:
            bool: True if verification succeeds.
        """
        cmd = [
            "cosign", "verify-blob",
            "--signature", str(sig_path),
            "--certificate", str(cert_path),
            str(file_path)
        ]

        # In keyless mode, we need to provide the identity of the signer
        # [TODO]: Move these to a specialized 'SecurityPolicy' object in v0.2.0
        env = os.environ.copy()
        env["COSIGN_EXPERIMENTAL"] = "1"

        logger.debug(
            "verifying_artifact_integrity",
            artifact=self.resolver.mask_path(file_path)
        )

        result = self._execute_cosign_command(cmd, env)

        if result.returncode == 0:
            logger.info("artifact_verification_passed", artifact=self.resolver.mask_path(file_path))
            return True
        
        error_msg = f"Artifact integrity verification failed: {result.stderr}"
        logger.warning("artifact_verification_failed", artifact=self.resolver.mask_path(file_path), error=error_msg)
        return False

    # --- Subprocess Layer ---

    def _execute_cosign_command(self, args: List[str], env: Dict[str, str]) -> subprocess.CompletedProcess:
        """Low-level subprocess wrapper with comprehensive audit logging."""
        try:
            return subprocess.run(
                args,
                env=env,
                capture_output=True,
                text=True,
                check=False
            )
        except FileNotFoundError:
            raise CryptographicSignerError(
                "Critical Dependency Failure: 'cosign' binary not found in PATH."
            )
        except Exception as e:
            raise CryptographicSignerError(f"Subprocess execution error: {str(e)}")
