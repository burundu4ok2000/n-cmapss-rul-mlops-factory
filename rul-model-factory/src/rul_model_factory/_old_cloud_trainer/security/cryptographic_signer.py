"""
SECURITY DOMAIN: Cryptographic Signer & Integrity Proof (V12.1.0)
ROLE: Non-repudiable artifact signing via Sigstore/Cosign.

This module is functionally isolated from ML logic. It provides deterministic 
probative value of artifact integrity for DORA and EU AI Act compliance.
"""

import os
import subprocess
from pathlib import Path
from typing import Tuple

def sign_artifact(filepath: Path) -> Tuple[Path, Path]:
    """
    Generates non-repudiable cryptographic proof of artifact integrity.
    
    Leverages OIDC-based Workload Identity to generate ephemeral signatures 
    via Sigstore/Cosign. This establishes a verifiable link between the 
    training worker identity and the produced binary weights.

    Args:
        filepath (Path): Local artifact pending verification.

    Returns:
        Tuple[Path, Path]: References to signed metadata (.sig and .cert).

    Raises:
        RuntimeError: If identity verification or signing fails (Fail-Closed).
    """
    sig_file = filepath.with_suffix(filepath.suffix + ".sig")
    cert_file = filepath.with_suffix(filepath.suffix + ".cert")
    
    print(f"[AUDIT:Security] Generating cryptographic signature for {filepath.name}...")
    
    try:
        # Native Identity Integration: Leverage Sigstore's built-in 
        # Google Workload Identity provider.
        env = os.environ.copy()
        env["COSIGN_YES"] = "true"
        env["COSIGN_EXPERIMENTAL"] = "1"  # Required for --output-certificate
        
        # We explicitly avoid handling OIDC tokens in Python to minimize 
        # the attack surface. cosign handles the metadata handshake natively.
        subprocess.run([
            "cosign", "sign-blob",
            "--output-signature", str(sig_file),
            "--output-certificate", str(cert_file),
            str(filepath)
        ], check=True, capture_output=True, text=True, env=env)
        
        return sig_file, cert_file
    except subprocess.CalledProcessError as e:
        error_msg = f"CRITICAL SECURITY FAILURE: Signing failed for {filepath.name}. Output: {e.stderr}"
        print(f"[INFO:Security] {error_msg}")
        raise RuntimeError(error_msg) from e
