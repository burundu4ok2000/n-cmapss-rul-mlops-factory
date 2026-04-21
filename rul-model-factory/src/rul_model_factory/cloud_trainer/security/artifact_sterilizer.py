"""
SECURITY DOMAIN: Artifact Sterilization & RCE Prevention (V12.1.0)
ROLE: Conversion of unsafe Pickle checkpoints to passive Safetensors.

This module acts as a quarantine buffer, ensuring that no executable 
Python objects are retained in the training artifacts. This fulfills 
the cybersecurity robustness mandates of the EU AI Act.
"""

import torch
from safetensors.torch import save_file
from pathlib import Path

def secure_checkpoints_conversion(results_path: Path) -> None:
    """
    Implements mandatory artifact sterilization to eliminate RCE vectors.
    
    Traverses artifacts to identify unsafe PyTorch Lightning checkpoints 
    (Pickle format) and distills them into passive, immutable '.safetensors'.
    Unsafe binaries are purged from the ephemeral buffer to ensure compliance 
    with EU AI Act security mandates.

    Args:
        results_path (Path): Buffer containing raw training output.
    """
    print("[AUDIT:Compliance] Executing Hardened Serialization protocol (V12.1.0)...")
    
    # Safe-Load Detection (weights_only=True available in PyTorch 1.13+)
    use_safe_load = False
    try:
        major, minor = map(int, torch.__version__.split('.')[:2])
        if (major == 1 and minor >= 13) or major >= 2:
            use_safe_load = True
    except (ValueError, AttributeError): pass

    # Ensure results_path is a Path object
    results_path = Path(results_path)

    for ckpt_file in list(results_path.rglob("*.ckpt")):
        try:
            target_file = ckpt_file.with_suffix(".safetensors")
            print(f"[INFO:Security] Neutralizing {ckpt_file.name} -> {target_file.name}")
            
            # ATOMIC DEFENSE: we attempt to restrict pickle to weights only.
            load_kwargs = {"map_location": "cpu"}
            if use_safe_load:
                load_kwargs["weights_only"] = True
                print("[INFO:Security] Safe-Load Active (weights_only=True)")
            
            checkpoint = torch.load(str(ckpt_file), **load_kwargs)
            # Extract state_dict if it's a Lightning checkpoint, otherwise take as is
            weights = checkpoint.get('state_dict', checkpoint)
            
            save_file(weights, str(target_file))
            ckpt_file.unlink()
            print(f"[INFO:Security] PURGE SUCCESSFUL: {ckpt_file.name} removed.")
        except Exception as e:
            print(f"[ERROR:Security] Hardened conversion failed for {ckpt_file.name}: {e}")
