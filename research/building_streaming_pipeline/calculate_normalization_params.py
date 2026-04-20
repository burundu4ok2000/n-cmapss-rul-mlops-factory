import pandas as pd
import numpy as np
from pathlib import Path

def main():
    # Target: DS02 Validation set as the SSOT for normalization parameters
    artifact_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc/data/rul_bayesian_20260420T0650Z_cpu_hpc.val_DS02.parquet")
    
    if not artifact_path.exists():
        print(f"ERROR: Artifact not found at {artifact_path}")
        return

    print(f"Auditing artifact: {artifact_path.name}")
    df = pd.read_parquet(artifact_path)
    
    # Feature ordering is critical for BigCeption architecture (18 features total)
    # W: Operating settings (4)
    # X_s: Core sensors (14)
    features = [
        'alt', 'Mach', 'TRA', 'T2', 
        'T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21', 'P24', 'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf'
    ]
    
    means = df[features].mean().values.tolist()
    stds = df[features].std().values.tolist()
    
    print("\n--- NORMALIZATION VECTORS (Z-SCORE) ---")
    print(f"MEANS = {means}")
    print(f"STDS  = {stds}")
    print("----------------------------------------")
    
    # Sanity check for Unit 11 altitude normalization
    sample_alt = 24996.0  # From telemetry logs
    norm_alt = (sample_alt - means[0]) / stds[0]
    print(f"Sanity Check: Raw Alt {sample_alt} -> Normalized {norm_alt:.4f}")

if __name__ == "__main__":
    main()
