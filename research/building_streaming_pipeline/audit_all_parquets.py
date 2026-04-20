import pandas as pd
import numpy as np
from pathlib import Path

def audit_file(path):
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    # Checking altitude as the most indicative feature
    return {
        "file": path.name,
        "alt_mean": df['alt'].mean(),
        "alt_max": df['alt'].max(),
        "is_normalized": df['alt'].max() < 100 # Rough heuristic
    }

def main():
    base_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc/data")
    
    files = list(base_path.glob("*.parquet"))
    results = []
    for f in files:
        stats = audit_file(f)
        if stats:
            results.append(stats)
            
    # Also check the workspace prepared file
    ws_file = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")
    ws_stats = audit_file(ws_file)
    if ws_stats:
        ws_stats['file'] = "WORKSPACE_READY_PARQUET"
        results.append(ws_stats)

    report = pd.DataFrame(results)
    print("\n--- PARQUET NORMALIZATION AUDIT ---")
    print(report.to_string())
    print("-----------------------------------")

if __name__ == "__main__":
    main()
