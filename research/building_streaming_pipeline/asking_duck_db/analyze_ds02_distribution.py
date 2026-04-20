import pandas as pd
from pathlib import Path

# Target staged file
STAGED_FILE = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")

def analyze_ds02():
    print(f"--- ANALYZING DS02 DISTRIBUTIONS ---")
    if not STAGED_FILE.exists():
        print(f"ERROR: Staged file not found: {STAGED_FILE}")
        return

    df = pd.read_parquet(STAGED_FILE)
    
    # Identify sensors (exclude metadata)
    meta_cols = ['unit', 'cycle', 'Fc', 'Hs', 'true_rul']
    sensor_cols = [c for c in df.columns if c not in meta_cols]
    
    # Compute stats
    stats = df[sensor_cols].agg(['mean', 'std']).T
    
    print("\nCalculated DS02 Industrial Constants:")
    print("REPLACE_MAP = {")
    for sensor, row in stats.iterrows():
        print(f"    '{sensor}': {{'mean': {row['mean']:.4f}, 'std': {row['std']:.4f}}},")
    print("}")

if __name__ == "__main__":
    analyze_ds02()
