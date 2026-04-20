import pandas as pd
from pathlib import Path

# Path as staged by the orchestrator
PARQUET_PATH = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")

def peak_data():
    print(f"--- PARQUET FORENSICS: {PARQUET_PATH.name} ---")
    if not PARQUET_PATH.exists():
        print(f"ERROR: File missing at {PARQUET_PATH}")
        return

    df = pd.read_parquet(PARQUET_PATH)
    print("\n[FIRST 5 ROWS]")
    # Showing key sensors to identify scale
    print(df[['unit', 'cycle', 'alt', 'Mach', 'TRA', 'T2', 'Nc']].head())
    
    print("\n[COLUMN STATS]")
    print(df[['alt', 'Mach', 'Nc']].describe().loc[['mean', 'std', 'min', 'max']])

if __name__ == "__main__":
    peak_data()
