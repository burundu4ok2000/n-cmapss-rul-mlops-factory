import pandas as pd
from pathlib import Path

PARQUET_PATH = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")

def full_audit():
    print(f"--- COMPLETE SCHEMA AUDIT: {PARQUET_PATH.name} ---")
    if not PARQUET_PATH.exists():
        print("ERROR: Parquet not found")
        return

    df = pd.read_parquet(PARQUET_PATH)
    print(f"\n[TOTAL COLUMNS: {len(df.columns)}]")
    print(sorted(df.columns.tolist()))
    
    print("\n[SAMPLE DATA - TOP 5]")
    # Showing first row values for identification
    print(df.iloc[0].to_dict())

if __name__ == "__main__":
    full_audit()
