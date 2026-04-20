import pandas as pd
import duckdb
from pathlib import Path

DB_PATH = Path(".workspace/persistence/telemetry_state.db")
REF_PARQUET = Path("rul-model-factory/data/processed/test_DS02-006.parquet")

def compare_scales():
    # 1. Load Reference (Parquet)
    if not REF_PARQUET.exists():
        print(f"Error: Reference parquet {REF_PARQUET} not found.")
        return
    
    ref_df = pd.read_parquet(REF_PARQUET)
    # N-CMAPSS DS02 columns: alt, Mach, TRA, T2...
    # Gaussian columns are usually named X_s in some versions, but let's check keys
    print("\n--- [Reference: Parquet (DS02-006)] ---")
    ref_cols = ['alt', 'Mach', 'P15', 'P2', 'T2']
    print(ref_df[ref_cols].describe().loc[['mean', 'std']])

    # 2. Load Live (DuckDB)
    if not DB_PATH.exists():
        print(f"Error: Database {DB_PATH} not found.")
        return
    
    con = duckdb.connect(str(DB_PATH), read_only=True)
    live_df = con.execute("SELECT * FROM fleet_telemetry LIMIT 1000").df()
    con.close()
    
    print("\n--- [Live: DuckDB (Current Pipeline)] ---")
    # Comparison of raw physical values we calculated vs expectation
    print(live_df[ref_cols].describe().loc[['mean', 'std']])
    
    print("\n--- [Z-Score Comparison] ---")
    z_ref_cols = [f"{c}_z" for c in ref_cols]
    # Check if parquet has Z-scores (often in N-CMAPSS artifacts they are in dedicated columns)
    # If not, we'll see.
    
    print("\nLive DB Z-score means:")
    print(live_df[z_ref_cols].mean())

if __name__ == "__main__":
    compare_scales()
