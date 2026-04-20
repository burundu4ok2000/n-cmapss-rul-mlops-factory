import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(".workspace/persistence/telemetry_state.db")

def audit_db_sanity():
    if not DB_PATH.exists():
        print(f"Error: Database {DB_PATH} not found.")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.execute("SELECT * FROM fleet_telemetry ORDER BY timestamp_ms DESC LIMIT 100").df()
        if df.empty:
            print("Database is currently empty (waiting for ingestion).")
            return

        print("\n--- [AUDIT: Physical Space Sanity] ---")
        phys_cols = ['alt', 'Mach', 'T2', 'T24', 'T30', 'T48', 'T50']
        print(df[phys_cols].describe().loc[['min', 'max', 'mean']])

        print("\n--- [AUDIT: Gaussian Manifold Sanity] ---")
        z_cols = ['alt_z', 'Mach_z', 'T2_z', 'T24_z', 'T30_z', 'T48_z', 'T50_z']
        print(df[z_cols].describe().loc[['min', 'max', 'mean']])
        
        # Check for the "Millions" bug
        if (df['alt'] > 1000000).any():
            print("\n!!! WARNING: Numerical Overflow detected in Physical Space !!!")
        else:
            print("\nSUCCESS: All physical values within engineering envelopes.")

    except Exception as e:
        print(f"Audit failure: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    audit_db_sanity()
