import duckdb
from pathlib import Path
import pandas as pd

# Standard project paths
DB_PATH = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/persistence/telemetry_state.db")

import shutil

def check_inference_results():
    print("--- MISSION DATA AUDIT (Live Inference) ---")
    
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        return

    # [SHADOW MIRROR] Atomic copy to bypass live lock
    TEMP_AUDIT_DB = DB_PATH.with_suffix(".db.audit")
    try:
        shutil.copyfile(str(DB_PATH), str(TEMP_AUDIT_DB))
        # Connect to the clone
        con = duckdb.connect(str(TEMP_AUDIT_DB), read_only=True)
        
        # Check record count
        count = con.execute("SELECT COUNT(*) FROM fleet_telemetry").fetchone()[0]
        print(f"Total Records Ingested: {count}")
        
        if count > 0:
            print("\nRecent Bayesian Predictions (Last 5):")
            df = con.query("SELECT unit, cycle, true_rul, predicted_rul, predicted_std FROM fleet_telemetry ORDER BY cycle DESC LIMIT 5").to_df()
            print(df.to_string(index=False))
        else:
            print("WARNING: Table is empty. Awaiting ingestion...")
            
        con.close()
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    check_inference_results()
