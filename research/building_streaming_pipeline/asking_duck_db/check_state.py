import duckdb
import pandas as pd
from pathlib import Path
import shutil
import os

def main():
    db_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/persistence/telemetry_state.db")
    tmp_path = db_path.with_suffix(".db.research_view")
    
    if not db_path.exists():
        print(f"DATABASE NOT FOUND: {db_path}")
        return

    print(f"Executing V18.4 Shadow Mirror Audit for: {db_path.name}")
    
    try:
        # Atomic copy bypasses OS-level and DuckDB-level locks
        shutil.copyfile(str(db_path), str(tmp_path))
        
        con = duckdb.connect(str(tmp_path), read_only=True)
        
        # Check table existence
        check_table = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fleet_telemetry'").fetchone()
        
        if not check_table:
            print("MISSION ABORT: Table 'fleet_telemetry' missing in snapshot.")
            con.close()
            return
            
        print("--- INDUSTRIAL STATE AUDIT (SNAPSHOT) ---")
        query = """
            SELECT 
                count(*) as row_count,
                round(avg(predicted_rul), 2) as avg_rul,
                round(stddev(predicted_rul), 2) as std_rul,
                min(predicted_rul) as min_rul,
                max(predicted_rul) as max_rul
            FROM fleet_telemetry
        """
        results = con.execute(query).df()
        print(results.to_string(index=False))
        
        print("\n--- SAMPLE LATEST PREDICTIONS ---")
        latest = con.execute("SELECT unit, cycle, predicted_rul, true_rul FROM fleet_telemetry ORDER BY timestamp_ms DESC LIMIT 5").df()
        print(latest.to_string(index=False))
        
        con.close()
        # Cleanup
        if tmp_path.exists():
            os.remove(tmp_path)
            
    except Exception as e:
        print(f"CRITICAL AUDIT FAILURE: {e}")

if __name__ == "__main__":
    main()
