import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(".workspace/persistence/telemetry_state.db")

def emergency_audit():
    print(f"--- [DEEP AUDIT: Node 2 Sink] ---")
    if not DB_PATH.exists():
        print(f"ERROR: Database file not found at {DB_PATH}")
        return

    try:
        # Using read_only=True to avoid lock contention with the active consumer
        con = duckdb.connect(str(DB_PATH), read_only=True)
        
        # 1. Row count check
        count = con.execute("SELECT count(*) FROM fleet_telemetry").fetchone()[0]
        print(f"Total Rows in 'fleet_telemetry': {count}")
        
        if count > 0:
            # 2. Grain analysis
            print("\nTop 5 records (Chrono):")
            df = con.execute("SELECT unit, cycle, true_rul, predicted_rul, timestamp_ms FROM fleet_telemetry ORDER BY timestamp_ms DESC LIMIT 5").df()
            print(df)
            
            # 3. Unit-Cycle distribution
            print("\nDistribution by Unit/Cycle:")
            dist = con.execute("SELECT unit, cycle, count(*) as samples FROM fleet_telemetry GROUP BY unit, cycle ORDER BY unit, cycle").df()
            print(dist)
        else:
            print("\nWARNING: Table exists but is completely EMPTY.")
            
        con.close()
    except Exception as e:
        print(f"CRITICAL ERROR during audit: {e}")

if __name__ == "__main__":
    emergency_audit()
