import duckdb
from pathlib import Path
import os

# Paths configuration to match the project structure
DB_PATH = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/persistence/telemetry_state.db")

def diagnose_db():
    print(f"--- [DIAGNOSTICS] Interrogating DuckDB: {DB_PATH.name} ---")
    
    if not DB_PATH.exists():
        print(f"[ERROR] Database file not found at {DB_PATH}")
        return

    # Check WAL and temporary files
    wal_path = DB_PATH.with_suffix(DB_PATH.suffix + ".wal")
    if wal_path.exists():
        print(f"[INFO] WAL file detected. Size: {wal_path.stat().st_size / 1024:.2f} KB")

    try:
        # Connect in READ_ONLY mode to mirror the dashboard's behavior
        con = duckdb.connect(str(DB_PATH), read_only=True)
        
        # 1. Check Tables
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"[INFO] Available tables: {tables}")
        
        if not any('fleet_telemetry' in t for t in tables):
            print("[CRITICAL] Table 'fleet_telemetry' missing!")
            return

        # 2. Check Global Stats
        count = con.execute("SELECT count(*) FROM fleet_telemetry").fetchone()[0]
        print(f"[SUCCESS] Total Rows in 'fleet_telemetry': {count}")
        
        # 3. Check Unit distribution
        units = con.execute("SELECT unit_id, count(*), max(cycle_id) FROM fleet_telemetry GROUP BY unit_id").fetchall()
        print("[AUDIT] Data distribution by Unit:")
        for uid, ucount, max_cycle in units:
            print(f"  - Unit {uid}: {ucount} rows, Max Cycle: {max_cycle}")
            
        # 4. Check for Unit 11 specifically
        u11_check = con.execute("SELECT * FROM fleet_telemetry WHERE unit_id = 11 LIMIT 1").fetchall()
        if not u11_check:
            print("[WARNING] No rows found for Unit 11 specifically.")
        else:
            print(f"[INFO] Unit 11 sample row: {u11_check[0]}")

        con.close()
    except Exception as e:
        print(f"[CRITICAL] Database interrogation failed: {e}")

if __name__ == "__main__":
    diagnose_db()
