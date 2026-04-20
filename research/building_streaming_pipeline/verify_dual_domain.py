import duckdb
from pathlib import Path
import os

DB_PATH = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/persistence/telemetry_state.db")

def verify_schema():
    print(f"--- POST-UPGRADE SCHEMA VERIFICATION: {DB_PATH.name} ---")
    if not DB_PATH.exists():
        print("INFO: Database file doesn't exist yet")
        return

    # [SHADOW:Mirror] Bypass OS-level lock
    TMP_PATH = DB_PATH.with_suffix(".db.verify")
    import shutil
    shutil.copyfile(str(DB_PATH), str(TMP_PATH))
    
    con = duckdb.connect(str(TMP_PATH), read_only=True)
    columns = con.execute("DESCRIBE fleet_telemetry").df()
    
    print("\n[DETECTED COLUMNS]")
    print(columns['column_name'].tolist())
    
    # Check for _z columns
    z_cols = [c for c in columns['column_name'] if c.endswith('_z')]
    print(f"\n[Z-SCORE COLUMNS FOUND: {len(z_cols)}/18]")
    
    if len(z_cols) == 18:
        print("SUCCESS: Dual-Domain schema is fully operational.")
    else:
        print("WARNING: Schema mismatch detected.")
        
    con.close()

if __name__ == "__main__":
    verify_schema()
