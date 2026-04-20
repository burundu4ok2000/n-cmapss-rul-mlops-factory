import duckdb
import pandas as pd
from pathlib import Path
import time

DB_PATH = Path(".workspace/persistence/telemetry_state.db")

def verify_idempotency():
    print(f"--- [Audit: V18.22 Engineering Standard] ---")
    if not DB_PATH.exists():
        print("[FAIL] Database artifact missing. Run the orchestrator first.")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # 1. Check for UNIQUE constraint in schema
        schema = con.execute("SELECT sql FROM sqlite_master WHERE name='fleet_telemetry'").fetchone()[0]
        if "UNIQUE" in schema or "idx_unit_cycle" in schema:
            print("[PASS] Schema Integrity: UNIQUE constraint active.")
        else:
            print("[FAIL] Schema Integrity: UNIQUE constraint MISSING.")

        # 2. Test UPSERT (INSERT OR REPLACE)
        unit, cycle = 999, 1
        val1 = (int(time.time()*1000), unit, cycle, 100.0, 1.0, 100.0)
        # Pad with zeros for sensor columns
        full_val1 = val1 + (0.0,) * (18 + 18)
        
        con.execute("INSERT OR REPLACE INTO fleet_telemetry VALUES (" + ",".join(['?']*42) + ")", full_val1)
        
        # Second insert with same (unit, cycle) but different prediction
        val2 = (int(time.time()*1000), unit, cycle, 50.0, 1.0, 100.0)
        full_val2 = val2 + (0.0,) * (18 + 18)
        con.execute("INSERT OR REPLACE INTO fleet_telemetry VALUES (" + ",".join(['?']*42) + ")", full_val2)
        
        count = con.execute(f"SELECT count(*) FROM fleet_telemetry WHERE unit={unit} AND cycle={cycle}").fetchone()[0]
        pred = con.execute(f"SELECT predicted_rul FROM fleet_telemetry WHERE unit={unit} AND cycle={cycle}").fetchone()[0]
        
        if count == 1:
            print("[PASS] Idempotency: Duplicate cycle suppressed.")
            if pred == 50.0:
                print("[PASS] Consistency: UPSERT correctly updated the existing row.")
        else:
            print(f"[FAIL] Idempotency: Multiple rows found for same cycle ({count}).")

    finally:
        con.close()

if __name__ == "__main__":
    verify_idempotency()
