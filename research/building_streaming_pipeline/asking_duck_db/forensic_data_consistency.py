import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(".workspace/persistence/telemetry_state.db")

def analyze_jitter(unit_id=14):
    if not DB_PATH.exists():
        print("DB not found.")
        return

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        # Check for multiple entries per cycle or non-monotonic RUL
        df = con.execute(f"""
            SELECT cycle, true_rul, predicted_rul, timestamp_ms 
            FROM fleet_telemetry 
            WHERE unit = {unit_id} 
            ORDER BY timestamp_ms ASC
        """).df()
        
        print(f"\n--- [Forensic: Unit {unit_id} Chronology] ---")
        print(df.head(40))
        
        # Detect jitter
        df['rul_diff'] = df['true_rul'].diff()
        jitter = df[df['rul_diff'] > 0]
        if not jitter.empty:
            print("\n!!! JITTER DETECTED: True RUL increased between packets !!!")
            print(jitter[['cycle', 'true_rul', 'rul_diff']])
        else:
            print("\nNominal: True RUL is monotonic.")

    finally:
        con.close()

if __name__ == "__main__":
    analyze_jitter(14)
    analyze_jitter(11)
