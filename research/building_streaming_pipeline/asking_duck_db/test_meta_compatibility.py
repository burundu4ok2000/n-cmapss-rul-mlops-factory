import duckdb
import os

# Context: Checking meta-data compatibility for Node 3 (Sentinel UI)
def test_meta():
    print("--- DUCKDB META COMPATIBILITY AUDIT ---")
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE fleet_telemetry (id INT, cycle INT)")
    
    print("Testing 'sqlite_master' alias...")
    try:
        res = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fleet_telemetry'").fetchone()
        print(f"Result (sqlite_master): {res}")
    except Exception as e:
        print(f"Error (sqlite_master): {e}")

    print("\nTesting 'information_schema.tables'...")
    try:
        res = con.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'fleet_telemetry'").fetchone()
        print(f"Result (information_schema): {res}")
    except Exception as e:
        print(f"Error (information_schema): {e}")

if __name__ == "__main__":
    test_meta()
