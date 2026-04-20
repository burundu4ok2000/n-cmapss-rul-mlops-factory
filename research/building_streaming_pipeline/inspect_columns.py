import pandas as pd
from pathlib import Path

def main():
    # Inspecting one of the validation parquets in the model folder
    base_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc/data")
    file_path = base_path / "rul_bayesian_20260420T0650Z_cpu_hpc.val_DS02.parquet"
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    df = pd.read_parquet(file_path)
    print(f"File: {file_path.name}")
    print(f"Columns: {df.columns.tolist()[:30]}...") # Show first 30 columns
    print(f"Sample data:\n{df.head(2).to_string()}")

if __name__ == "__main__":
    main()
