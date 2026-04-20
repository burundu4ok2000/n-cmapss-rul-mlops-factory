import pandas as pd
from pathlib import Path
import sys

# Add src to path to import config
sys.path.append(str(Path(__file__).parents[2] / "streaming_pipeline" / "streaming_pipeline" / "src"))

try:
    from streaming_pipeline.config import PREPARED_PARQUET_FILE
    print(f"[CHECK] Target file: {PREPARED_PARQUET_FILE}")

    if not PREPARED_PARQUET_FILE.exists():
        print(f"[ERROR] File not found!")
        sys.exit(1)

    df = pd.read_parquet(PREPARED_PARQUET_FILE)
    print(f"[INFO] Dataset shape: {df.shape}")
    print(f"[INFO] Columns: {df.columns.tolist()}")

    for unit_id in [11, 14, 15]:
        unit_data = df[df['unit'] == unit_id]
        if unit_data.empty:
            print(f"[WARN] No data for Unit {unit_id}")
            continue
        
        print(f"\n--- Audit Unit {unit_id} ---")
        # Check first 5 and last 5 cycles
        cycles = sorted(unit_data['cycle'].unique())
        print(f"Cycle range: {min(cycles)} to {max(cycles)}")
        
        sample = unit_data[['cycle', 'true_rul']].drop_duplicates().sort_values('cycle')
        print("Sample True RUL values per cycle:")
        print(sample.head(5))
        print("...")
        print(sample.tail(5))
        
        is_constant = sample['true_rul'].nunique() == 1
        if is_constant:
            print(f"[!!!] ALERT: True RUL for Unit {unit_id} IS CONSTANT at {sample['true_rul'].iloc[0]}")
        else:
            print(f"[OK] True RUL for Unit {unit_id} is dynamic.")

except Exception as e:
    print(f"[CRITICAL] Research failed: {e}")
    import traceback
    traceback.print_exc()
