import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys

PARQUET_FILE = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")
REPORT_FILE = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/research/building_dbt/parquet_inspection_report.md")

def log_and_write(content, file):
    print(content)
    file.write(content + "\n")

def inspect_parquet():
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    with open(REPORT_FILE, "w") as f:
        log_and_write(f"# Parquet Inspection Report: {PARQUET_FILE.name}", f)
        log_and_write(f"Generated on: {pd.Timestamp.now()}", f)
        
        # 1. Metadata & Schema
        table = pq.read_table(PARQUET_FILE)
        log_and_write("\n## Schema Information", f)
        log_and_write("```text", f)
        log_and_write(str(table.schema), f)
        log_and_write("```", f)
        
        # 2. Detailed Column Analysis
        df = pd.read_parquet(PARQUET_FILE)
        log_and_write("\n## Data Statistics", f)
        log_and_write("### Null Counts", f)
        log_and_write("```text", f)
        log_and_write(df.isnull().sum().to_string(), f)
        log_and_write("```", f)
        
        log_and_write("\n### Column Types", f)
        log_and_write("```text", f)
        log_and_write(df.dtypes.to_string(), f)
        log_and_write("```", f)
        
        log_and_write("\n### Sample Data (Head 5)", f)
        log_and_write("```text", f)
        log_and_write(df.head().to_string(), f)
        log_and_write("```", f)
        
        log_and_write("\n### Numeric Summary", f)
        log_and_write("```text", f)
        log_and_write(df.describe().to_string(), f)
        log_and_write("```", f)
        
        # 3. Domain Specific Identifiers
        log_and_write("\n## Domain Identifiers", f)
        if 'unit' in df.columns:
            log_and_write(f"- **Units found**: {df['unit'].unique().tolist()}", f)
        if 'cycle' in df.columns:
            log_and_write(f"- **Max Cycle recorded**: {df['cycle'].max()}", f)
        if 'fc' in df.columns:
            log_and_write(f"- **Flight Classes (fc) found**: {df['fc'].unique().tolist()}", f)

if __name__ == "__main__":
    inspect_parquet()
    print(f"\n[SUCCESS] Report generated: {REPORT_FILE}")
