import pandas as pd
from pathlib import Path

PARQUET_FILE = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/prepared-telemetry/test_DS02_prepared.parquet")
REPORT_FILE = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/research/building_dbt/cycle_density_report.md")

def log_and_write(content, file):
    print(content)
    file.write(content + "\n")

def analyze_cycles():
    df = pd.read_parquet(PARQUET_FILE)
    
    with open(REPORT_FILE, "w") as f:
        log_and_write(f"# Cycle Density Report: {PARQUET_FILE.name}", f)
        
        # Group by unit and cycle to see packet distribution
        stats = df.groupby(['unit', 'cycle']).size().reset_index(name='packet_count')
        
        log_and_write("\n## Packets per Cycle Summary", f)
        log_and_write("```text", f)
        log_and_write(stats.groupby('unit')['packet_count'].describe().to_string(), f)
        log_and_write("```", f)
        
        log_and_write("\n## Detailed Unit Trajectories", f)
        for unit in df['unit'].unique():
            unit_stats = stats[stats['unit'] == unit]
            log_and_write(f"### Unit {int(unit)}", f)
            log_and_write(f"- **Total Cycles**: {len(unit_stats)}", f)
            log_and_write(f"- **Avg Packets/Cycle**: {unit_stats['packet_count'].mean():.2f}", f)
            log_and_write(f"- **Min/Max Packets**: {unit_stats['packet_count'].min()} / {unit_stats['packet_count'].max()}", f)

if __name__ == "__main__":
    analyze_cycles()
    print(f"\n[SUCCESS] Cycle density report generated: {REPORT_FILE}")
