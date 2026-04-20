import h5py
import numpy as np
import yaml
from pathlib import Path

def get_test_report(file_path):
    report = {"test_unit_profiles": {}}
    
    with h5py.File(file_path, 'r') as hdf:
        a_test = np.array(hdf.get('A_test'))
        y_test = np.array(hdf.get('Y_test'))
        
        units = np.unique(a_test[:, 0])
        
        for u in units:
            u_mask = (a_test[:, 0] == u)
            u_data_y = y_test[u_mask]
            u_data_a = a_test[u_mask]
            u_cycles = np.unique(u_data_a[:, 1])
            
            report["test_unit_profiles"][f"unit_{int(u)}"] = {
                "total_rows": int(len(u_data_y)),
                "total_cycles": int(len(u_cycles)),
                "min_rul_found": int(np.min(u_data_y)),
                "max_rul_found": int(np.max(u_data_y))
            }
    return report

def main():
    file_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/raw-telemetry/N-CMAPSS_DS02-006.h5")
    output_dir = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/research/de_research_on_ds02-006")
    
    report = get_test_report(file_path)
    with open(output_dir / "test_units_lifecycle.yml", "w") as f:
        yaml.dump(report["test_unit_profiles"], f, default_flow_style=False)
    
    print("Test set probe complete.")

if __name__ == "__main__":
    main()
