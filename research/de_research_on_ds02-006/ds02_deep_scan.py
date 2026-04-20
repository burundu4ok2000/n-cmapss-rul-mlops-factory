import h5py
import numpy as np
import yaml
from pathlib import Path

def get_full_report(file_path):
    report = {
        "unit_profiles": {},
        "sensor_fidelity": {},
        "operational_envelopes": {},
        "cross_dataset_integrity": {}
    }
    
    with h5py.File(file_path, 'r') as hdf:
        # Load labels
        w_var = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf.get('W_var'))]
        x_s_var = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf.get('X_s_var'))]
        a_var = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf.get('A_var'))]
        
        # Load main data segments (dev for deeper stats)
        w_dev = np.array(hdf.get('W_dev'))
        x_s_dev = np.array(hdf.get('X_s_dev'))
        a_dev = np.array(hdf.get('A_dev'))
        y_dev = np.array(hdf.get('Y_dev'))
        
        units = np.unique(a_dev[:, 0])
        
        # 1. UNIT PROFILES
        for u in units:
            u_mask = (a_dev[:, 0] == u)
            u_data_a = a_dev[u_mask]
            u_cycles = np.unique(u_data_a[:, 1])
            
            # Health State (hs) flip point
            hs_col = u_data_a[:, 3]
            flip_point = np.where(hs_col == 1)[0]
            degradation_start_cycle = -1
            if len(flip_point) > 0:
                degradation_start_cycle = int(u_data_a[flip_point[0], 1])
                
            report["unit_profiles"][f"unit_{int(u)}"] = {
                "total_rows": int(len(u_data_a)),
                "total_cycles": int(len(u_cycles)),
                "min_cycle": int(np.min(u_cycles)),
                "max_cycle": int(np.max(u_cycles)),
                "degradation_start_cycle": degradation_start_cycle,
                "flight_classes": [int(f) for f in np.unique(u_data_a[:, 2])]
            }

        # 2. OPERATIONAL ENVELOPES (By Unit)
        for u in units:
            u_mask = (a_dev[:, 0] == u)
            u_data_w = w_dev[u_mask]
            
            u_env = {}
            for i, name in enumerate(w_var):
                col_data = u_data_w[:, i]
                u_env[name] = {
                    "min": float(np.min(col_data)),
                    "max": float(np.max(col_data)),
                    "mean": float(np.mean(col_data)),
                    "std": float(np.std(col_data))
                }
            report["operational_envelopes"][f"unit_{int(u)}"] = u_env

        # 3. SENSOR FIDELITY (Global Dev stats)
        for i, name in enumerate(x_s_var):
            col_data = x_s_dev[:, i]
            report["sensor_fidelity"][name] = {
                "min": float(np.min(col_data)),
                "max": float(np.max(col_data)),
                "mean": float(np.mean(col_data)),
                "std": float(np.std(col_data)),
                "nan_count": int(np.isnan(col_data).sum()),
                "zero_count": int((col_data == 0).sum())
            }

        # 4. CROSS-DATASET INTEGRITY
        report["cross_dataset_integrity"] = {
            "rows_mismatch_check": bool(len(w_dev) == len(x_s_dev) == len(a_dev) == len(y_dev)),
            "total_dev_rows": int(len(w_dev)),
            "y_max_rul": int(np.max(y_dev)),
            "y_min_rul": int(np.min(y_dev))
        }

    return report

def main():
    file_path = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/raw-telemetry/N-CMAPSS_DS02-006.h5")
    output_dir = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/research/de_research_on_ds02-006")
    
    print(f"Deep scanning {file_path.name}...")
    full_report = get_full_report(file_path)
    
    # Save partitioned reports for better readability
    with open(output_dir / "units_lifecycle.yml", "w") as f:
        yaml.dump(full_report["unit_profiles"], f, default_flow_style=False)
        
    with open(output_dir / "operational_envelopes.yml", "w") as f:
        yaml.dump(full_report["operational_envelopes"], f, default_flow_style=False)
        
    with open(output_dir / "sensor_stats.yml", "w") as f:
        yaml.dump(full_report["sensor_fidelity"], f, default_flow_style=False)
        
    with open(output_dir / "data_integrity.yml", "w") as f:
        yaml.dump(full_report["cross_dataset_integrity"], f, default_flow_style=False)

    print("Deep research complete. 4 YAML reports generated.")

if __name__ == "__main__":
    main()
