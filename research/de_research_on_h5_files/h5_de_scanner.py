import h5py
import numpy as np
import os
import yaml
from pathlib import Path

def scan_h5_file(file_path):
    report = {
        "file_name": file_path.name,
        "file_size_gb": round(file_path.stat().st_size / (1024**3), 3),
        "structure": {},
        "metadata": {},
        "de_insights": {
            "sensors": {},
            "flight_conditions": {},
            "fleet_stats": {},
            "hidden_data": {}
        }
    }
    
    with h5py.File(file_path, 'r') as hdf:
        report["metadata"]["root_attributes"] = dict(hdf.attrs)
        for key in hdf.keys():
            ds = hdf[key]
            report["structure"][key] = {
                "shape": list(ds.shape),
                "dtype": str(ds.dtype),
                "compression": ds.compression if hasattr(ds, 'compression') else None
            }
            
        var_maps = {
            "W_var": "operative_conditions",
            "X_s_var": "physical_sensors",
            "X_v_var": "virtual_sensors",
            "T_var": "health_parameters",
            "A_var": "auxiliary_data"
        }
        
        for v_key, v_label in var_maps.items():
            if v_key in hdf:
                names = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf[v_key])]
                report["de_insights"]["sensors"][v_label] = names

        if 'A_dev' in hdf and 'A_test' in hdf:
            a_dev = np.array(hdf['A_dev'])
            a_test = np.array(hdf['A_test'])
            units_dev = np.unique(a_dev[:, 0])
            units_test = np.unique(a_test[:, 0])
            report["de_insights"]["fleet_stats"] = {
                "total_units": int(len(units_dev) + len(units_test)),
                "units_dev": [int(u) for u in units_dev],
                "units_test": [int(u) for u in units_test],
                "total_rows": int(len(a_dev) + len(a_test))
            }

        if 'W_dev' in hdf and 'W_var' in hdf:
            w_names = [n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf['W_var'])]
            w_data = np.array(hdf['W_dev'])
            for idx, name in enumerate(w_names):
                col_data = w_data[:, idx]
                report["de_insights"]["flight_conditions"][name] = {
                    "min": float(np.min(col_data)),
                    "max": float(np.max(col_data)),
                    "mean": float(np.mean(col_data))
                }

        all_var_names = []
        for key in ["W_var", "X_s_var", "X_v_var", "T_var", "A_var"]:
            if key in hdf:
                all_var_names.extend([n.decode('utf-8') if isinstance(n, bytes) else str(n) for n in np.array(hdf[key])])
        
        search_terms = ["gps", "long", "lat", "apt", "airport", "loc", "iata", "icao", "time", "date", "year"]
        found_meta = [v for v in all_var_names if any(term in v.lower() for term in search_terms)]
        report["de_insights"]["hidden_data"]["potential_metadata_cols"] = found_meta

    return report

def main():
    data_dir = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/raw-telemetry")
    output_dir = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/research/de_research_on_h5_files")
    
    h5_files = sorted(list(data_dir.glob("*.h5")))
    summary_report = {}
    
    for f in h5_files:
        print(f"Scanning {f.name}...")
        try:
            file_report = scan_h5_file(f)
            summary_report[f.name] = file_report
            with open(output_dir / f"report_{f.stem}.yml", "w") as out:
                yaml.dump(file_report, out, default_flow_style=False, allow_unicode=True)
        except Exception as e:
            print(f"Error scanning {f.name}: {e}")

    with open(output_dir / "global_synthesis.yml", "w") as out:
        yaml.dump(summary_report, out, default_flow_style=False, allow_unicode=True)
    
    print("Investigation complete.")

if __name__ == "__main__":
    main()
