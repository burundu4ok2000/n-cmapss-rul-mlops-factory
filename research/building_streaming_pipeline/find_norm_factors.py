import pandas as pd
import numpy as np
import h5py
from pathlib import Path

def main():
    # 1. Paths
    raw_h5 = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/.workspace/raw-telemetry/N-CMAPSS_DS02-006.h5")
    norm_parquet = Path("/home/donald_trump/developer/n-cmapss-agentic-factory/rul-model-factory/artifacts/runs/rul_bayesian_20260420T0650Z_cpu_hpc/data/rul_bayesian_20260420T0650Z_cpu_hpc.val_DS02.parquet")
    
    if not raw_h5.exists() or not norm_parquet.exists():
        print("Missing required files for cross-audit.")
        return

    print("Loading datasets for mathematical alignment (Numpy Mode)...")
    
    # Load normalized data
    df_norm = pd.read_parquet(norm_parquet)
    # Target Unit 16
    target_unit = 16
    df_norm_u = df_norm[df_norm['unit'] == target_unit].head(1000)
    
    # Load raw data from H5
    with h5py.File(raw_h5, 'r') as hf:
        W = np.array(hf.get('W_test'))
        X_s = np.array(hf.get('X_s_test'))
        A = np.array(hf.get('A_test'))
        
        raw_columns = ['alt', 'Mach', 'TRA', 'T2', 'T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21', 'P24', 'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf']
        raw_data = np.concatenate([W, X_s], axis=1)
        df_raw = pd.DataFrame(raw_data, columns=raw_columns)
        
        df_raw['unit'] = A[:, 1]
        df_raw['cycle'] = A[:, 2]
        
    df_raw_u = df_raw[df_raw['unit'] == target_unit].head(1000)
    
    print(f"Solving normalization linear system for Unit {target_unit}...")
    
    features = ['alt', 'Mach', 'TRA', 'T2', 'T24', 'T30', 'T48', 'T50', 'P15', 'P2', 'P21', 'P24', 'Ps30', 'P40', 'P50', 'Nf', 'Nc', 'Wf']
    
    means = []
    stds = []
    
    for f in features:
        raw = df_raw_u[f].values
        norm = df_norm_u[f].values
        
        # We need two distinct points to solve the linear system
        # (Raw1 - M) / S = N1
        # (Raw2 - M) / S = N2 -> S = (Raw2 - Raw1)/(N2 - N1)
        
        # To be robust, use the delta between first and late sample
        idx1, idx2 = 0, -1
        if raw[idx2] == raw[idx1] or norm[idx2] == norm[idx1]:
            # Fallback to max/min in the subset
            idx1 = np.argmin(raw)
            idx2 = np.argmax(raw)
            
        if raw[idx2] == raw[idx1]: # Constant signal?
            std_calc = 1.0
            mean_calc = raw[idx1]
        else:
            std_calc = (raw[idx2] - raw[idx1]) / (norm[idx2] - norm[idx1])
            mean_calc = raw[idx1] - norm[idx1] * std_calc
            
        means.append(float(mean_calc))
        stds.append(float(std_calc))
        
    print("\n--- DERIVED INDUSTRIAL NORMALIZATION VECTORS (NUMPY CORE) ---")
    print(f"MEANS = {means}")
    print(f"STDS  = {stds}")
    print("-------------------------------------------------------------")
    
    # Validation check
    f0 = features[0]
    raw_val = df_raw_u[f0].iloc[10]
    norm_val = df_norm_u[f0].iloc[10]
    calculated = (raw_val - means[0]) / stds[0]
    print(f"Validation ({f0}): Raw {raw_val} -> Target {norm_val:.6f} | Calc {calculated:.6f}")

if __name__ == "__main__":
    main()
