"""
NASA N-CMAPSS Dataset Ingestion and Provisioning Utility (V12.1.1).

This utility implements an Atomic-Swap pattern for the acquisition and preparation 
of the N-CMAPSS dataset. 

Architectural Alignment:
- Root Storage: .workspace/
- Sub-Directories: raw-telemetry, dataset-resources, local-logs (Siblings).
- Atomic Deployment: Uses temporary staging to ensure directory immutability.
"""

import os
import shutil
import zipfile
import datetime
import argparse
import h5py
from pathlib import Path
import httpx
from loguru import logger
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, DownloadColumn, TransferSpeedColumn

# ==============================================================================
# CONFIGURATION & PATH RESOLUTION
# ==============================================================================
DATASET_URL = "https://phm-datasets.s3.amazonaws.com/NASA/17.+Turbofan+Engine+Degradation+Simulation+Data+Set+2.zip"

# Resolve project root: parents[4] from src/infrastructure_setup/data_logistics/
PROJECT_ROOT = Path(__file__).resolve().parents[4]
WORKSPACE_ROOT = PROJECT_ROOT / ".workspace"

# Immutable Target Structure (Siblings within .workspace)
TELEMETRY_DIR = WORKSPACE_ROOT / "raw-telemetry"
RESOURCES_DIR = WORKSPACE_ROOT / "dataset-resources"
LOG_DIR = WORKSPACE_ROOT / "local-logs"

# Atomic Staging Zone (Hidden inside .workspace)
STAGE_DIR = WORKSPACE_ROOT / ".tmp_ingestion_buffer"

# Logging configuration
LOG_DIR.mkdir(parents=True, exist_ok=True)
TIMESTAMP = datetime.datetime.now(datetime.UTC).strftime('%Y%m%dT%H%M%SZ')
LOG_FILE = LOG_DIR / f"dataset_ingestion_{TIMESTAMP}.log"

logger.remove()
logger.add(str(LOG_FILE), level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

def download_file(url: str, dest_path: Path, progress: Progress):
    """Downloads a dataset archive using streaming."""
    logger.info(f"Downloading dataset from: {url}")
    with httpx.stream("GET", url, follow_redirects=True, timeout=None) as response:
        if response.status_code != 200:
            raise RuntimeError(f"Download failed with status {response.status_code}")
        
        total = int(response.headers.get("Content-Length", 0))
        task_id = progress.add_task("[cyan]Acquiring NASA Dataset...", total=total)
        
        with open(dest_path, "wb") as f:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
                progress.update(task_id, advance=len(chunk))
    logger.info(f"Acquisition successful: {dest_path}")

def extract_and_sort(zip_path: Path, staging_root: Path, progress: Progress):
    """Extracts archives and sorts artifacts into a temporary staging structure."""
    temp_extract = staging_root / "raw_extraction"
    temp_extract.mkdir()
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(temp_extract)
    
    # Handle nested zips
    zips = list(temp_extract.rglob("*.zip"))
    if zips:
        task_id = progress.add_task("[yellow]Unpacking sub-archives...", total=len(zips))
        for z in zips:
            with zipfile.ZipFile(z, 'r') as sub_zip:
                sub_zip.extractall(z.parent)
            z.unlink()
            progress.update(task_id, advance=1)

    # Initialize staging hierarchy
    (staging_root / "telemetry").mkdir()
    (staging_root / "resources").mkdir()

    # Artifact Categorization
    all_files = list(temp_extract.rglob("*"))
    for f in all_files:
        if f.is_dir(): continue
        
        if f.suffix.lower() == ".h5":
            if h5py.is_hdf5(f):
                shutil.move(str(f), str(staging_root / "telemetry" / f.name))
            else:
                logger.error(f"CORRUPTION DETECTED: {f.name} is not a valid HDF5 file. Purging.")
                f.unlink()
        elif f.suffix.lower() in [".pdf", ".ipynb"]:
            shutil.move(str(f), str(staging_root / "resources" / f.name))
    
    shutil.rmtree(temp_extract)
    logger.info("Artifact categorization complete.")

def main():
    parser = argparse.ArgumentParser(description="NASA N-CMAPSS Atomic Ingestion Utility")
    parser.add_argument("--local", type=str, help="Path to a local ZIP file for offline ingestion")
    args = parser.parse_args()

    logger.info("Initializing Atomic Ingestion Cycle")
    
    # Ensure workspace exists
    WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)
    
    if STAGE_DIR.exists():
        shutil.rmtree(STAGE_DIR)
    STAGE_DIR.mkdir(parents=True)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            DownloadColumn(),
            TransferSpeedColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            transient=True
        ) as progress:
            
            source_zip = None
            if args.local:
                source_zip = Path(args.local).resolve()
                logger.info(f"Local ingestion mode: {source_zip}")
            else:
                download_target = STAGE_DIR / "acquisition.zip"
                download_file(DATASET_URL, download_target, progress)
                source_zip = download_target

            extract_and_sort(source_zip, STAGE_DIR, progress)

            # ATOMIC DEPLOYMENT
            logger.info("Performing final atomic deployment...")
            
            # 1. Telemetry Deploy
            TELEMETRY_DIR.mkdir(exist_ok=True)
            for h5 in (STAGE_DIR / "telemetry").glob("*.h5"):
                shutil.move(str(h5), str(TELEMETRY_DIR / h5.name))
            
            # 2. Resources Deploy
            RESOURCES_DIR.mkdir(exist_ok=True)
            for res in (STAGE_DIR / "resources").iterdir():
                shutil.move(str(res), str(RESOURCES_DIR / res.name))

            logger.success(f"Ingestion complete. Workspace synchronized at: {WORKSPACE_ROOT}")

    except Exception as e:
        logger.exception("Critical failure during atomic ingestion")
        print(f"\n[bold red]Ingestion Failure:[/bold red] {e}")
    finally:
        if STAGE_DIR.exists():
            shutil.rmtree(STAGE_DIR)

if __name__ == "__main__":
    main()
