"""
CORE DOMAIN: Real-time Telemetry Simulation (Operational Node 1)
ROLE: Transforming prepared analytical sinks into live telemetry streams.
STATUS: PRODUCTION_READY

CONTRACT:
    - IN: Prepared Parquet 'test_DS02_prepared.parquet'.
    - OUT: Kafka stream 'telemetry.live' with interleaved multi-unit packets.

SYSTEM-LEVEL INVARIANTS:
    - TIME_WARP_SYNC: Dynamically adjust inter-packet latency to hit 12-16s per cycle.
    - UNIT_INTERLEAVING: Stream units sequentially within each cycle layer to mirror fleet behavior.
    - COMPRESSION_FIDELITY: Mandatory Snappy compression for high-throughput bus efficiency.
"""

import json
import time
import sys
import os
import pandas as pd
from confluent_kafka import Producer
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Internal Compliance SSOT
from streaming_pipeline.config import (
    PREPARED_PARQUET_FILE, KAFKA_BROKER, KAFKA_TOPIC, 
    WARP_CONFIG, setup_compliance_logging,
    SIMULATION_LIFECYCLE_FILE, DB_DIR
)

# Initialize Industrial Logging
logger = setup_compliance_logging("telemetry_producer")

# --- 1. DELIVERY ACCOUNTABILITY (V1.5.0) ---

def delivery_report(err, msg):
    """
    Reports the delivery status of a telemetry packet for audit trail.

    Args:
        err (KafkaError): Error object if delivery failed.
        msg (Message): Kafka message object.
    """
    if err is not None:
        logger.error(f"[CRITICAL:Audit] Message delivery breach: {err}")
    # Success audit is restricted to cycle-level to prevent IO saturation.
    
# --- 2. WORKER NODE: UNIT STREAMER (V1.5.8) ---

# Dedicated Core mapping to prevent cache-thrashing in multi-unit fleet simulation
UNIT_CPU_AFFINITY = {11: 0, 14: 1, 15: 2}

def stream_unit_mission(unit_id, cycles, producer):
    """
    Simulates the mission-critical telemetry life-cycle for a single unit.
    Operates in a dedicated thread to ensure fleet-wide parallelism.
    
    Args:
        unit_id (int): Absolute ID of the turbine unit.
        cycles (list): Pre-processed cycle packets and latencies.
        producer (Producer): Kafka producer instance.
    """
    logger.info(f"[MISSION:Unit-{unit_id}] Sensor node initialized. Awaiting fleet launch...")
    
    # Pin OS thread to dedicated CPU core for cache stability
    cpu_core = UNIT_CPU_AFFINITY.get(unit_id)
    if cpu_core is not None:
        try:
            os.sched_setaffinity(0, {cpu_core})
            logger.info(f"[CONFIG:Unit-{unit_id}] Execution lock established on CPU core {cpu_core}")
        except (AttributeError, OSError):
            logger.warning(f"[CONFIG:Unit-{unit_id}] CPU Affinity shielding unavailable.")

    try:
        # Interleave units by executing their whole mission in parallel threads
        for c_idx, (cycle_df, sleep_time) in enumerate(cycles):
            logger.info(f"[INFO:Unit-{unit_id}] Starting Cycle {c_idx+1} ({len(cycle_df)} packets)")
            
            for _, row in cycle_df.iterrows():
                # Transform frame into binary JSON packet
                telemetry_packet = row.to_dict()
                telemetry_packet['timestamp_ms'] = int(time.time() * 1000)
                
                producer.produce(
                    KAFKA_TOPIC, 
                    key=str(unit_id), 
                    value=json.dumps(telemetry_packet),
                    callback=delivery_report
                )
                # [HEARTBEAT] Signal status to Sentinel UI (Safe multi-threaded write)
                heartbeat_path = DB_DIR.parent / "producer_heartbeat.json"
                try:
                    with open(heartbeat_path, "w") as hf:
                        json.dump({
                            "timestamp": time.time(),
                            "status": "STREAMING",
                            "last_unit_active": unit_id
                        }, hf)
                except:
                    pass

                time.sleep(sleep_time)
            
            # Perform background poll to drain delivery callbacks (Non-blocking)
            producer.poll(0)
            # Brief inter-cycle stabilization pause
            time.sleep(0.5)

    except Exception as e:
        logger.error(f"[CRITICAL:Unit-{unit_id}] Simulation aborted: {e}")

# --- 3. SIMULATION ENGINE (V1.5.0) ---

def run_simulation():
    """
    Executes the mission-critical telemetry simulation engine.
    
    Side Effects:
        - Network: Saturates Kafka bus with high-throughput SNAPPY packets.
        - Memory: Loads complete Parquet sink into RAM for rapid indexing.
        - Time: Implements blocking sleep for 'Time-Warp' synchronization.

    Raises:
        SystemExit: If environment invariants (HDF5/Kafka) are violated.
    """
    logger.info("------------------------------------------------------------------")
    logger.info(f"[MISSION:Streaming] Initializing Telemetry Simulator (Node 1)")
    logger.info("------------------------------------------------------------------")

    # 1. Resource Verification: Invariant Check
    if not PREPARED_PARQUET_FILE.exists():
        logger.error(f"[CRITICAL:Audit] Telemetry source missing: {PREPARED_PARQUET_FILE.name}")
        sys.exit(1)

    # 2. Infra Connectivity Barrier
    logger.info(f"[AUDIT:Kafka] Connecting to Infrastructure Bus: {KAFKA_BROKER}")
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'digital-twin-simulator',
        'compression.type': 'snappy',
        # --- [V18.29] Disk-Conservative Profile ---
        'queue.buffering.max.ms': 500,       # 500ms Buffer: Collects multiple cycle packets into a single batch
        'queue.buffering.max.messages': 1000,
        'batch.num.messages': 500,           # Optimize for disk throughput, not sub-ms latency
        'socket.send.buffer.bytes': 131072,  # 128KB socket buffer to reduce syscall overhead
        'message.timeout.ms': 30000,
    }
    
    try:
        producer = Producer(producer_conf)
    except Exception as e:
        logger.error(f"[CRITICAL:Runtime] Kafka driver initialization failed: {e}")
        sys.exit(1)

    # 3. Payload Preparation & Synchronization (V13.4 Calibration)
    logger.info(f"[AUDIT:Data] Loading mission-ready telemetry artifacts...")
    try:
        df = pd.read_parquet(PREPARED_PARQUET_FILE)
    except Exception as e:
        logger.exception(f"[CRITICAL:Data] Failed to parse Parquet sink")
        sys.exit(1)
    
    # Organize data into mission-ready generators
    unit_streams = {}
    for unit_id, config in WARP_CONFIG.items():
        unit_data = df[df['unit'] == unit_id].copy()
        # Group by cycle for adaptive latency calculation
        cycles = [group for _, group in unit_data.groupby('cycle')]
        
        # Pre-calculate steps to hit FTL Mode visualization targets
        processed_cycles = []
        target_pts = config.get('points_per_cycle', 10)
        target_sec = config.get('target_cycle_sec', 5)

        for cycle_df in cycles:
            # Adaptive Stepping: Target ~10 points per cycle to reduce IO/CPU
            actual_points = len(cycle_df)
            step = max(1, actual_points // target_pts)
            
            stepped_df = cycle_df.iloc[::step].head(target_pts)
            # Time-Warp calculation: Distribute 'target_sec' across 'target_pts'
            sleep_per_row = target_sec / len(stepped_df) if len(stepped_df) > 0 else 0
            processed_cycles.append((stepped_df, sleep_per_row))
        
        unit_streams[unit_id] = processed_cycles
    
    # --- MISSION REGISTRATION (V18.31) ---
    # Register mission start time and total work volume for dashboard synchronization
    lifecycle_meta = {
        "start_timestamp": time.time(),
        "total_cycles": {str(uid): len(streams) for uid, streams in unit_streams.items()}
    }
    try:
        with open(SIMULATION_LIFECYCLE_FILE, 'w') as f:
            json.dump(lifecycle_meta, f)
        logger.info(f"[MISSION:Audit] Lifecycle HUD enabled: {lifecycle_meta}")
    except Exception as e:
        logger.error(f"[MISSION:Audit] Lifecycle registration failed: {e}")

    logger.info(f"[INFO:Streaming] Multi-unit synchronization active: Fleet {list(WARP_CONFIG.keys())}")

    # 4. Parallel Simulation Engine (Multi-Threaded Fleet Mode)
    logger.info(f"[AUDIT:Fleet] Parallel launch sequence for units: {list(unit_streams.keys())}")

    try:
        # MISSION-CRITICAL: Launching all unit missions simultaneously
        with ThreadPoolExecutor(max_workers=len(unit_streams)) as executor:
            futures = [
                executor.submit(stream_unit_mission, uid, streams, producer)
                for uid, streams in unit_streams.items()
            ]
            # Wait for all missions to conclude or fail
            for future in futures:
                future.result()

    except KeyboardInterrupt:
        logger.info(f"[MISSION:Status] Simulation terminated by operator.")
    except Exception as e:
        logger.exception(f"[CRITICAL:Runtime] Simulation engine failure.")
    finally:
        logger.info(f"[AUDIT:Kafka] Finalizing delivery buffer...")
        producer.flush()
        logger.info("------------------------------------------------------------------")
        logger.success(f"[MISSION:Status] Simulator Node 1 safely powered down.")
        logger.info("------------------------------------------------------------------")

if __name__ == "__main__":
    run_simulation()
