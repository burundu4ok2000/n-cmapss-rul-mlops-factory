# Stream Processing

### Apache Kafka Architecture

#### Topic Design

```bash
# Create topic with proper configuration
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic user-events \
    --partitions 24 \
    --replication-factor 3 \
    --config retention.ms=604800000 \        # 7 days
    --config retention.bytes=107374182400 \  # 100GB
    --config cleanup.policy=delete \
    --config min.insync.replicas=2 \         # Durability
    --config segment.bytes=1073741824        # 1GB segments
```

**Partition Count Guidelines:**

| Throughput | Partitions | Notes |
|------------|------------|-------|
| < 10K msg/s | 6-12 | Single consumer can handle |
| 10K-100K msg/s | 24-48 | Multiple consumers needed |
| > 100K msg/s | 100+ | Scale consumers with partitions |

**Partition Key Selection:**

```python
# Good partition keys: Even distribution, related data together
# For user events: user_id (events for same user on same partition)
# For orders: order_id (if no ordering needed) or customer_id (if needed)

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def send_event(event):
    # Use user_id as key for user-based partitioning
    producer.send(
        topic='user-events',
        key=event['user_id'],  # Partition key
        value=event
    )
```

### Spark Structured Streaming

#### Watermarks and Late Data

```python
from pyspark.sql.functions import window, col

# Read stream
events = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Add watermark for late data handling
# Data arriving more than 10 minutes late will be dropped
windowed_counts = events \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "5 minutes", "1 minute"),  # 5-min windows, 1-min slide
        "event_type"
    ) \
    .count()

# Write with append mode (only final results for complete windows)
query = windowed_counts.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/windowed_counts") \
    .start()
```

**Watermark Behavior:**

```
Timeline: ─────────────────────────────────────────▶
Events:   E1   E2   E3        E4(late)    E5
          │    │    │         │           │
Time:     10:00 10:02 10:05   10:03       10:15
          ▲                   ▲
          │                   │
          Current            Arrives at 10:15
          watermark          but event_time=10:03
          = max_event_time
            - threshold
          = 10:05 - 10min    If watermark > event_time:
          = 9:55               Event is dropped (too late)
```

#### Stateful Operations

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

# Session windows using flatMapGroupsWithState
def session_aggregation(key, events, state):
    """Aggregate events into sessions with 30-minute timeout."""

    # Get or initialize state
    if state.exists:
        session = state.get
    else:
        session = {"start": None, "events": [], "total": 0}

    # Process new events
    for event in events:
        if session["start"] is None:
            session["start"] = event.timestamp
        session["events"].append(event)
        session["total"] += event.value

    # Set timeout (session expires after 30 min of inactivity)
    state.setTimeoutDuration("30 minutes")

    # Check if session should close
    if state.hasTimedOut():
        # Emit completed session
        output = {
            "user_id": key,
            "session_start": session["start"],
            "event_count": len(session["events"]),
            "total_value": session["total"]
        }
        state.remove()
        yield output
    else:
        # Update state
        state.update(session)

# Apply stateful operation
sessions = events \
    .groupByKey(lambda e: e.user_id) \
    .flatMapGroupsWithState(
        session_aggregation,
        outputMode="append",
        stateTimeout=GroupStateTimeout.ProcessingTimeTimeout()
    )
```

---
