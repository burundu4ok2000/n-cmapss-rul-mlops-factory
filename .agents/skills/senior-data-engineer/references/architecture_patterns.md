# Architecture Patterns

### Lambda Architecture

The Lambda architecture combines batch and stream processing for comprehensive data handling.

```
                    ┌─────────────────────────────────────┐
                    │           Data Sources              │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │         Message Queue (Kafka)        │
                    └───────┬─────────────────┬───────────┘
                            │                 │
              ┌─────────────▼─────┐   ┌───────▼─────────────┐
              │    Batch Layer    │   │    Speed Layer      │
              │   (Spark/Airflow) │   │   (Flink/Spark SS)  │
              └─────────────┬─────┘   └───────┬─────────────┘
                            │                 │
              ┌─────────────▼─────┐   ┌───────▼─────────────┐
              │   Master Dataset  │   │   Real-time Views   │
              │   (Data Lake)     │   │   (Redis/Druid)     │
              └─────────────┬─────┘   └───────┬─────────────┘
                            │                 │
                    ┌───────▼─────────────────▼───────┐
                    │          Serving Layer           │
                    │   (Merged Batch + Real-time)     │
                    └─────────────────────────────────┘
```

**Components:**

1. **Batch Layer**
   - Processes complete historical data
   - Creates precomputed batch views
   - Handles complex transformations, ML training
   - Reprocessable from raw data

2. **Speed Layer**
   - Processes real-time data stream
   - Creates real-time views for recent data
   - Low latency, simpler transformations
   - Compensates for batch layer delay

3. **Serving Layer**
   - Merges batch and real-time views
   - Responds to queries
   - Provides unified interface

**Implementation Example:**

```python
# Batch layer: Daily aggregation with Spark
def batch_daily_aggregation(spark, date):
    """Process full day of data for batch views."""
    raw_df = spark.read.parquet(f"s3://data-lake/raw/events/date={date}")

    aggregated = raw_df.groupBy("user_id", "event_type") \
        .agg(
            count("*").alias("event_count"),
            sum("revenue").alias("total_revenue"),
            max("timestamp").alias("last_event")
        )

    aggregated.write \
        .mode("overwrite") \
        .partitionBy("event_type") \
        .parquet(f"s3://data-lake/batch-views/daily_agg/date={date}")

# Speed layer: Real-time aggregation with Spark Structured Streaming
def speed_realtime_aggregation(spark):
    """Process streaming data for real-time views."""
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events") \
        .load()

    parsed = stream_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    aggregated = parsed \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window("timestamp", "1 minute"),
            "user_id",
            "event_type"
        ) \
        .agg(count("*").alias("event_count"))

    query = aggregated.writeStream \
        .format("redis") \
        .option("host", "redis") \
        .outputMode("update") \
        .start()

    return query
```

### Kappa Architecture

Kappa simplifies Lambda by using only stream processing with replay capability.

```
                    ┌─────────────────────────────────────┐
                    │           Data Sources              │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │    Immutable Log (Kafka/Kinesis)     │
                    │         (Long retention)             │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │        Stream Processor              │
                    │      (Flink/Spark Streaming)         │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │         Serving Layer                │
                    │    (Database/Data Warehouse)         │
                    └─────────────────────────────────────┘
```

**Key Principles:**

1. **Single Processing Path**: All data processed as streams
2. **Immutable Log**: Kafka/Kinesis as source of truth with long retention
3. **Reprocessing via Replay**: Re-run stream processor from beginning when needed

**Reprocessing Strategy:**

```python
# Reprocessing in Kappa architecture
class KappaReprocessor:
    """Handle reprocessing by replaying from Kafka."""

    def __init__(self, kafka_config, flink_job):
        self.kafka = kafka_config
        self.job = flink_job

    def reprocess(self, from_timestamp: str):
        """Reprocess all data from a specific timestamp."""

        # 1. Start new consumer group reading from timestamp
        new_consumer_group = f"reprocess-{uuid.uuid4()}"

        # 2. Configure stream processor with new group
        self.job.set_config({
            "group.id": new_consumer_group,
            "auto.offset.reset": "none"  # We'll set offset manually
        })

        # 3. Seek to timestamp
        offsets = self._get_offsets_for_timestamp(from_timestamp)
        self.job.seek_to_offsets(offsets)

        # 4. Write to new output table/topic
        output_table = f"events_reprocessed_{datetime.now().strftime('%Y%m%d')}"
        self.job.set_output(output_table)

        # 5. Run until caught up
        self.job.run_until_caught_up()

        # 6. Swap output tables atomically
        self._atomic_table_swap("events", output_table)

    def _get_offsets_for_timestamp(self, timestamp):
        """Get Kafka offsets for a specific timestamp."""
        consumer = KafkaConsumer(bootstrap_servers=self.kafka["brokers"])
        partitions = consumer.partitions_for_topic("events")

        offsets = {}
        for partition in partitions:
            tp = TopicPartition("events", partition)
            offset = consumer.offsets_for_times({tp: timestamp})
            offsets[tp] = offset[tp].offset

        return offsets
```

### Medallion Architecture (Bronze/Silver/Gold)

Common in data lakehouses (Databricks, Delta Lake).

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│  (Raw Data) │     │ (Cleansed)  │     │ (Analytics) │
└─────────────┘     └─────────────┘     └─────────────┘
     │                    │                    │
     ▼                    ▼                    ▼
 Landing zone        Validated,          Aggregated,
 Append-only         deduplicated,       business-ready
 Schema evolution    standardized        Star schema
```

**Implementation with Delta Lake:**

```python
# Bronze: Raw ingestion
def ingest_to_bronze(spark, source_path, bronze_path):
    """Ingest raw data to bronze layer."""
    df = spark.read.format("json").load(source_path)

    # Add metadata
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", input_file_name())

    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_path)

# Silver: Cleansing and validation
def bronze_to_silver(spark, bronze_path, silver_path):
    """Transform bronze to silver with cleansing."""
    bronze_df = spark.read.format("delta").load(bronze_path)

    # Read last processed version
    last_version = get_last_processed_version(silver_path, "bronze")

    # Get only new records
    new_records = bronze_df.filter(col("_commit_version") > last_version)

    # Cleanse and validate
    silver_df = new_records \
        .filter(col("user_id").isNotNull()) \
        .filter(col("event_type").isin(["click", "view", "purchase"])) \
        .withColumn("event_date", to_date("timestamp")) \
        .dropDuplicates(["event_id"])

    # Merge to silver (upsert)
    silver_table = DeltaTable.forPath(spark, silver_path)

    silver_table.alias("target") \
        .merge(
            silver_df.alias("source"),
            "target.event_id = source.event_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

# Gold: Business aggregations
def silver_to_gold(spark, silver_path, gold_path):
    """Create business-ready aggregations in gold layer."""
    silver_df = spark.read.format("delta").load(silver_path)

    # Daily user metrics
    daily_metrics = silver_df \
        .groupBy("user_id", "event_date") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("sessions"),
            sum(when(col("event_type") == "purchase", col("revenue")).otherwise(0)).alias("revenue"),
            max("timestamp").alias("last_activity")
        )

    # Write as gold table
    daily_metrics.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .save(gold_path + "/daily_user_metrics")
```

---
