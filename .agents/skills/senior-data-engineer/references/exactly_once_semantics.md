# Exactly-Once Semantics

### Producer Idempotence

```python
from kafka import KafkaProducer

# Enable idempotent producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks='all',                    # Wait for all replicas
    enable_idempotence=True,       # Exactly-once per partition
    max_in_flight_requests_per_connection=5,  # Max with idempotence
    retries=2147483647,            # Infinite retries
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Producer will deduplicate based on sequence numbers
for i in range(100):
    producer.send('topic', {'id': i, 'data': 'value'})

producer.flush()
```

### Transactional Processing

```python
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Transactional producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    transactional_id='my-transactional-id',  # Enable transactions
    enable_idempotence=True,
    acks='all'
)

producer.init_transactions()

def process_with_transactions(consumer, producer):
    """Read-process-write with exactly-once semantics."""

    try:
        producer.begin_transaction()

        # Read
        records = consumer.poll(timeout_ms=1000)

        for tp, messages in records.items():
            for message in messages:
                # Process
                result = transform(message.value)

                # Write to output topic
                producer.send('output-topic', result)

        # Commit offsets and transaction atomically
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.group_id
        )
        producer.commit_transaction()

    except KafkaError as e:
        producer.abort_transaction()
        raise
```

### Spark Exactly-Once to External Systems

```python
# Use foreachBatch with idempotent writes
def write_to_database_idempotent(batch_df, batch_id):
    """Write batch with exactly-once semantics."""

    # Add batch_id for deduplication
    batch_with_id = batch_df.withColumn("batch_id", lit(batch_id))

    # Use MERGE for idempotent writes
    batch_with_id.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/db") \
        .option("dbtable", "staging_events") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # Merge staging to final (idempotent)
    execute_sql("""
        MERGE INTO events AS target
        USING staging_events AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Clean staging
    execute_sql("TRUNCATE staging_events")

query = events.writeStream \
    .foreachBatch(write_to_database_idempotent) \
    .option("checkpointLocation", "/checkpoints/to-postgres") \
    .start()
```

---
