# Batch Processing

### Apache Spark Best Practices

#### Memory Management

```python
# Optimal Spark configuration for batch jobs
spark = SparkSession.builder \
    .appName("BatchETL") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

**Memory Tuning Guidelines:**

| Data Size | Executors | Memory/Executor | Cores/Executor |
|-----------|-----------|-----------------|----------------|
| < 10 GB   | 2-4       | 4-8 GB          | 2-4            |
| 10-100 GB | 10-20     | 8-16 GB         | 4-8            |
| 100+ GB   | 50+       | 16-32 GB        | 4-8            |

#### Partition Optimization

```python
# Repartition vs Coalesce
# Repartition: Full shuffle, use for increasing partitions
df_repartitioned = df.repartition(100, "date")  # Partition by column

# Coalesce: No shuffle, use for decreasing partitions
df_coalesced = df.coalesce(10)  # Reduce partitions without shuffle

# Optimal partition size: 128-256 MB each
# Calculate partitions:
# num_partitions = total_data_size_mb / 200

# Check current partitions
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Repartition for optimal join performance
large_df = large_df.repartition(200, "join_key")
small_df = small_df.repartition(200, "join_key")
result = large_df.join(small_df, "join_key")
```

#### Join Optimization

```python
# Broadcast join for small tables (< 10MB by default)
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
result = large_df.join(broadcast(small_df), "key")

# Increase broadcast threshold if needed
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")

# Sort-merge join for large tables
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

# Bucket tables for frequent joins
df.write \
    .bucketBy(100, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("bucketed_orders")
```

#### Caching Strategy

```python
# Cache when:
# 1. DataFrame is used multiple times
# 2. After expensive transformations
# 3. Before iterative operations

# Use MEMORY_AND_DISK for large datasets
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)

# Cache only necessary columns
df.select("id", "value").cache()

# Unpersist when done
df.unpersist()

# Check storage
spark.catalog.clearCache()  # Clear all caches
```

### Airflow DAG Patterns

#### Idempotent Tasks

```python
# Always design idempotent tasks
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

@dag(
    schedule_interval="@daily",
    start_date=days_ago(7),
    catchup=True,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)
def idempotent_etl():

    @task
    def extract(execution_date=None):
        """Idempotent extraction - same date always returns same data."""
        date_str = execution_date.strftime("%Y-%m-%d")

        # Query for specific date only
        query = f"""
            SELECT * FROM source_table
            WHERE DATE(created_at) = '{date_str}'
        """
        return query_database(query)

    @task
    def transform(data):
        """Pure function - no side effects."""
        return [transform_record(r) for r in data]

    @task
    def load(data, execution_date=None):
        """Idempotent load - delete before insert or use MERGE."""
        date_str = execution_date.strftime("%Y-%m-%d")

        # Option 1: Delete and reinsert
        execute_sql(f"DELETE FROM target WHERE date = '{date_str}'")
        insert_data(data)

        # Option 2: Use MERGE/UPSERT
        # MERGE INTO target USING source ON target.id = source.id
        # WHEN MATCHED THEN UPDATE
        # WHEN NOT MATCHED THEN INSERT

    raw = extract()
    transformed = transform(raw)
    load(transformed)

dag = idempotent_etl()
```

#### Backfill Pattern

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

def process_date(ds, **kwargs):
    """Process a single date - supports backfill."""
    logical_date = datetime.strptime(ds, "%Y-%m-%d")

    # Always process specific date, not "latest"
    data = extract_for_date(logical_date)
    transformed = transform(data)

    # Use partition/date-specific target
    load_to_partition(transformed, partition=ds)

with DAG(
    "backfillable_etl",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Enable backfill
    max_active_runs=3,  # Limit parallel backfills
) as dag:

    process = PythonOperator(
        task_id="process",
        python_callable=process_date,
        provide_context=True,
    )

# Backfill command:
# airflow dags backfill -s 2024-01-01 -e 2024-01-31 backfillable_etl
```

---
