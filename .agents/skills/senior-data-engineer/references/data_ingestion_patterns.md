# Data Ingestion Patterns

### Change Data Capture (CDC)

```python
# Using Debezium with Kafka Connect
# connector-config.json
{
    "name": "postgres-cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": "password",
        "database.dbname": "source_db",
        "database.server.name": "source",
        "table.include.list": "public.orders,public.customers",
        "plugin.name": "pgoutput",
        "publication.name": "dbz_publication",
        "slot.name": "debezium_slot",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false"
    }
}
```

**Processing CDC Events:**

```python
def process_cdc_event(event):
    """Process Debezium CDC event."""

    operation = event.get("op")

    if operation == "c":  # Create (INSERT)
        after = event.get("after")
        return {"action": "insert", "data": after}

    elif operation == "u":  # Update
        before = event.get("before")
        after = event.get("after")
        return {"action": "update", "before": before, "after": after}

    elif operation == "d":  # Delete
        before = event.get("before")
        return {"action": "delete", "data": before}

    elif operation == "r":  # Read (snapshot)
        after = event.get("after")
        return {"action": "snapshot", "data": after}
```

### Bulk Ingestion

```python
# Efficient bulk loading to data warehouse
from concurrent.futures import ThreadPoolExecutor
import boto3

class BulkIngester:
    """Bulk ingest data to Snowflake via S3."""

    def __init__(self, s3_bucket: str, snowflake_conn):
        self.s3 = boto3.client('s3')
        self.bucket = s3_bucket
        self.snowflake = snowflake_conn

    def ingest_dataframe(self, df, table_name: str, mode: str = "append"):
        """Bulk ingest DataFrame to Snowflake."""

        # 1. Write to S3 as Parquet (compressed, columnar)
        s3_path = f"s3://{self.bucket}/staging/{table_name}/{uuid.uuid4()}"
        df.write.parquet(s3_path)

        # 2. Create external stage if not exists
        self.snowflake.execute(f"""
            CREATE STAGE IF NOT EXISTS {table_name}_stage
            URL = '{s3_path}'
            CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...')
            FILE_FORMAT = (TYPE = 'PARQUET')
        """)

        # 3. COPY INTO (much faster than INSERT)
        if mode == "overwrite":
            self.snowflake.execute(f"TRUNCATE TABLE {table_name}")

        self.snowflake.execute(f"""
            COPY INTO {table_name}
            FROM @{table_name}_stage
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)

        # 4. Cleanup staging files
        self._cleanup_s3(s3_path)
```

---
