# Partitioning and Clustering

### Partitioning Strategies

**Time-based Partitioning (Most Common):**

```sql
-- BigQuery
CREATE TABLE fct_events
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, event_type
AS SELECT * FROM raw_events;

-- Snowflake (automatic micro-partitioning)
-- Explicit clustering for optimization
ALTER TABLE fct_events CLUSTER BY (event_date, user_id);

-- Spark/Delta Lake
df.write \
    .format("delta") \
    .partitionBy("event_date") \
    .save("/path/to/table")
```

**Partition Pruning:**

```sql
-- Query with partition filter (fast)
SELECT * FROM fct_events
WHERE event_date = '2024-01-15';  -- Scans only 1 partition

-- Query without partition filter (slow - full scan)
SELECT * FROM fct_events
WHERE user_id = '12345';  -- Scans all partitions
```

**Partition Size Guidelines:**

| Partition | Size Target | Notes |
|-----------|-------------|-------|
| Daily | 1-10 GB | Ideal for most cases |
| Hourly | 100 MB - 1 GB | High-volume streaming |
| Monthly | 10-100 GB | Infrequent access |

### Clustering

```sql
-- BigQuery clustering (up to 4 columns)
CREATE TABLE fct_sales
PARTITION BY DATE(sale_date)
CLUSTER BY customer_id, product_id
AS SELECT * FROM raw_sales;

-- Snowflake clustering
CREATE TABLE fct_sales (
    sale_id INT,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    sale_date DATE,
    amount DECIMAL(10,2)
)
CLUSTER BY (customer_id, sale_date);

-- Delta Lake Z-ordering
OPTIMIZE events ZORDER BY (user_id, event_type);
```

**When to Cluster:**

| Column Type | Cluster? | Notes |
|-------------|----------|-------|
| High cardinality filter columns | Yes | customer_id, product_id |
| Join keys | Yes | Improves join performance |
| Low cardinality | Maybe | status, type (limited benefit) |
| Frequently updated | No | Clustering breaks on updates |

---
