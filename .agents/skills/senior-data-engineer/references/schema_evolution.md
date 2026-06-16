# Schema Evolution

### Adding Columns

```sql
-- Safe: Add nullable column
ALTER TABLE fct_orders ADD COLUMN discount_amount DECIMAL(10,2);

-- With default
ALTER TABLE fct_orders ADD COLUMN currency VARCHAR(3) DEFAULT 'USD';

-- dbt handling
{{
    config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )
}}
```

### Handling in Spark/Delta

```python
# Delta Lake schema evolution
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/path/to/table")

# Explicit schema enforcement
spark.sql("""
    ALTER TABLE delta.`/path/to/table`
    ADD COLUMNS (new_column STRING)
""")

# Schema merge on read
df = spark.read \
    .option("mergeSchema", "true") \
    .format("delta") \
    .load("/path/to/table")
```

### Backward Compatibility

```sql
-- Create view for backward compatibility
CREATE VIEW orders_v1 AS
SELECT
    order_id,
    customer_id,
    amount,
    -- Map new columns to old schema
    COALESCE(discount_amount, 0) as discount,
    COALESCE(currency, 'USD') as currency
FROM orders_v2;

-- Deprecation pattern
CREATE VIEW orders_deprecated AS
SELECT * FROM orders_v1;
-- Add comment: "DEPRECATED: Use orders_v2. Will be removed 2024-06-01"
```

### Data Contracts for Schema Changes

```yaml
# contracts/orders_contract.yaml
name: orders
version: "2.0.0"
owner: data-team@company.com

schema:
  order_id:
    type: string
    required: true
    breaking_change: never

  customer_id:
    type: string
    required: true
    breaking_change: never

  amount:
    type: decimal
    precision: 10
    scale: 2
    required: true

  # New in v2.0.0
  discount_amount:
    type: decimal
    precision: 10
    scale: 2
    required: false
    added_in: "2.0.0"
    default: 0

  # Deprecated in v2.0.0
  legacy_status:
    type: string
    deprecated: true
    removed_in: "3.0.0"
    migration: "Use order_status instead"

compatibility:
  backward: true   # v2 readers can read v1 data
  forward: true    # v1 readers can read v2 data
```
