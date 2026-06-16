# dbt Best Practices

### Model Organization

```
models/
├── staging/           # 1:1 with source tables
│   ├── stg_orders.sql
│   ├── stg_customers.sql
│   └── _staging.yml
├── intermediate/      # Business logic transformations
│   ├── int_orders_enriched.sql
│   └── _intermediate.yml
└── marts/             # Business-facing models
    ├── core/
    │   ├── dim_customers.sql
    │   ├── fct_orders.sql
    │   └── _core.yml
    └── marketing/
        ├── mrt_customer_segments.sql
        └── _marketing.yml
```

### Staging Models

```sql
-- models/staging/stg_orders.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'orders') }}
),

renamed AS (
    SELECT
        -- Primary key
        id as order_id,

        -- Foreign keys
        customer_id,
        product_id,

        -- Timestamps
        created_at as order_created_at,
        updated_at as order_updated_at,

        -- Measures
        quantity,
        CAST(unit_price AS DECIMAL(10,2)) as unit_price,
        CAST(discount AS DECIMAL(5,2)) as discount_percent,

        -- Status
        UPPER(status) as order_status

    FROM source
)

SELECT * FROM renamed
```

### Intermediate Models

```sql
-- models/intermediate/int_orders_enriched.sql
{{
    config(
        materialized='ephemeral'  -- Not persisted, just CTE
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT
        o.order_id,
        o.order_created_at,
        o.order_status,

        -- Customer info
        c.customer_id,
        c.customer_name,
        c.customer_segment,

        -- Product info
        p.product_id,
        p.product_name,
        p.category,

        -- Calculated fields
        o.quantity,
        o.unit_price,
        o.quantity * o.unit_price as gross_amount,
        o.quantity * o.unit_price * (1 - COALESCE(o.discount_percent, 0) / 100) as net_amount

    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products p ON o.product_id = p.product_id
)

SELECT * FROM enriched
```

### Incremental Models

```sql
-- models/marts/fct_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['order_date']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
    -- Only process new/changed records
    WHERE order_updated_at > (
        SELECT COALESCE(MAX(order_updated_at), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
),

final AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        DATE(order_created_at) as order_date,
        order_created_at,
        order_updated_at,
        order_status,
        quantity,
        unit_price,
        gross_amount,
        net_amount,
        CURRENT_TIMESTAMP as _loaded_at
    FROM orders
)

SELECT * FROM final
```

### Testing

```yaml
# models/marts/_core.yml
version: 2

models:
  - name: fct_orders
    description: "Order fact table"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: net_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: true

      - name: order_date
        tests:
          - not_null
          - dbt_utils.recency:
              datepart: day
              field: order_date
              interval: 1
```

### Macros

```sql
-- macros/generate_surrogate_key.sql
{% macro generate_surrogate_key(columns) %}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ROUND({{ column_name }} / 100.0, 2)
{% endmacro %}

-- macros/safe_divide.sql
{% macro safe_divide(numerator, denominator, default=0) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN {{ default }}
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}

-- Usage in models:
-- {{ safe_divide('revenue', 'orders') }} as avg_order_value
```

---
