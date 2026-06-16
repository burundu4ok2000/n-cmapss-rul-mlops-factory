# Dimensional Modeling

### Star Schema

The most common pattern for analytical data models. One fact table surrounded by dimension tables.

```
                    ┌─────────────┐
                    │ dim_product │
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────▼───────┐    ┌─────────────┐
│ dim_customer│◄───│   fct_sales   │───►│  dim_date   │
└─────────────┘    └───────┬───────┘    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │  dim_store  │
                    └─────────────┘
```

**Fact Table (fct_sales):**

```sql
CREATE TABLE fct_sales (
    sale_id BIGINT PRIMARY KEY,

    -- Foreign keys to dimensions
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    store_key INT REFERENCES dim_store(store_key),
    date_key INT REFERENCES dim_date(date_key),

    -- Degenerate dimension (no separate table)
    order_number VARCHAR(50),

    -- Measures (facts)
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),

    -- Audit columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Partition by date for query performance
ALTER TABLE fct_sales
PARTITION BY RANGE (date_key);
```

**Dimension Table (dim_customer):**

```sql
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,  -- Surrogate key
    customer_id VARCHAR(50),       -- Natural/business key

    -- Attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),

    -- Hierarchies
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(50),

    -- SCD tracking
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,

    -- Audit
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**Date Dimension:**

```sql
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,      -- YYYYMMDD format
    full_date DATE,

    -- Day attributes
    day_of_week INT,
    day_of_month INT,
    day_of_year INT,
    day_name VARCHAR(10),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,

    -- Week attributes
    week_of_year INT,
    week_start_date DATE,
    week_end_date DATE,

    -- Month attributes
    month_number INT,
    month_name VARCHAR(10),
    month_start_date DATE,
    month_end_date DATE,

    -- Quarter attributes
    quarter_number INT,
    quarter_name VARCHAR(10),

    -- Year attributes
    year_number INT,
    fiscal_year INT,
    fiscal_quarter INT,

    -- Relative flags
    is_current_day BOOLEAN,
    is_current_week BOOLEAN,
    is_current_month BOOLEAN,
    is_current_quarter BOOLEAN,
    is_current_year BOOLEAN
);

-- Generate date dimension
INSERT INTO dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
    d as full_date,
    EXTRACT(DOW FROM d) as day_of_week,
    EXTRACT(DAY FROM d) as day_of_month,
    EXTRACT(DOY FROM d) as day_of_year,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    FALSE as is_holiday,  -- Update from holiday calendar
    EXTRACT(WEEK FROM d) as week_of_year,
    DATE_TRUNC('week', d) as week_start_date,
    DATE_TRUNC('week', d) + INTERVAL '6 days' as week_end_date,
    EXTRACT(MONTH FROM d) as month_number,
    TO_CHAR(d, 'Month') as month_name,
    DATE_TRUNC('month', d) as month_start_date,
    (DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::DATE as month_end_date,
    EXTRACT(QUARTER FROM d) as quarter_number,
    'Q' || EXTRACT(QUARTER FROM d) as quarter_name,
    EXTRACT(YEAR FROM d) as year_number,
    -- Fiscal year (assuming July start)
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1
         ELSE EXTRACT(YEAR FROM d) END as fiscal_year,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN CEIL((EXTRACT(MONTH FROM d) - 6) / 3.0)
         ELSE CEIL((EXTRACT(MONTH FROM d) + 6) / 3.0) END as fiscal_quarter,
    d = CURRENT_DATE as is_current_day,
    d >= DATE_TRUNC('week', CURRENT_DATE) AND d < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '7 days' as is_current_week,
    DATE_TRUNC('month', d) = DATE_TRUNC('month', CURRENT_DATE) as is_current_month,
    DATE_TRUNC('quarter', d) = DATE_TRUNC('quarter', CURRENT_DATE) as is_current_quarter,
    EXTRACT(YEAR FROM d) = EXTRACT(YEAR FROM CURRENT_DATE) as is_current_year
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d;
```

### Snowflake Schema

Normalized dimensions for reduced storage and update anomalies.

```
                         ┌─────────────┐
                         │ dim_category│
                         └──────┬──────┘
                                │
┌─────────────┐    ┌───────────▼────┐    ┌─────────────┐
│ dim_customer│◄───│   fct_sales    │───►│ dim_product │
└──────┬──────┘    └───────┬────────┘    └──────┬──────┘
       │                   │                    │
┌──────▼──────┐    ┌───────▼───────┐    ┌──────▼──────┐
│ dim_geography│   │   dim_date    │    │  dim_brand  │
└─────────────┘    └───────────────┘    └─────────────┘
```

**When to use Snowflake vs Star:**

| Criteria | Star Schema | Snowflake Schema |
|----------|-------------|------------------|
| Query complexity | Simple JOINs | More JOINs required |
| Query performance | Faster (fewer JOINs) | Slower |
| Storage | Higher (denormalized) | Lower (normalized) |
| ETL complexity | Higher | Lower |
| Dimension updates | Multiple places | Single place |
| Best for | BI/reporting | Storage-constrained |

### One Big Table (OBT)

Fully denormalized single table - gaining popularity with modern columnar warehouses.

```sql
CREATE TABLE obt_sales AS
SELECT
    -- Fact measures
    s.sale_id,
    s.quantity,
    s.unit_price,
    s.total_amount,

    -- Customer attributes (denormalized)
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    c.country,

    -- Product attributes (denormalized)
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,

    -- Date attributes (denormalized)
    d.full_date as sale_date,
    d.year_number,
    d.quarter_number,
    d.month_name,
    d.week_of_year,
    d.is_weekend

FROM fct_sales s
JOIN dim_customer c ON s.customer_key = c.customer_key AND c.is_current
JOIN dim_product p ON s.product_key = p.product_key AND p.is_current
JOIN dim_date d ON s.date_key = d.date_key;
```

**OBT Tradeoffs:**

| Pros | Cons |
|------|------|
| Simple queries (no JOINs) | Storage bloat |
| Fast for analytics | Harder to maintain |
| Great with columnar storage | Stale data risk |
| Self-documenting | Update anomalies |

---
