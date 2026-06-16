# Slowly Changing Dimensions

### Type 0: Fixed Dimension

No changes allowed - original value preserved forever.

```sql
-- Type 0: Never update these fields
CREATE TABLE dim_customer_type0 (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(50),
    original_signup_date DATE,  -- Never changes
    original_source VARCHAR(50) -- Never changes
);
```

### Type 1: Overwrite

Simply overwrite old value with new. No history preserved.

```sql
-- Type 1: Update in place
UPDATE dim_customer
SET
    email = 'new.email@example.com',
    updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 'CUST001';

-- dbt implementation (Type 1)
-- models/dim_customer_type1.sql
{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,  -- Current value only
    phone,
    address,
    CURRENT_TIMESTAMP as updated_at
FROM {{ source('raw', 'customers') }}
```

### Type 2: Add New Row

Create new record with new values. Full history preserved.

```sql
-- Type 2 dimension structure
CREATE TABLE dim_customer_scd2 (
    customer_key SERIAL PRIMARY KEY,    -- Surrogate key
    customer_id VARCHAR(50),            -- Natural key
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),

    -- SCD2 tracking columns
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN,

    -- Hash for change detection
    row_hash VARCHAR(64)
);

-- SCD2 merge logic
MERGE INTO dim_customer_scd2 AS target
USING (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        MD5(CONCAT(first_name, last_name, email, city, state)) as row_hash
    FROM staging_customers
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE

-- Close existing record if changed
WHEN MATCHED AND target.row_hash != source.row_hash THEN
    UPDATE SET
        effective_end_date = CURRENT_TIMESTAMP,
        is_current = FALSE

-- Insert new record for changes
WHEN NOT MATCHED OR (MATCHED AND target.row_hash != source.row_hash) THEN
    INSERT (customer_id, first_name, last_name, email, city, state,
            effective_start_date, effective_end_date, is_current, row_hash)
    VALUES (source.customer_id, source.first_name, source.last_name, source.email,
            source.city, source.state, CURRENT_TIMESTAMP, '9999-12-31', TRUE, source.row_hash);
```

**dbt SCD2 Implementation:**

```sql
-- models/dim_customer_scd2.sql
{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'city', 'state']
    )
}}

WITH source_data AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        city,
        state,
        MD5(CONCAT_WS('|', first_name, last_name, email, city, state)) as row_hash,
        CURRENT_TIMESTAMP as extracted_at
    FROM {{ source('raw', 'customers') }}
),

{% if is_incremental() %}
-- Get current records that have changed
changed_records AS (
    SELECT
        s.*,
        t.customer_key as existing_key
    FROM source_data s
    LEFT JOIN {{ this }} t
        ON s.customer_id = t.customer_id
        AND t.is_current = TRUE
    WHERE t.customer_key IS NULL  -- New record
       OR t.row_hash != s.row_hash  -- Changed record
)
{% endif %}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'extracted_at']) }} as customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    extracted_at as effective_start_date,
    CAST('9999-12-31' AS TIMESTAMP) as effective_end_date,
    TRUE as is_current,
    row_hash
{% if is_incremental() %}
FROM changed_records
{% else %}
FROM source_data
{% endif %}
```

### Type 3: Add New Column

Add column for previous value. Limited history (usually just prior value).

```sql
-- Type 3: Previous value column
CREATE TABLE dim_customer_scd3 (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(50),
    city VARCHAR(100),
    previous_city VARCHAR(100),  -- Previous value
    city_change_date DATE,
    state VARCHAR(100),
    previous_state VARCHAR(100),
    state_change_date DATE
);

-- Update Type 3
UPDATE dim_customer_scd3
SET
    previous_city = city,
    city = 'New York',
    city_change_date = CURRENT_DATE
WHERE customer_id = 'CUST001';
```

### Type 4: Mini-Dimension

Separate rapidly changing attributes into a mini-dimension.

```sql
-- Main customer dimension (slowly changing)
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255)
);

-- Mini-dimension for rapidly changing attributes
CREATE TABLE dim_customer_profile (
    profile_key INT PRIMARY KEY,
    age_band VARCHAR(20),      -- '18-24', '25-34', etc.
    income_band VARCHAR(20),   -- 'Low', 'Medium', 'High'
    loyalty_tier VARCHAR(20)   -- 'Bronze', 'Silver', 'Gold'
);

-- Fact table references both
CREATE TABLE fct_sales (
    sale_id BIGINT PRIMARY KEY,
    customer_key INT REFERENCES dim_customer,
    profile_key INT REFERENCES dim_customer_profile,  -- Current profile at time of sale
    ...
);
```

### Type 6: Hybrid (1 + 2 + 3)

Combines Types 1, 2, and 3 for maximum flexibility.

```sql
-- Type 6: Combined approach
CREATE TABLE dim_customer_scd6 (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(50),

    -- Current values (Type 1 - always updated)
    current_city VARCHAR(100),
    current_state VARCHAR(100),

    -- Historical values (Type 2 - row versioned)
    historical_city VARCHAR(100),
    historical_state VARCHAR(100),

    -- Previous values (Type 3)
    previous_city VARCHAR(100),

    -- SCD2 tracking
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN
);
```

---
