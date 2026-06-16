# Data Vault Modeling

### Core Concepts

Data Vault provides:
- Full historization
- Parallel loading
- Flexibility for changing business rules
- Auditability

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Hub_Customer│◄───│Link_Customer│───►│  Hub_Order  │
│              │    │   _Order    │    │             │
└──────┬───────┘    └─────────────┘    └──────┬──────┘
       │                                      │
       ▼                                      ▼
┌─────────────┐                        ┌─────────────┐
│Sat_Customer │                        │  Sat_Order  │
│  _Details   │                        │  _Details   │
└─────────────┘                        └─────────────┘
```

### Hub Tables

Business keys and surrogate keys only.

```sql
-- Hub: Business entity identifier
CREATE TABLE hub_customer (
    hub_customer_key VARCHAR(64) PRIMARY KEY,  -- Hash of business key
    customer_id VARCHAR(50),                    -- Business key
    load_date TIMESTAMP,
    record_source VARCHAR(100)
);

-- Hub loading (idempotent insert)
INSERT INTO hub_customer (hub_customer_key, customer_id, load_date, record_source)
SELECT
    MD5(customer_id) as hub_customer_key,
    customer_id,
    CURRENT_TIMESTAMP as load_date,
    'SOURCE_CRM' as record_source
FROM staging_customers s
WHERE NOT EXISTS (
    SELECT 1 FROM hub_customer h
    WHERE h.customer_id = s.customer_id
);
```

### Satellite Tables

Descriptive attributes with full history.

```sql
-- Satellite: Attributes with history
CREATE TABLE sat_customer_details (
    hub_customer_key VARCHAR(64),
    load_date TIMESTAMP,
    load_end_date TIMESTAMP,

    -- Descriptive attributes
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),

    -- Change detection
    hash_diff VARCHAR(64),
    record_source VARCHAR(100),

    PRIMARY KEY (hub_customer_key, load_date),
    FOREIGN KEY (hub_customer_key) REFERENCES hub_customer
);

-- Satellite loading (delta detection)
INSERT INTO sat_customer_details
SELECT
    MD5(s.customer_id) as hub_customer_key,
    CURRENT_TIMESTAMP as load_date,
    NULL as load_end_date,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    MD5(CONCAT_WS('|', s.first_name, s.last_name, s.email, s.phone)) as hash_diff,
    'SOURCE_CRM' as record_source
FROM staging_customers s
LEFT JOIN sat_customer_details sat
    ON MD5(s.customer_id) = sat.hub_customer_key
    AND sat.load_end_date IS NULL
WHERE sat.hub_customer_key IS NULL  -- New customer
   OR sat.hash_diff != MD5(CONCAT_WS('|', s.first_name, s.last_name, s.email, s.phone));  -- Changed

-- Close previous satellite records
UPDATE sat_customer_details
SET load_end_date = CURRENT_TIMESTAMP
WHERE hub_customer_key IN (
    SELECT MD5(customer_id) FROM staging_customers
)
AND load_end_date IS NULL
AND load_date < CURRENT_TIMESTAMP;
```

### Link Tables

Relationships between hubs.

```sql
-- Link: Relationship between entities
CREATE TABLE link_customer_order (
    link_customer_order_key VARCHAR(64) PRIMARY KEY,
    hub_customer_key VARCHAR(64),
    hub_order_key VARCHAR(64),
    load_date TIMESTAMP,
    record_source VARCHAR(100),

    FOREIGN KEY (hub_customer_key) REFERENCES hub_customer,
    FOREIGN KEY (hub_order_key) REFERENCES hub_order
);

-- Link loading
INSERT INTO link_customer_order
SELECT
    MD5(CONCAT(s.customer_id, '|', s.order_id)) as link_customer_order_key,
    MD5(s.customer_id) as hub_customer_key,
    MD5(s.order_id) as hub_order_key,
    CURRENT_TIMESTAMP as load_date,
    'SOURCE_ORDERS' as record_source
FROM staging_orders s
WHERE NOT EXISTS (
    SELECT 1 FROM link_customer_order l
    WHERE l.hub_customer_key = MD5(s.customer_id)
      AND l.hub_order_key = MD5(s.order_id)
);
```

---
