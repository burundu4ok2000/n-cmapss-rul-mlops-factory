---
name: "senior-data-engineer"
description: Data engineering skill for building scalable data pipelines, ETL/ELT systems, and data infrastructure. Expertise in Python, SQL, Spark, Airflow, dbt, Kafka, and modern data stack. Includes data modeling, pipeline orchestration, data quality, and DataOps. Use when designing data architectures, building data pipelines, optimizing data workflows, implementing data governance, or troubleshooting data issues.
---

# Senior Data Engineer: Architectural Blueprints & Production Patterns

This skill serves as a high-fidelity reference for building scalable, reliable, and cost-effective data platforms. When using these resources, adopt the mindset of a **Principal Data Engineer with 15+ years of experience**, focusing on architectural trade-offs and operational maturity.

### Core Principles:
- **Scalability & Sovereignty**: Prioritize designs that handle petabyte-scale loads while maintaining strict control over data gravity and security.
- **Idempotence & Reliability**: Every task must be re-runnable without side effects. Emphasize "Design for Failure" using DLQs, retries with backoff, and circuit breakers.
- **Observability by Default**: Pipelines are only as good as their monitoring. Enforce data contracts, testing at every layer, and end-to-end lineage tracking.

Refer to the modular guides in the `references/` directory for precise implementation blueprints and best practices.


### 1. Data Pipeline Architecture
Core principles and blueprints for robust data systems:
- **Architecture Patterns**: Lambda, Kappa, and Medallion (Bronze/Silver/Gold) implementation guides. See `references/architecture_patterns.md`.
- **Batch Processing**: Apache Spark optimization and idempotent Airflow DAG patterns. See `references/batch_processing.md`.
- **Stream Processing**: Kafka topic design and Spark Structured Streaming state management. See `references/stream_processing.md`.
- **Exactly-Once Semantics**: Producer idempotence, transactions, and idempotent external writes. See `references/exactly_once_semantics.md`.
- **Error Handling**: Dead Letter Queues (DLQ) and Circuit Breaker patterns for resilient pipelines. See `references/error_handling.md`.
- **Data Ingestion Patterns**: Change Data Capture (CDC) with Debezium and efficient bulk loading. See `references/data_ingestion_patterns.md`.
- **Orchestration**: Complex dependency management and dynamic DAG generation. See `references/orchestration.md`.


### 2. Data Modeling Patterns
Comprehensive architectural patterns for analytics and warehousing:
- **Dimensional Modeling**: Comparative analysis of Star, Snowflake, and OBT schemas for modern columnar warehouses. See `references/dimensional_modeling.md`.
- **Slowly Changing Dimensions**: Technical implementation of SCD Types 0-6 with SQL and dbt. See `references/slowly_changing_dimensions.md`.
- **Data Vault 2.0**: Agile warehousing using Hubs, Links, and Satellites for maximum auditability. See `references/data_vault_modeling.md`.
- **dbt Best Practices**: Model organization, intermediate layer abstraction, and optimized incremental materialization. See `references/dbt_best_practices.md`.
- **Partitioning and Clustering**: Physical optimization strategies for performance at scale. See `references/partitioning_and_clustering.md`.
- **Schema Evolution**: Managing breaking changes, backward compatibility, and data contracts. See `references/schema_evolution.md`.


### 3. DataOps Best Practices
Methodologies for data quality, reliability, and lifecycle management:
- **Data Testing**: Frameworks like Great Expectations and dbt tests for validating data at every stage. See `references/data_testing.md`.
- **Data Contracts**: Defining and enforcing schemas and SLAs between producers and consumers. See `references/data_contracts.md`.
- **CI/CD for Data**: Automated testing, linting (SQLFluff), and Slim CI patterns for dbt. See `references/cicd_pipelines.md`.
- **Observability and Lineage**: Tracking data flow with OpenLineage and monitoring metrics with Prometheus. See `references/observability_and_lineage.md`.
- **Incident Response**: Runbooks and procedures for handling pipeline failures and data issues. See `references/incident_response.md`.
- **Cost Optimization**: Strategies for analyzing and reducing warehouse and storage costs. See `references/cost_optimization.md`.


### 4. Troubleshooting & Performance
See `references/troubleshooting.md` for:
- **Pipeline Failures**: Resolving Airflow timeouts, Spark OOM errors, and Kafka consumer lag.
- **Data Quality Issues**: Detecting and fixing duplicates, stale data, and schema drift.
- **Performance Tuning**: Query plan analysis, partition pruning, and dbt model optimization.
- **Infrastructure Recovery**: Incident response patterns for critical data pipeline outages.

### 5. Engineering Workflows
See `references/workflows.md` for:
- **Batch ETL Blueprint**: Step-by-step from source discovery to Snowflake/dbt marts.
- **Streaming Implementation**: Building Kafka-to-Delta Lake pipelines with Spark Streaming.
- **DQ Framework Setup**: Integrating Great Expectations and dbt-expectations into pipelines.
- **Contract Enforcement**: Practical implementation of Data Contracts across producer teams.
