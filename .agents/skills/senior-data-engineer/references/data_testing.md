# Data Testing Frameworks

### Great Expectations

```python
# great_expectations_suite.py
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

# Initialize context
context = gx.get_context()

# Create expectation suite
suite = context.add_expectation_suite("orders_suite")

# Get validator
validator = context.get_validator(
    batch_request=BatchRequest(
        datasource_name="warehouse",
        data_asset_name="orders",
    ),
    expectation_suite_name="orders_suite"
)

# Schema expectations
validator.expect_table_columns_to_match_set(
    column_set=["order_id", "customer_id", "amount", "created_at", "status"],
    exact_match=True
)

# Completeness expectations
validator.expect_column_values_to_not_be_null(
    column="order_id",
    mostly=1.0  # 100% must be non-null
)

validator.expect_column_values_to_not_be_null(
    column="customer_id",
    mostly=0.99  # 99% must be non-null
)

# Uniqueness expectations
validator.expect_column_values_to_be_unique("order_id")

# Type expectations
validator.expect_column_values_to_be_of_type("amount", "FLOAT")
validator.expect_column_values_to_be_of_type("created_at", "TIMESTAMP")

# Range expectations
validator.expect_column_values_to_be_between(
    column="amount",
    min_value=0,
    max_value=1000000,
    mostly=0.999
)

# Categorical expectations
validator.expect_column_values_to_be_in_set(
    column="status",
    value_set=["pending", "confirmed", "shipped", "delivered", "cancelled"]
)

# Distribution expectations
validator.expect_column_mean_to_be_between(
    column="amount",
    min_value=50,
    max_value=500
)

# Freshness expectations
validator.expect_column_max_to_be_between(
    column="created_at",
    min_value={"$PARAMETER": "now() - interval '24 hours'"},
    max_value={"$PARAMETER": "now()"}
)

# Cross-table expectations (referential integrity)
validator.expect_column_pair_values_to_be_in_set(
    column_A="customer_id",
    column_B="customer_status",
    value_pairs_set=[
        ("cust_001", "active"),
        ("cust_002", "active"),
        # ...
    ]
)

# Save suite
validator.save_expectation_suite(discard_failed_expectations=False)

# Run validation
checkpoint = context.add_or_update_checkpoint(
    name="orders_checkpoint",
    validations=[
        {
            "batch_request": {
                "datasource_name": "warehouse",
                "data_asset_name": "orders",
            },
            "expectation_suite_name": "orders_suite",
        }
    ],
)

results = checkpoint.run()
print(f"Validation success: {results.success}")
```

### dbt Tests

```yaml
# models/marts/schema.yml
version: 2

models:
  - name: fct_orders
    description: "Order fact table with comprehensive testing"

    # Model-level tests
    tests:
      # Row count consistency
      - dbt_utils.equal_rowcount:
          compare_model: ref('stg_orders')

      # Expression test
      - dbt_utils.expression_is_true:
          expression: "net_amount >= 0"

      # Recency test
      - dbt_utils.recency:
          datepart: hour
          field: _loaded_at
          interval: 24

    columns:
      - name: order_id
        description: "Primary key - unique order identifier"
        tests:
          - unique
          - not_null
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^ORD-[0-9]{10}$"

      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
              severity: warn  # Don't fail, just warn

      - name: order_date
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'"
              max_value: "current_date"

      - name: net_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
              inclusive: true

      - name: quantity
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1
              max_value: 1000
              row_condition: "status != 'cancelled'"

      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']

  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null

      - name: email
        tests:
          - unique:
              where: "is_current = true"
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"

# Custom generic test
# tests/generic/test_no_orphan_records.sql
{% test no_orphan_records(model, column_name, parent_model, parent_column) %}
SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} NOT IN (
    SELECT {{ parent_column }}
    FROM {{ parent_model }}
)
{% endtest %}
```

### Custom Data Quality Checks

```python
# data_quality/quality_checks.py
from dataclasses import dataclass
from typing import List, Dict, Any, Callable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

@dataclass
class QualityCheck:
    name: str
    description: str
    severity: str  # "critical", "warning", "info"
    check_func: Callable
    threshold: float = 1.0

@dataclass
class QualityResult:
    check_name: str
    passed: bool
    actual_value: float
    threshold: float
    message: str
    timestamp: datetime

class DataQualityValidator:
    """Comprehensive data quality validation framework."""

    def __init__(self, connection):
        self.conn = connection
        self.checks: List[QualityCheck] = []
        self.results: List[QualityResult] = []

    def add_check(self, check: QualityCheck):
        self.checks.append(check)

    # Built-in check generators
    def add_null_check(self, table: str, column: str, max_null_rate: float = 0.0):
        def check_nulls():
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulls
                FROM {table}
            """
            result = self.conn.execute(query).fetchone()
            null_rate = result[1] / result[0] if result[0] > 0 else 0
            return null_rate <= max_null_rate, null_rate

        self.add_check(QualityCheck(
            name=f"null_check_{table}_{column}",
            description=f"Check null rate for {table}.{column}",
            severity="critical" if max_null_rate == 0 else "warning",
            check_func=check_nulls,
            threshold=max_null_rate
        ))

    def add_uniqueness_check(self, table: str, column: str):
        def check_unique():
            query = f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(DISTINCT {column}) as distinct_count
                FROM {table}
            """
            result = self.conn.execute(query).fetchone()
            is_unique = result[0] == result[1]
            duplicate_rate = 1 - (result[1] / result[0]) if result[0] > 0 else 0
            return is_unique, duplicate_rate

        self.add_check(QualityCheck(
            name=f"uniqueness_check_{table}_{column}",
            description=f"Check uniqueness for {table}.{column}",
            severity="critical",
            check_func=check_unique,
            threshold=0.0
        ))

    def add_freshness_check(self, table: str, timestamp_column: str, max_hours: int):
        def check_freshness():
            query = f"""
                SELECT MAX({timestamp_column}) as latest
                FROM {table}
            """
            result = self.conn.execute(query).fetchone()
            if result[0] is None:
                return False, float('inf')

            hours_old = (datetime.now() - result[0]).total_seconds() / 3600
            return hours_old <= max_hours, hours_old

        self.add_check(QualityCheck(
            name=f"freshness_check_{table}",
            description=f"Check data freshness for {table}",
            severity="critical",
            check_func=check_freshness,
            threshold=max_hours
        ))

    def add_range_check(self, table: str, column: str, min_val: float, max_val: float):
        def check_range():
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {column} < {min_val} OR {column} > {max_val} THEN 1 ELSE 0 END) as out_of_range
                FROM {table}
            """
            result = self.conn.execute(query).fetchone()
            violation_rate = result[1] / result[0] if result[0] > 0 else 0
            return violation_rate == 0, violation_rate

        self.add_check(QualityCheck(
            name=f"range_check_{table}_{column}",
            description=f"Check range [{min_val}, {max_val}] for {table}.{column}",
            severity="warning",
            check_func=check_range,
            threshold=0.0
        ))

    def add_referential_integrity_check(self, child_table: str, child_column: str,
                                        parent_table: str, parent_column: str):
        def check_referential():
            query = f"""
                SELECT COUNT(*)
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
                WHERE p.{parent_column} IS NULL AND c.{child_column} IS NOT NULL
            """
            result = self.conn.execute(query).fetchone()
            orphan_count = result[0]
            return orphan_count == 0, orphan_count

        self.add_check(QualityCheck(
            name=f"referential_integrity_{child_table}_{child_column}",
            description=f"Check FK {child_table}.{child_column} -> {parent_table}.{parent_column}",
            severity="warning",
            check_func=check_referential,
            threshold=0
        ))

    def run_all_checks(self) -> Dict[str, Any]:
        """Execute all quality checks and return results."""
        self.results = []

        for check in self.checks:
            try:
                passed, actual_value = check.check_func()
                result = QualityResult(
                    check_name=check.name,
                    passed=passed,
                    actual_value=actual_value,
                    threshold=check.threshold,
                    message=f"{'PASSED' if passed else 'FAILED'}: {check.description}",
                    timestamp=datetime.now()
                )
            except Exception as e:
                result = QualityResult(
                    check_name=check.name,
                    passed=False,
                    actual_value=-1,
                    threshold=check.threshold,
                    message=f"ERROR: {str(e)}",
                    timestamp=datetime.now()
                )

            self.results.append(result)
            logger.info(result.message)

        # Summary
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed

        critical_failures = [
            r for r, c in zip(self.results, self.checks)
            if not r.passed and c.severity == "critical"
        ]

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "success_rate": passed / total if total > 0 else 0,
            "critical_failures": len(critical_failures),
            "results": self.results,
            "overall_passed": len(critical_failures) == 0
        }
```

---
