# Data Contracts

### Contract Definition

```yaml
# contracts/orders_v2.yaml
contract:
  name: orders
  version: "2.0.0"
  owner: data-platform@company.com
  team: Data Engineering
  slack_channel: "#data-platform-alerts"

description: |
  Order events from the e-commerce platform.
  Contains all customer orders with line items.

schema:
  type: object
  required:
    - order_id
    - customer_id
    - created_at
    - total_amount
  properties:
    order_id:
      type: string
      format: uuid
      description: "Unique order identifier"
      pii: false
      breaking_change: never

    customer_id:
      type: string
      description: "Customer identifier (foreign key)"
      pii: true
      retention_days: 365

    created_at:
      type: timestamp
      format: "ISO8601"
      timezone: "UTC"
      description: "Order creation timestamp"

    total_amount:
      type: decimal
      precision: 10
      scale: 2
      minimum: 0
      description: "Total order amount in USD"

    status:
      type: string
      enum: ["pending", "confirmed", "shipped", "delivered", "cancelled"]
      default: "pending"

    line_items:
      type: array
      items:
        type: object
        properties:
          product_id:
            type: string
          quantity:
            type: integer
            minimum: 1
          unit_price:
            type: decimal

# Quality SLAs
quality:
  freshness:
    max_delay_minutes: 60
    check_frequency: "*/15 * * * *"  # Every 15 minutes

  completeness:
    required_fields_null_rate: 0.0
    optional_fields_null_rate: 0.05

  uniqueness:
    order_id: true
    combination: ["order_id", "line_item_id"]

  validity:
    total_amount:
      min: 0
      max: 1000000
    status:
      allowed_values: ["pending", "confirmed", "shipped", "delivered", "cancelled"]

  volume:
    min_daily_records: 1000
    max_daily_records: 1000000
    anomaly_threshold: 0.5  # 50% deviation from average

# Semantic versioning rules
versioning:
  breaking_changes:
    - removing_required_field
    - changing_field_type
    - renaming_field
  non_breaking_changes:
    - adding_optional_field
    - adding_enum_value
    - changing_description

# Consumers
consumers:
  - name: analytics-dashboard
    team: Analytics
    contact: analytics@company.com
    usage: "Daily KPI dashboards"
    required_fields: ["order_id", "customer_id", "total_amount", "created_at"]

  - name: ml-churn-prediction
    team: ML Platform
    contact: ml-team@company.com
    usage: "Customer churn prediction model"
    required_fields: ["customer_id", "created_at", "total_amount"]

  - name: finance-reporting
    team: Finance
    contact: finance@company.com
    usage: "Revenue reconciliation"
    required_fields: ["order_id", "total_amount", "status"]

# Change management
change_process:
  notification_lead_time_days: 14
  approval_required_from:
    - data-platform-lead
    - affected-consumer-teams
  rollback_plan_required: true
```

### Contract Validation

```python
# contracts/validator.py
import yaml
import json
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from datetime import datetime
import jsonschema

@dataclass
class ContractValidationResult:
    contract_name: str
    version: str
    timestamp: datetime
    passed: bool
    schema_valid: bool
    quality_checks_passed: bool
    sla_checks_passed: bool
    violations: List[Dict[str, Any]]

class ContractValidator:
    """Validate data against contract definitions."""

    def __init__(self, contract_path: str):
        with open(contract_path) as f:
            self.contract = yaml.safe_load(f)

        self.contract_name = self.contract['contract']['name']
        self.version = self.contract['contract']['version']

    def validate_schema(self, data: List[Dict]) -> List[Dict]:
        """Validate data against JSON schema."""
        violations = []
        schema = self.contract['schema']

        for i, record in enumerate(data):
            try:
                jsonschema.validate(record, schema)
            except jsonschema.ValidationError as e:
                violations.append({
                    "type": "schema_violation",
                    "record_index": i,
                    "field": e.path[0] if e.path else None,
                    "message": e.message
                })

        return violations

    def validate_quality_slas(self, connection, table_name: str) -> List[Dict]:
        """Validate quality SLAs."""
        violations = []
        quality = self.contract.get('quality', {})

        # Freshness check
        if 'freshness' in quality:
            max_delay = quality['freshness']['max_delay_minutes']
            query = f"SELECT MAX(created_at) FROM {table_name}"
            result = connection.execute(query).fetchone()
            if result[0]:
                age_minutes = (datetime.now() - result[0]).total_seconds() / 60
                if age_minutes > max_delay:
                    violations.append({
                        "type": "freshness_violation",
                        "sla": f"max_delay_minutes: {max_delay}",
                        "actual": f"{age_minutes:.0f} minutes old",
                        "severity": "critical"
                    })

        # Completeness check
        if 'completeness' in quality:
            for field in self.contract['schema'].get('required', []):
                query = f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM {table_name}
                """
                result = connection.execute(query).fetchone()
                null_rate = result[1] / result[0] if result[0] > 0 else 0
                max_rate = quality['completeness']['required_fields_null_rate']
                if null_rate > max_rate:
                    violations.append({
                        "type": "completeness_violation",
                        "field": field,
                        "sla": f"null_rate <= {max_rate}",
                        "actual": f"null_rate = {null_rate:.4f}",
                        "severity": "critical"
                    })

        # Uniqueness check
        if 'uniqueness' in quality:
            for field, should_be_unique in quality['uniqueness'].items():
                if field == 'combination':
                    continue
                if should_be_unique:
                    query = f"""
                        SELECT COUNT(*) - COUNT(DISTINCT {field})
                        FROM {table_name}
                    """
                    result = connection.execute(query).fetchone()
                    if result[0] > 0:
                        violations.append({
                            "type": "uniqueness_violation",
                            "field": field,
                            "duplicates": result[0],
                            "severity": "critical"
                        })

        # Volume check
        if 'volume' in quality:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE DATE(created_at) = CURRENT_DATE"
            result = connection.execute(query).fetchone()
            daily_count = result[0]

            if daily_count < quality['volume']['min_daily_records']:
                violations.append({
                    "type": "volume_violation",
                    "sla": f"min_daily_records: {quality['volume']['min_daily_records']}",
                    "actual": daily_count,
                    "severity": "warning"
                })

        return violations

    def validate(self, connection, table_name: str, sample_data: List[Dict] = None) -> ContractValidationResult:
        """Run full contract validation."""
        violations = []

        # Schema validation (on sample data)
        schema_violations = []
        if sample_data:
            schema_violations = self.validate_schema(sample_data)
            violations.extend(schema_violations)

        # Quality SLA validation
        quality_violations = self.validate_quality_slas(connection, table_name)
        violations.extend(quality_violations)

        return ContractValidationResult(
            contract_name=self.contract_name,
            version=self.version,
            timestamp=datetime.now(),
            passed=len([v for v in violations if v.get('severity') == 'critical']) == 0,
            schema_valid=len(schema_violations) == 0,
            quality_checks_passed=len([v for v in quality_violations if v.get('severity') == 'critical']) == 0,
            sla_checks_passed=True,  # Add SLA timing checks
            violations=violations
        )
```

---
