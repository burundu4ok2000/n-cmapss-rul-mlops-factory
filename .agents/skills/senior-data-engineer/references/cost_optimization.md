# Cost Optimization

### Query Cost Analysis

```sql
-- Snowflake query cost analysis
SELECT
    query_id,
    user_name,
    warehouse_name,
    execution_time / 1000 as execution_seconds,
    bytes_scanned / 1e9 as gb_scanned,
    credits_used_cloud_services,
    query_text
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY credits_used_cloud_services DESC
LIMIT 20;

-- BigQuery cost analysis
SELECT
    user_email,
    query,
    total_bytes_processed / 1e12 as tb_processed,
    total_bytes_processed / 1e12 * 5 as estimated_cost_usd,  -- $5/TB
    creation_time
FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_USER`
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 20;
```

### Cost Optimization Strategies

```python
# cost/optimizer.py
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd

@dataclass
class CostRecommendation:
    category: str
    current_cost: float
    potential_savings: float
    recommendation: str
    priority: str

class CostOptimizer:
    """Analyze and optimize data platform costs."""

    def __init__(self, connection):
        self.conn = connection

    def analyze_query_costs(self) -> List[CostRecommendation]:
        """Identify expensive queries and optimization opportunities."""
        recommendations = []

        # Find queries scanning full tables
        full_scans = self.conn.execute("""
            SELECT
                query_text,
                COUNT(*) as execution_count,
                AVG(bytes_scanned) as avg_bytes,
                SUM(credits_used) as total_credits
            FROM query_history
            WHERE bytes_scanned > 1e10  -- > 10GB
              AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
            GROUP BY query_text
            HAVING COUNT(*) > 10
            ORDER BY total_credits DESC
        """).fetchall()

        for query, count, avg_bytes, credits in full_scans:
            recommendations.append(CostRecommendation(
                category="Query Optimization",
                current_cost=credits,
                potential_savings=credits * 0.7,  # Estimate 70% savings
                recommendation=f"Add WHERE clause or partitioning to reduce scan. Query runs {count}x/week, scans {avg_bytes/1e9:.1f}GB each time.",
                priority="high"
            ))

        return recommendations

    def analyze_storage_costs(self) -> List[CostRecommendation]:
        """Identify storage optimization opportunities."""
        recommendations = []

        # Find large unused tables
        unused_tables = self.conn.execute("""
            SELECT
                table_name,
                bytes / 1e9 as size_gb,
                last_accessed
            FROM table_metadata
            WHERE last_accessed < DATEADD(day, -90, CURRENT_TIMESTAMP)
              AND bytes > 1e9  -- > 1GB
            ORDER BY bytes DESC
        """).fetchall()

        for table, size, last_accessed in unused_tables:
            monthly_cost = size * 0.023  # $0.023/GB/month for S3
            recommendations.append(CostRecommendation(
                category="Storage",
                current_cost=monthly_cost,
                potential_savings=monthly_cost,
                recommendation=f"Table {table} ({size:.1f}GB) not accessed since {last_accessed}. Consider archiving or deleting.",
                priority="medium"
            ))

        # Find tables without partitioning
        unpartitioned = self.conn.execute("""
            SELECT table_name, bytes / 1e9 as size_gb
            FROM table_metadata
            WHERE partition_column IS NULL
              AND bytes > 10e9  -- > 10GB
        """).fetchall()

        for table, size in unpartitioned:
            recommendations.append(CostRecommendation(
                category="Storage",
                current_cost=0,
                potential_savings=size * 0.1,  # Estimate 10% query cost savings
                recommendation=f"Table {table} ({size:.1f}GB) is not partitioned. Add partitioning to reduce query costs.",
                priority="high"
            ))

        return recommendations

    def analyze_compute_costs(self) -> List[CostRecommendation]:
        """Identify compute optimization opportunities."""
        recommendations = []

        # Find oversized warehouses
        warehouse_util = self.conn.execute("""
            SELECT
                warehouse_name,
                warehouse_size,
                AVG(avg_running_queries) as avg_queries,
                AVG(credits_used) as avg_credits
            FROM warehouse_metering_history
            WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
            GROUP BY warehouse_name, warehouse_size
        """).fetchall()

        for wh, size, avg_queries, avg_credits in warehouse_util:
            if avg_queries < 1 and size not in ['X-Small', 'Small']:
                recommendations.append(CostRecommendation(
                    category="Compute",
                    current_cost=avg_credits * 7,  # Weekly
                    potential_savings=avg_credits * 7 * 0.5,
                    recommendation=f"Warehouse {wh} ({size}) has low utilization ({avg_queries:.1f} avg queries). Consider downsizing.",
                    priority="high"
                ))

        return recommendations

    def generate_report(self) -> Dict:
        """Generate comprehensive cost optimization report."""
        all_recommendations = (
            self.analyze_query_costs() +
            self.analyze_storage_costs() +
            self.analyze_compute_costs()
        )

        total_current = sum(r.current_cost for r in all_recommendations)
        total_savings = sum(r.potential_savings for r in all_recommendations)

        return {
            "total_current_monthly_cost": total_current,
            "total_potential_savings": total_savings,
            "savings_percentage": total_savings / total_current * 100 if total_current > 0 else 0,
            "recommendations": [
                {
                    "category": r.category,
                    "current_cost": r.current_cost,
                    "potential_savings": r.potential_savings,
                    "recommendation": r.recommendation,
                    "priority": r.priority
                }
                for r in sorted(all_recommendations, key=lambda x: -x.potential_savings)
            ]
        }
```
