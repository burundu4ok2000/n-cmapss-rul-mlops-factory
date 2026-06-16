# Observability and Lineage

### Data Lineage with OpenLineage

```python
# lineage/openlineage_emitter.py
from openlineage.client import OpenLineageClient
from openlineage.client.run import Run, RunEvent, RunState, Job, Dataset
from openlineage.client.facet import (
    SchemaDatasetFacet,
    SchemaField,
    SqlJobFacet,
    DataQualityMetricsInputDatasetFacet
)
from datetime import datetime
import uuid

class DataLineageEmitter:
    """Emit data lineage events to OpenLineage."""

    def __init__(self, api_url: str, namespace: str = "data-platform"):
        self.client = OpenLineageClient(url=api_url)
        self.namespace = namespace

    def emit_job_start(self, job_name: str, inputs: list, outputs: list,
                       sql: str = None) -> str:
        """Emit job start event."""
        run_id = str(uuid.uuid4())

        # Build input datasets
        input_datasets = [
            Dataset(
                namespace=self.namespace,
                name=inp['name'],
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaField(name=f['name'], type=f['type'])
                            for f in inp.get('schema', [])
                        ]
                    )
                }
            )
            for inp in inputs
        ]

        # Build output datasets
        output_datasets = [
            Dataset(
                namespace=self.namespace,
                name=out['name'],
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaField(name=f['name'], type=f['type'])
                            for f in out.get('schema', [])
                        ]
                    )
                }
            )
            for out in outputs
        ]

        # Build job facets
        job_facets = {}
        if sql:
            job_facets["sql"] = SqlJobFacet(query=sql)

        # Create and emit event
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(namespace=self.namespace, name=job_name, facets=job_facets),
            inputs=input_datasets,
            outputs=output_datasets
        )

        self.client.emit(event)
        return run_id

    def emit_job_complete(self, job_name: str, run_id: str,
                          output_metrics: dict = None):
        """Emit job completion event."""
        output_facets = {}
        if output_metrics:
            output_facets["dataQuality"] = DataQualityMetricsInputDatasetFacet(
                rowCount=output_metrics.get('row_count'),
                bytes=output_metrics.get('bytes')
            )

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(namespace=self.namespace, name=job_name),
            inputs=[],
            outputs=[]
        )

        self.client.emit(event)

    def emit_job_fail(self, job_name: str, run_id: str, error_message: str):
        """Emit job failure event."""
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id, facets={
                "errorMessage": {"message": error_message}
            }),
            job=Job(namespace=self.namespace, name=job_name),
            inputs=[],
            outputs=[]
        )

        self.client.emit(event)


# Usage example
emitter = DataLineageEmitter("http://marquez:5000/api/v1/lineage")

run_id = emitter.emit_job_start(
    job_name="transform_orders",
    inputs=[
        {"name": "raw.orders", "schema": [
            {"name": "id", "type": "string"},
            {"name": "amount", "type": "decimal"}
        ]}
    ],
    outputs=[
        {"name": "analytics.fct_orders", "schema": [
            {"name": "order_id", "type": "string"},
            {"name": "net_amount", "type": "decimal"}
        ]}
    ],
    sql="SELECT id as order_id, amount as net_amount FROM raw.orders"
)

# After job completes
emitter.emit_job_complete(
    job_name="transform_orders",
    run_id=run_id,
    output_metrics={"row_count": 1500000, "bytes": 125000000}
)
```

### Pipeline Monitoring with Prometheus

```python
# monitoring/metrics.py
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from functools import wraps
import time

# Define metrics
PIPELINE_RUNS = Counter(
    'pipeline_runs_total',
    'Total number of pipeline runs',
    ['pipeline_name', 'status']
)

PIPELINE_DURATION = Histogram(
    'pipeline_duration_seconds',
    'Pipeline execution duration',
    ['pipeline_name'],
    buckets=[60, 300, 600, 1800, 3600, 7200]
)

ROWS_PROCESSED = Counter(
    'rows_processed_total',
    'Total rows processed by pipeline',
    ['pipeline_name', 'table_name']
)

DATA_FRESHNESS = Gauge(
    'data_freshness_hours',
    'Hours since last data update',
    ['table_name']
)

DATA_QUALITY_SCORE = Gauge(
    'data_quality_score',
    'Data quality score (0-1)',
    ['table_name', 'check_type']
)

def track_pipeline(pipeline_name: str):
    """Decorator to track pipeline execution."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                PIPELINE_RUNS.labels(pipeline_name=pipeline_name, status='success').inc()
                return result
            except Exception as e:
                PIPELINE_RUNS.labels(pipeline_name=pipeline_name, status='failure').inc()
                raise
            finally:
                duration = time.time() - start_time
                PIPELINE_DURATION.labels(pipeline_name=pipeline_name).observe(duration)
        return wrapper
    return decorator

def record_rows_processed(pipeline_name: str, table_name: str, row_count: int):
    """Record number of rows processed."""
    ROWS_PROCESSED.labels(pipeline_name=pipeline_name, table_name=table_name).inc(row_count)

def update_freshness(table_name: str, hours_since_update: float):
    """Update data freshness metric."""
    DATA_FRESHNESS.labels(table_name=table_name).set(hours_since_update)

def update_quality_score(table_name: str, check_type: str, score: float):
    """Update data quality score."""
    DATA_QUALITY_SCORE.labels(table_name=table_name, check_type=check_type).set(score)

# Start metrics server
if __name__ == '__main__':
    start_http_server(8000)
```

### Alerting Configuration

```yaml
# alerting/prometheus_rules.yml
groups:
  - name: data_quality_alerts
    rules:
      - alert: DataFreshnessAlert
        expr: data_freshness_hours > 24
        for: 15m
        labels:
          severity: critical
          team: data-platform
        annotations:
          summary: "Data freshness SLA violated"
          description: "Table {{ $labels.table_name }} has not been updated for {{ $value }} hours"

      - alert: DataQualityDegraded
        expr: data_quality_score < 0.95
        for: 10m
        labels:
          severity: warning
          team: data-platform
        annotations:
          summary: "Data quality below threshold"
          description: "Table {{ $labels.table_name }} quality score is {{ $value }}"

      - alert: PipelineFailure
        expr: increase(pipeline_runs_total{status="failure"}[1h]) > 0
        for: 5m
        labels:
          severity: critical
          team: data-platform
        annotations:
          summary: "Pipeline failure detected"
          description: "Pipeline {{ $labels.pipeline_name }} has failed"

      - alert: PipelineSlowdown
        expr: histogram_quantile(0.95, rate(pipeline_duration_seconds_bucket[1h])) > 3600
        for: 30m
        labels:
          severity: warning
          team: data-platform
        annotations:
          summary: "Pipeline execution time degraded"
          description: "Pipeline {{ $labels.pipeline_name }} p95 duration is {{ $value }} seconds"

      - alert: LowRowCount
        expr: increase(rows_processed_total[24h]) < 1000
        for: 1h
        labels:
          severity: warning
          team: data-platform
        annotations:
          summary: "Unusually low row count"
          description: "Pipeline {{ $labels.pipeline_name }} processed only {{ $value }} rows in 24h"
```

---
