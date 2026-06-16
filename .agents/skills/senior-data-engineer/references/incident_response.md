# Incident Response

### Runbook Template

```markdown
# Incident Runbook: Data Pipeline Failure

# Overview
This runbook covers procedures for handling data pipeline failures.

# Severity Levels
- **P1 (Critical)**: Data older than 24 hours, revenue-impacting
- **P2 (High)**: Data older than 4 hours, customer-facing dashboards affected
- **P3 (Medium)**: Data older than 1 hour, internal reports delayed
- **P4 (Low)**: Non-critical pipeline, no business impact

# Initial Response (First 15 minutes)

### 1. Acknowledge the Alert
```bash
# Acknowledge in PagerDuty
curl -X POST https://api.pagerduty.com/incidents/{incident_id}/acknowledge

# Post in #data-incidents Slack channel
```

### 2. Assess Impact
- Which tables are affected?
- Which downstream consumers are impacted?
- What is the data freshness currently?

```sql
-- Check data freshness
SELECT
    table_name,
    MAX(updated_at) as last_update,
    DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP) as hours_stale
FROM information_schema.tables
WHERE table_schema = 'analytics'
GROUP BY table_name
ORDER BY hours_stale DESC;
```

### 3. Identify Root Cause

#### Check Pipeline Status
```bash
# Airflow
airflow dags list-runs -d <dag_id> --state failed

# dbt
dbt debug
dbt run --select state:failed

# Spark
spark-submit --status <application_id>
```

#### Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| OOM errors | Data volume spike | Increase memory, add partitioning |
| Timeout | Slow query | Optimize query, check locks |
| Connection refused | Network/auth | Check credentials, VPC rules |
| Schema mismatch | Source change | Update schema, add contract |
| Duplicate key | Upstream bug | Deduplicate, fix source |

# Resolution Procedures

### Restart Failed Pipeline
```bash
# Clear failed Airflow task
airflow tasks clear <dag_id> -t <task_id> -s <start_date> -e <end_date>

# Rerun dbt model
dbt run --select <model_name>+

# Resubmit Spark job
spark-submit --deploy-mode cluster <job.py>
```

### Backfill Missing Data
```bash
# Airflow backfill
airflow dags backfill -s 2024-01-01 -e 2024-01-02 <dag_id>

# dbt incremental refresh
dbt run --full-refresh --select <model_name>
```

### Rollback Procedure
```bash
# dbt rollback (use previous version)
git checkout <previous_sha> -- models/<model>.sql
dbt run --select <model_name>

# Delta Lake time travel
spark.sql("""
    RESTORE TABLE analytics.orders TO VERSION AS OF 10
""")
```

# Post-Incident

### 1. Write Incident Report
- Timeline of events
- Root cause analysis
- Impact assessment
- Remediation steps taken
- Follow-up action items

### 2. Update Monitoring
- Add missing alerts
- Adjust thresholds
- Improve documentation

### 3. Share Learnings
- Post in #data-engineering
- Update runbooks
- Schedule blameless postmortem if P1/P2
```

---
