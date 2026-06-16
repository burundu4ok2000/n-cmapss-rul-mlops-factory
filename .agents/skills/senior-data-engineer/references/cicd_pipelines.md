# CI/CD for Data Pipelines

### GitHub Actions Workflow

```yaml
# .github/workflows/data-pipeline-ci.yml
name: Data Pipeline CI/CD

on:
  push:
    branches: [main, develop]
    paths:
      - 'dbt/**'
      - 'airflow/**'
      - 'tests/**'
  pull_request:
    branches: [main]

env:
  DBT_PROFILES_DIR: ./dbt
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install sqlfluff dbt-core dbt-snowflake

      - name: Lint SQL
        run: |
          sqlfluff lint dbt/models --dialect snowflake

      - name: Lint dbt project
        run: |
          cd dbt && dbt deps && dbt compile

  test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-core dbt-snowflake pytest great-expectations

      - name: Run dbt tests on CI schema
        run: |
          cd dbt
          dbt deps
          dbt seed --target ci
          dbt run --target ci --select state:modified+
          dbt test --target ci --select state:modified+

      - name: Run data contract tests
        run: |
          pytest tests/contracts/ -v

      - name: Run Great Expectations validation
        run: |
          great_expectations checkpoint run ci_checkpoint

  deploy-staging:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/develop'
    environment: staging
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to staging
        run: |
          cd dbt
          dbt deps
          dbt run --target staging
          dbt test --target staging

      - name: Run data quality checks
        run: |
          python scripts/run_quality_checks.py --env staging

  deploy-production:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - uses: actions/checkout@v4

      - name: Deploy to production
        run: |
          cd dbt
          dbt deps
          dbt run --target prod --full-refresh-models tag:full_refresh
          dbt run --target prod
          dbt test --target prod

      - name: Notify on success
        if: success()
        run: |
          curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
            -H 'Content-type: application/json' \
            -d '{"text":"dbt production deployment successful!"}'

      - name: Notify on failure
        if: failure()
        run: |
          curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
            -H 'Content-type: application/json' \
            -d '{"text":"dbt production deployment FAILED!"}'
```

### dbt CI Configuration

```yaml
# dbt_project.yml
name: 'analytics'
version: '1.0.0'

config-version: 2
profile: 'analytics'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

# Slim CI configuration
on-run-start:
  - "{{ dbt_utils.log_info('Starting dbt run') }}"

on-run-end:
  - "{{ dbt_utils.log_info('dbt run complete') }}"

vars:
  # CI testing with limited data
  ci_limit: "{{ 1000 if target.name == 'ci' else none }}"

# Model configurations
models:
  analytics:
    staging:
      +materialized: view
      +schema: staging

    intermediate:
      +materialized: ephemeral

    marts:
      +materialized: table
      +schema: marts

      core:
        +tags: ['core', 'daily']

      marketing:
        +tags: ['marketing', 'daily']
```

### Slim CI with State Comparison

```bash
# scripts/slim_ci.sh
#!/bin/bash
set -e

# Download production manifest for state comparison
aws s3 cp s3://dbt-artifacts/prod/manifest.json ./target/prod_manifest.json

# Run only modified models and their downstream dependencies
dbt run \
  --target ci \
  --select state:modified+ \
  --state ./target/prod_manifest.json

# Test only affected models
dbt test \
  --target ci \
  --select state:modified+ \
  --state ./target/prod_manifest.json

# Upload CI artifacts
dbt docs generate
aws s3 sync ./target s3://dbt-artifacts/ci/${GITHUB_SHA}/
```

---
