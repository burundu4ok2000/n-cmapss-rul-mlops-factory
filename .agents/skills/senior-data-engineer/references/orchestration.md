# Orchestration

### Dependency Management

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

with DAG("complex_pipeline") as dag:

    # Wait for upstream DAG
    wait_for_source = ExternalTaskSensor(
        task_id="wait_for_source_etl",
        external_dag_id="source_etl_dag",
        external_task_id="final_task",
        execution_delta=timedelta(hours=0),
        timeout=3600,
        mode="poke",
        poke_interval=60,
    )

    # Parallel extraction group
    with TaskGroup("extract") as extract_group:
        extract_orders = PythonOperator(
            task_id="extract_orders",
            python_callable=extract_orders_func,
        )
        extract_customers = PythonOperator(
            task_id="extract_customers",
            python_callable=extract_customers_func,
        )
        extract_products = PythonOperator(
            task_id="extract_products",
            python_callable=extract_products_func,
        )

    # Sequential transformation
    with TaskGroup("transform") as transform_group:
        join_data = PythonOperator(
            task_id="join_data",
            python_callable=join_func,
        )
        aggregate = PythonOperator(
            task_id="aggregate",
            python_callable=aggregate_func,
        )
        join_data >> aggregate

    # Load
    load = PythonOperator(
        task_id="load",
        python_callable=load_func,
    )

    # Define dependencies
    wait_for_source >> extract_group >> transform_group >> load
```

### Dynamic DAG Generation

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yaml

def create_etl_dag(config: dict) -> DAG:
    """Factory function to create ETL DAGs from config."""

    dag = DAG(
        dag_id=f"etl_{config['source']}_{config['destination']}",
        schedule_interval=config.get('schedule', '@daily'),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['etl', 'auto-generated'],
    )

    with dag:
        extract = PythonOperator(
            task_id='extract',
            python_callable=create_extract_func(config['source']),
        )

        transform = PythonOperator(
            task_id='transform',
            python_callable=create_transform_func(config['transformations']),
        )

        load = PythonOperator(
            task_id='load',
            python_callable=create_load_func(config['destination']),
        )

        extract >> transform >> load

    return dag

# Load configurations
with open('/config/etl_pipelines.yaml') as f:
    configs = yaml.safe_load(f)

# Generate DAGs
for config in configs['pipelines']:
    dag_id = f"etl_{config['source']}_{config['destination']}"
    globals()[dag_id] = create_etl_dag(config)
```
