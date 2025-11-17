from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Define the DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(dag_id="simple_empty_dag", schedule=None, default_args=default_args) as dag:
    # Create the simplest possible task
    empty_task = EmptyOperator(
        task_id="do_nothing",
    )
