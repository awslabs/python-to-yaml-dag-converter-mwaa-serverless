from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def my_function(a: int):
    print("Hello from task!")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sample_dag",
    default_args=default_args,
    schedule="@daily",
)

task1 = PythonOperator(
    task_id="task1",
    python_callable=my_function,
    op_args=[42],
    dag=dag,
)
