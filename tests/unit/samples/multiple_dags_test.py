from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# First DAG
dag = DAG(
    "bash_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
)

task1 = BashOperator(
    task_id="bash_task",
    bash_command='echo "Hello World"',
    dag=dag,
)

# Second DAG
dag2 = DAG(
    "bash_dag2",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
)

task2 = BashOperator(
    task_id="bash_task",
    bash_command='echo "Hello World"',
    dag=dag2,
)
