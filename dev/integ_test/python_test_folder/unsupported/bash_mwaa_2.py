import datetime as dt
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sdk import cross_downstream

layers = 3
ntasks = 1
sleep_seconds = 10

dag = DAG(
    dag_id="test-dag",
    schedule="@once",
    default_args={"start_date": dt.datetime(2026, 12, 1, 0, 0, 0)},
    is_paused_upon_creation=False,
)
previous_layer = None
for layer_n in range(layers):
    current_layer = [
        BashOperator(
            task_id=f"task{layer_n}-{task_n}",
            retries=2,
            retry_delay=timedelta(seconds=60),
            dag=dag,
            bash_command=f"sleep {sleep_seconds}",
        )
        for task_n in range(ntasks)
    ]
    if previous_layer:
        cross_downstream(previous_layer, current_layer)
    previous_layer = current_layer
