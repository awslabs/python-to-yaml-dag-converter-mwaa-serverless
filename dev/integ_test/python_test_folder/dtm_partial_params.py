from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="dynamic-dag", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily") as dag:

    @task
    def add(x: int, y: int):
        return x + y

    added_values = add.partial(y=10).expand(x=[1, 2, 3])
