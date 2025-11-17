from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="repeated_mapping", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily") as dag:

    @task
    def add(x: int, y: int):
        return x + y

    added_values = add.expand(x=[2, 4, 8], y=[5, 10])
