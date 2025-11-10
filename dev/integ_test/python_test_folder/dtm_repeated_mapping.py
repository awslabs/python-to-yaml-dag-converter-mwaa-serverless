from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="dynamic-map", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily") as dag:

    @task
    def add_one(x: int):
        return x + 1

    first = add_one.expand(x=[1, 2, 3])
    second = add_one.expand(x=first)
