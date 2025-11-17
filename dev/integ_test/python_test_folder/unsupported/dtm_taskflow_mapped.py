from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="dynamic-map", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily") as dag:

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)
