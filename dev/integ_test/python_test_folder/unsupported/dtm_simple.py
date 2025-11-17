from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="example_task_mapping_second_order", schedule="@daily", default_args={"start_date": datetime(2026, 3, 4)}
) as dag2:

    @task
    def get_nums():
        return [1, 2, 3]

    @task
    def times_2(num):
        return num * 2

    @task
    def add_10(num):
        return num + 10

    _get_nums = get_nums()
    _times_2 = times_2.expand(num=_get_nums)
    add_10.expand(num=_times_2)
