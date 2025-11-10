import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

from dependencies.sample import build_numbers_list, double


def test(number: int):
    return "hi"


@task
def get_random_number():
    return 5


@task
def double_number(number12321: int):
    return double(number12321)


@task
def get_numbers_list():
    return build_numbers_list()


@task
def process_results(doubled_num: int, numbers_list: list):
    print(f"Doubled number: {doubled_num}")
    print(f"Numbers list: {numbers_list}")
    return {"doubled": doubled_num, "list": numbers_list}


@dag(
    dag_id="decorator_sample_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@daily",
    catchup=False,
    description="Sample DAG using TaskFlow API decorators",
)
def my_dag():
    random_num = get_random_number()
    doubled = double_number(random_num)
    numbers = get_numbers_list()
    final_result = process_results(2, numbers_list=numbers)
    return final_result


dag_instance = my_dag()
