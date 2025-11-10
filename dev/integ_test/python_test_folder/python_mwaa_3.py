import os

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


def print_env_variable():
    print("Starting environment variable check")

    try:
        env_value = os.environ.get("TEST_ENV_VARIABLE", "default")
        if env_value == "test_value":
            print("TEST_ENV_VARIABLE is set correctly")
        else:
            raise AirflowException(f"TEST_ENV_VARIABLE is set incorrectly to: {env_value}")
    except KeyError:
        raise AirflowException("TEST_ENV_VARIABLE is not set in the environment")


# Define the DAG
with DAG(
    dag_id="startup_script_test",
    schedule="@once",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
        "start_date": "2026-01-01",
    },
    is_paused_upon_creation=False,
) as dag:
    print_env_task = PythonOperator(task_id="check_environment_variable", python_callable=print_env_variable, dag=dag)

print_env_task
