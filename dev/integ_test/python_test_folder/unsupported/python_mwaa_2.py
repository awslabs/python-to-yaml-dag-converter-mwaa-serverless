from airflow import DAG
from airflow.operators.python import PythonOperator

# For DataPlane Canary Tests
with DAG(
    "trigger_scale_up",
    default_args={"start_date": "2026-01-01"},
    schedule="0,30 * * * *",
    is_paused_upon_creation=False,
) as dag:

    def is_canary():
        from os import environ

        if "AIRFLOW_ENV_NAME" in environ:
            if environ["AIRFLOW_ENV_NAME"].startswith("happy-integ-loop-env-DataPlaneCanary"):
                return True
        return False

    NUM_TASKS = 15 if is_canary() else 0

    def sleep_and_print_version():
        from time import sleep

        sleep(300)
        from os import environ

        import boto3
        import botocore

        print("boto3 version" + boto3.__version__)
        print("botocore version" + botocore.__version__)
        if "MWAA_IS_BASE_WORKER" in environ:
            print("MWAA_IS_BASE_WORKER=" + environ["MWAA_IS_BASE_WORKER"])
        else:
            print("IS_ADDITIONAL_WORKER")

    def create_task(task_id):
        task = PythonOperator(
            task_id=task_id,
            python_callable=sleep_and_print_version,
        )

        return task

    [create_task(f"sleep_and_print_{i}") for i in range(NUM_TASKS)]
