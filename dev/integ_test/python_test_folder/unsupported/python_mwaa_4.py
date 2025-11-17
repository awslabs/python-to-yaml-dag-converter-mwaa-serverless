from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

NUM_TASKS = 10

default_args = {
    "owner": "airflow",
    "start_date": "2026-01-01",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def publish_many_log(**kwargs):
    """
    A task to publish 1000 logs
    """
    LOGGING_LINES = 1000
    taskId = kwargs["task_id"]
    print("start of the logs.")
    for i in range(LOGGING_LINES):
        print(f"{taskId}: {i}th logging.")


with DAG(
    dag_id="many_logging_dag",
    schedule="@daily",  # run at the time we trigger it
    default_args=default_args,
    is_paused_upon_creation=False,
) as dag:
    """
    This dag runs 10 tasks concurrently
    each task logging 1000 logs.
    """
    tasks_start = EmptyOperator(task_id="tasks_start")
    tasks_end = EmptyOperator(task_id="tasks_end")
    t = []
    for i in range(1, NUM_TASKS + 1):
        task_id = f"logging_task_{i}"
        t.append(
            PythonOperator(
                task_id=task_id,
                python_callable=publish_many_log,
                op_kwargs={"task_id": task_id},
            )
        )

    tasks_start >> t >> tasks_end
