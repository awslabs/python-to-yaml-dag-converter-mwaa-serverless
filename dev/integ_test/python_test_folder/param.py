from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Param


def my_function(a: int):
    print("Hello from task!")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    "sample_dag",
    default_args=default_args,
    schedule="@daily",
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=my_function,
        op_args=[42],
    )
