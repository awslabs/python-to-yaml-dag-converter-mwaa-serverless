from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def testing():
    return {"hi": "a"}


def total_func(str1, str2):
    return str1 + " " + str2


# Create the DAG
dag = DAG(dag_id="dynamic-map", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily")
test = PythonOperator(
    task_id="test",
    python_callable=testing,
    dag=dag,
)
test1 = PythonOperator(
    task_id="test1",
    python_callable=testing,
    dag=dag,
)
# Create the total task
total_task = PythonOperator(
    task_id="total",
    python_callable=total_func,
    op_kwargs={"str1": test.output["hi"], "str2": test1.output["hi"]},
    dag=dag,
)
# Set dependencies
[test, test1] >> total_task
