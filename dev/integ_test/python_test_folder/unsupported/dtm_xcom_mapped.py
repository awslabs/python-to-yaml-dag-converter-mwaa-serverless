from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Define the functions outside the DAG
def count_lines_func(*args):
    filename = args[0]
    print("hi")
    return filename


def total_func(lines):
    return sum(lines)


def list_some_names():
    return [["Name1"], ["Name2"], ["Name3"]]


# Create the DAG
dag = DAG(dag_id="dynamic-map", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily")

list_filenames = PythonOperator(
    task_id="get_input",
    python_callable=list_some_names,
    dag=dag,
)

counts = PythonOperator.partial(
    task_id="count_lines",
    python_callable=count_lines_func,
    dag=dag,
).expand(op_args=list_filenames.output)

total_task = PythonOperator(
    task_id="total",
    python_callable=total_func,
    op_kwargs={"lines": counts.output},
    dag=dag,
)

# Set dependencies
list_filenames >> counts >> total_task
