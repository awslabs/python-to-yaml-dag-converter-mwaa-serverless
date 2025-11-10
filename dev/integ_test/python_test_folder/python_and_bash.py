"""
Sample DAG with Bash and Python operators.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_hello():
    """Simple Python function."""
    print("Hello from Python!")
    return "success"


def process_data():
    """Process some data."""
    data = [1, 2, 3, 4, 5]
    result = sum(data)
    print(f"Sum of data: {result}")
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    "sample_bash_python_dag",
    default_args=default_args,
    description="A sample DAG with Bash and Python operators",
    schedule="@daily",
)
# Bash operator to create directory
create_dir = BashOperator(
    task_id="create_directory", bash_command='mkdir -p /tmp/sample_dag && echo "Directory created"', dag=dag
)
# Bash operator to list files
list_files = BashOperator(task_id="list_files", bash_command="ls -la /tmp/sample_dag", dag=dag)
# Python operator to process data
process_task = PythonOperator(task_id="process_data", python_callable=process_data, dag=dag)
# Bash operator to cleanup
cleanup = BashOperator(task_id="cleanup", bash_command='rm -rf /tmp/sample_dag && echo "Cleanup completed"', dag=dag)
# Set task dependencies
create_dir >> list_files >> process_task >> cleanup
