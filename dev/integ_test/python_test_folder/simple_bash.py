"""
Sample DAG with only Bash operators.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
dag = DAG(
    "bash_only_dag",
    default_args=default_args,
    description="A sample DAG with only Bash operators",
    schedule="@daily",
)
# Create working directory
create_workspace = BashOperator(
    task_id="create_workspace", bash_command='mkdir -p /tmp/bash_workflow && echo "Workspace created"', dag=dag
)
# Download sample data
download_data = BashOperator(
    task_id="download_data",
    bash_command='echo "sample,data,file" > /tmp/bash_workflow/data.csv && echo "Data downloaded"',
    dag=dag,
)
# Process the data
process_data = BashOperator(
    task_id="process_data",
    bash_command="wc -l /tmp/bash_workflow/data.csv > /tmp/bash_workflow/line_count.txt",
    dag=dag,
)
# Generate report
generate_report = BashOperator(
    task_id="generate_report",
    bash_command='echo "Processing completed at $(date)" > /tmp/bash_workflow/report.txt',
    dag=dag,
)
# Archive results
archive_results = BashOperator(
    task_id="archive_results", bash_command="tar -czf /tmp/bash_workflow_results.tar.gz -C /tmp bash_workflow/", dag=dag
)
# Cleanup workspace
cleanup_workspace = BashOperator(
    task_id="cleanup_workspace", bash_command='rm -rf /tmp/bash_workflow && echo "Workspace cleaned"', dag=dag
)
# Set task dependencies
create_workspace >> download_data >> process_data >> generate_report >> archive_results >> cleanup_workspace
