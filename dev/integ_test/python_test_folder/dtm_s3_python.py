from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator


# Define the functions outside the DAG
def count_lines_func(aws_conn_id, bucket, filename):
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return len(hook.read_key(filename, bucket).splitlines())


def total_func(lines):
    return sum(lines)


# Create the DAG
dag = DAG(dag_id="dynamic-map", default_args={"start_date": datetime(2026, 4, 2)}, schedule="@daily")

# Create the S3ListOperator
list_filenames = S3ListOperator(
    task_id="get_input",
    bucket="example-bucket",
    prefix='incoming/provider_a/{{ data_interval_start.strftime("%Y-%m-%d") }}',
    dag=dag,
)

# Create the mapped count_lines task using op_args for the mapping
counts = PythonOperator.partial(
    task_id="count_lines",
    python_callable=count_lines_func,
    op_kwargs={"aws_conn_id": "aws_default", "bucket": list_filenames.bucket},
    dag=dag,
).expand(
    op_args=list_filenames.output  # Use op_args for the filename
)

# Create the total task
total_task = PythonOperator(
    task_id="total",
    python_callable=total_func,
    op_kwargs={"lines": counts.output},
    dag=dag,
)

# Set dependencies
list_filenames >> counts >> total_task
