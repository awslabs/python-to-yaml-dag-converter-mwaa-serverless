from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

# Define the DAG
with DAG(
    dag_id="aws_s3_cleanup",
    schedule="@daily",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    # Define a list of objects to delete
    object_keys = [
        "logs/2023-01-01/app.log",
        "logs/2023-01-02/app.log",
        "logs/2023-01-03/app.log",
        "logs/2023-01-04/app.log",
        "logs/2023-01-05/app.log",
    ]

    # Delete objects using dynamic task mapping
    delete_objects = S3DeleteObjectsOperator.partial(
        task_id="delete_s3_object",
        aws_conn_id="aws_default",
        bucket="my-log-bucket",
    ).expand(
        keys=[[key] for key in object_keys]  # Each task gets a list with one key
    )
