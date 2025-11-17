from __future__ import annotations

from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3ListOperator,
)

DAG_ID = "example_s3"
DATA = """
    apple,0.5
    milk,2.5
    bread,4.0
"""
# Empty string prefix refers to the bucket root
# See what prefix is here https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
PREFIX = ""
DELIMITER = "/"
TAG_KEY = "test-s3-bucket-tagging-key"
TAG_VALUE = "test-s3-bucket-tagging-value"
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 7, 6),
    "retries": 1,
}
# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    default_args=default_args,
)
bucket_name = "py2yml-my-personal-test-s3-bucket"
bucket_name_2 = "py2yml-my-personal-test-s3-bucket-v2"
key = "py2yml-key"
key_2 = "py2yml-key-v2"
key_regex_pattern = ".*-key"


def check_fn(files: list, **kwargs) -> bool:
    """
    Example of custom check: check if all files are bigger than ``20 bytes``
    :param files: List of S3 object attributes.
    :return: true if the criteria is met
    """
    return all(f.get("Size", 0) > 20 for f in files)


# Create bucket
create_bucket = S3CreateBucketOperator(
    task_id="create_bucket",
    bucket_name=bucket_name,
    dag=dag,
)
create_bucket_2 = S3CreateBucketOperator(
    task_id="create_bucket_2",
    bucket_name=bucket_name_2,
    dag=dag,
)
# Create object
create_object = S3CreateObjectOperator(
    task_id="create_object",
    s3_bucket=bucket_name,
    s3_key=key,
    data=DATA,
    replace=True,
    dag=dag,
)
create_object_2 = S3CreateObjectOperator(
    task_id="create_object_2",
    s3_bucket=bucket_name,
    s3_key=key_2,
    data=DATA,
    replace=True,
    dag=dag,
)
# List keys
list_keys = S3ListOperator(
    task_id="list_keys",
    bucket=bucket_name,
    prefix=PREFIX,
    dag=dag,
)
# Delete bucket
delete_bucket = S3DeleteBucketOperator(
    task_id="delete_bucket",
    bucket_name=bucket_name,
    force_delete=True,
    dag=dag,
)
# Delete bucket 2
delete_bucket_2 = S3DeleteBucketOperator(
    task_id="delete_bucket_2",
    bucket_name=bucket_name_2,
    force_delete=True,
    dag=dag,
)
# Set task dependencies
chain(
    # TEST SETUP
    # TEST BODY
    create_bucket,
    create_bucket_2,
    create_object,
    create_object_2,
    list_keys,
    # TEST TEARDOWN
    delete_bucket,
    delete_bucket_2,
)
