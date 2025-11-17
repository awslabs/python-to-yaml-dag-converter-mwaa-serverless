from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteObjectsOperator,
    S3ListOperator,
)

# Define the DAG
with DAG(
    dag_id="s3_dtm_example",
    schedule="@daily",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    # Define bucket name and object keys
    bucket_name = "dtm-example-bucket"
    object_keys = [f"data/file_{i}.txt" for i in range(3)]

    # Create S3 bucket
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    create_object_tasks = []
    for i in range(3):
        create_object = S3CreateObjectOperator(
            task_id=f"create_object_{i}",
            s3_bucket=bucket_name,
            s3_key=object_keys[i],
            data="This is a test file",
            replace=True,
        )
        create_object_tasks.append(create_object)

    # List objects in the bucket
    list_objects = S3ListOperator(
        task_id="list_objects",
        bucket=bucket_name,
    )

    # Use dynamic task mapping with S3TransferOperator to process each object
    # This uses the output from list_objects via XCom
    delete_objects = S3DeleteObjectsOperator.partial(
        task_id="delete_objects",
        bucket=bucket_name,
    ).expand(
        # This will extract the 'Key' from each object in the list_objects output
        keys=list_objects.output
    )

    # Delete the bucket
    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
    )

    # Set task dependencies
    create_bucket >> create_object_tasks >> list_objects >> delete_objects >> delete_bucket
