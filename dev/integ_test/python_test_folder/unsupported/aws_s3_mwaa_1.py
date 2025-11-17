import datetime
import time
import uuid

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from botocore.exceptions import ClientError

TARGET_BUCKET_TAG = "DeferrableOperatorTestBucket"

with DAG(
    dag_id="deferrable_operator_test_dag_s3",
    default_args={"start_date": "2026-01-01"},
    schedule="@once",
    is_paused_upon_creation=False,
) as dag:

    def create_s3_key_path(**kwargs):
        # Create S3 client
        s3_client = boto3.client("s3")
        buckets = s3_client.list_buckets()["Buckets"]

        # Test bucket is created after certain date, it will help us filter out the ones that are not relevant
        bucket_creation_date = datetime.datetime(2023, 7, 20)

        buckets = filter(lambda bucket: (bucket["CreationDate"].replace(tzinfo=None) > bucket_creation_date), buckets)

        test_bucket = ""

        # Find the right bucket that has the tag DeferrableOperatorTestBucket
        for bucket in buckets:
            try:
                tags = s3_client.get_bucket_tagging(Bucket=bucket["Name"])["TagSet"]
                for tag in tags:
                    if tag["Key"] == "Name" and tag["Value"] == TARGET_BUCKET_TAG:
                        test_bucket = bucket["Name"]
                        break
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "NoSuchTagSet":
                    # Handle the case where the bucket does not have any tags
                    print(f"Bucket {bucket['Name']} does not have any tags. Ignoring")
                elif error_code == "IllegalLocationConstraintException":
                    # Handle the case where the bucket belongs to other region
                    print(f"Bucket {bucket['Name']} does not belong to current region. Ignoring")
                else:
                    # Handle other ClientErrors if needed
                    print(f"Other exception {error_code} encountered with bucket {bucket['Name']}. Ignoring")

        if test_bucket == "":
            raise NameError("Deferrable operator test bucket is not found")

        # Create a random UUID as s3 key
        test_key = str(uuid.uuid4())

        print("Test bucket is: " + test_bucket)
        print("Test key is: " + test_key)

        # Push them so it's visible to other tasks
        ti = kwargs["ti"]
        ti.xcom_push(key="test_bucket", value=test_bucket)
        ti.xcom_push(key="test_key", value=test_key)

    generate_s3_key_path = PythonOperator(
        task_id="generate_s3_key_path",
        python_callable=create_s3_key_path,
    )

    def sleep_job():
        time.sleep(30)

    sleep_before_writing_to_s3 = PythonOperator(
        task_id="sleep_before_writing_to_s3",
        python_callable=sleep_job,
    )

    s3_sensor_deferrable = S3KeySensor(
        task_id="s3_sensor_deferrable",
        bucket_name="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_bucket') }}",
        bucket_key="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_key') }}",
        deferrable=True,
    )

    write_to_s3_operator = S3CreateObjectOperator(
        task_id="write_data_to_s3",
        s3_bucket="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_bucket') }}",
        s3_key="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_key') }}",
        data="test-data",
    )

    delete_from_s3_operator = S3DeleteObjectsOperator(
        task_id="delete_data_from_s3",
        bucket="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_bucket') }}",
        keys="{{ ti.xcom_pull(task_ids='generate_s3_key_path',key='test_key') }}",
    )

    generate_s3_key_path >> [sleep_before_writing_to_s3, s3_sensor_deferrable]
    write_to_s3_operator.set_upstream(sleep_before_writing_to_s3)
    s3_sensor_deferrable >> delete_from_s3_operator
