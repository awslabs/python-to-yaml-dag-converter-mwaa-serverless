from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3ListOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Default arguments for all DAGs
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Load configuration from YAML file
def load_aws_configs():
    # If file doesn't exist, use default configs
    return [
        {
            "name": "data_lake",
            "schedule": "@daily",
            "buckets": [
                {"name": "data-lake-raw", "region": "us-east-1"},
                {"name": "data-lake-processed", "region": "us-east-1"},
            ],
            "files": [
                {"bucket": "data-lake-raw", "key": "sample/data1.csv", "content": "sample,data,1"},
                {"bucket": "data-lake-raw", "key": "sample/data2.csv", "content": "sample,data,2"},
            ],
        },
        {
            "name": "analytics",
            "schedule": "@hourly",
            "buckets": [{"name": "analytics-reports", "region": "us-west-2"}],
            "files": [{"bucket": "analytics-reports", "key": "reports/daily.json", "content": '{"status":"ok"}'}],
        },
    ]


# Create DAGs dynamically
def generate_aws_dags():
    dags = {}
    configs = load_aws_configs()

    for config in configs:
        dag_id = f"aws_{config['name']}"
        schedule = config.get("schedule", "@daily")

        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=schedule,
            description=f"Dynamically generated AWS DAG for {config['name']}",
        )

        # Create tasks based on configuration
        with dag:
            # Create bucket tasks
            bucket_tasks = []
            for bucket_config in config["buckets"]:
                create_bucket = S3CreateBucketOperator(
                    task_id=f"create_bucket_{bucket_config['name']}",
                    bucket_name=bucket_config["name"],
                    region_name=bucket_config.get("region", "us-east-1"),
                )
                bucket_tasks.append(create_bucket)

            # Create file tasks
            file_tasks = []
            for file_config in config["files"]:
                create_file = S3CreateObjectOperator(
                    task_id=f"create_file_{file_config['bucket']}_{file_config['key'].replace('/', '_')}",
                    s3_bucket=file_config["bucket"],
                    s3_key=file_config["key"],
                    data=file_config["content"],
                    replace=True,
                )
                file_tasks.append(create_file)

                # Set dependency: create bucket before creating files
                for bucket_task in bucket_tasks:
                    if f"create_bucket_{file_config['bucket']}" == bucket_task.task_id:
                        bucket_task >> create_file

            # List objects in each bucket
            list_tasks = []
            for bucket_config in config["buckets"]:
                list_objects = S3ListOperator(
                    task_id=f"list_{bucket_config['name']}",
                    bucket=bucket_config["name"],
                )
                list_tasks.append(list_objects)

                # Set dependency: create files before listing
                for file_task in file_tasks:
                    if file_task.task_id.startswith(f"create_file_{bucket_config['name']}"):
                        file_task >> list_objects

            # Add a sensor for each bucket to check if files exist
            sensor_tasks = []
            for file_config in config["files"]:
                sensor = S3KeySensor(
                    task_id=f"sense_{file_config['bucket']}_{file_config['key'].replace('/', '_')}",
                    bucket_key=f"{file_config['key']}",
                    bucket_name=file_config["bucket"],
                    poke_interval=30,  # Check every 30 seconds
                    timeout=300,  # Timeout after 5 minutes
                )
                sensor_tasks.append(sensor)

                # Set dependency: list objects before sensing
                for list_task in list_tasks:
                    if f"list_{file_config['bucket']}" == list_task.task_id:
                        list_task >> sensor

        # Add DAG to dictionary
        dags[dag_id] = dag

    return dags


# Generate all DAGs
globals().update(generate_aws_dags())
