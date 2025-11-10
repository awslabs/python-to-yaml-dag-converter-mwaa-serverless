# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import boto3
import tenacity
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from tenacity import before_log, before_sleep_log

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

log = logging.getLogger(__name__)

DAG_ID = "example_dynamodb_to_s3"

TABLE_ATTRIBUTES = [
    {"AttributeName": "ID", "AttributeType": "S"},
    {"AttributeName": "Value", "AttributeType": "S"},
]
TABLE_KEY_SCHEMA = [
    {"AttributeName": "ID", "KeyType": "HASH"},
    {"AttributeName": "Value", "KeyType": "RANGE"},
]
TABLE_THROUGHPUT = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
S3_KEY_PREFIX = "dynamodb-segmented-file"


# UpdateContinuousBackups API might need multiple attempts to succeed
# Sometimes the API returns the error "Backups are being enabled for the table: <...>. Please retry later"
# Using a retry strategy with exponential backoff to remediate that
@tenacity.retry(
    stop=tenacity.stop_after_attempt(20),
    wait=tenacity.wait_exponential(min=5),
    before=before_log(log, logging.INFO),
    before_sleep=before_sleep_log(log, logging.WARNING),
)
def enable_point_in_time_recovery(table_name: str):
    boto3.client("dynamodb").update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={
            "PointInTimeRecoveryEnabled": True,
        },
    )


@task
def set_up_table(table_name: str):
    dynamo_resource = boto3.resource("dynamodb")
    table = dynamo_resource.create_table(
        AttributeDefinitions=TABLE_ATTRIBUTES,
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        ProvisionedThroughput=TABLE_THROUGHPUT,
    )
    boto3.client("dynamodb").get_waiter("table_exists").wait(
        TableName=table_name, WaiterConfig={"Delay": 10, "MaxAttempts": 10}
    )
    enable_point_in_time_recovery(table_name)
    table.put_item(Item={"ID": "123", "Value": "Testing"})


@task
def get_export_time(table_name: str):
    r = boto3.client("dynamodb").describe_continuous_backups(
        TableName=table_name,
    )

    return r["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]["EarliestRestorableDateTime"]


@task
def wait_for_bucket(s3_bucket_name):
    waiter = boto3.client("s3").get_waiter("bucket_exists")
    waiter.wait(Bucket=s3_bucket_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dynamodb_table(table_name: str):
    boto3.resource("dynamodb").Table(table_name).delete()
    boto3.client("dynamodb").get_waiter("table_not_exists").wait(
        TableName=table_name, WaiterConfig={"Delay": 10, "MaxAttempts": 10}
    )


@task
def get_latest_export_time(table_name: str):
    r = boto3.client("dynamodb").describe_continuous_backups(
        TableName=table_name,
    )

    return r["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]["LatestRestorableDateTime"]


@task.short_circuit()
def should_run_incremental_export(start_time: datetime, end_time: datetime):
    return end_time >= (start_time + timedelta(minutes=15))


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    table_name = f"{env_id}-dynamodb-table"
    bucket_name = f"{env_id}-dynamodb-bucket"

    create_table_task = set_up_table(table_name=table_name)

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    # [START howto_transfer_dynamodb_to_s3]
    backup_db = DynamoDBToS3Operator(
        task_id="backup_db",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=20,
    )
    # [END howto_transfer_dynamodb_to_s3]

    # [START howto_transfer_dynamodb_to_s3_segmented]
    # Segmenting allows the transfer to be parallelized into {segment} number of parallel tasks.
    backup_db_segment_1 = DynamoDBToS3Operator(
        task_id="backup_db_segment_1",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        s3_key_prefix=f"{S3_KEY_PREFIX}-1-",
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 0,
        },
    )

    backup_db_segment_2 = DynamoDBToS3Operator(
        task_id="backup_db_segment_2",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        # Max output file size in bytes.  If the Table is too large, multiple files will be created.
        file_size=1000,
        s3_key_prefix=f"{S3_KEY_PREFIX}-2-",
        dynamodb_scan_kwargs={
            "TotalSegments": 2,
            "Segment": 1,
        },
    )
    # [END howto_transfer_dynamodb_to_s3_segmented]

    export_time = get_export_time(table_name)
    # [START howto_transfer_dynamodb_to_s3_in_some_point_in_time_full_export]
    backup_db_to_point_in_time_full_export = DynamoDBToS3Operator(
        task_id="backup_db_to_point_in_time_full_export",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        point_in_time_export=True,
        export_time=export_time,
        s3_key_prefix=f"{S3_KEY_PREFIX}-3-",
    )
    # [END howto_transfer_dynamodb_to_s3_in_some_point_in_time_full_export]
    backup_db_to_point_in_time_full_export.max_attempts = 90

    delete_table_task = delete_dynamodb_table(table_name=table_name)

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        trigger_rule=TriggerRule.ALL_DONE,
        force_delete=True,
    )

    # Flattened incremental export tasks
    end_time_task = get_latest_export_time(table_name)

    # [START howto_transfer_dynamodb_to_s3_in_some_point_in_time_incremental_export]
    backup_db_to_point_in_time_incremental_export = DynamoDBToS3Operator(
        task_id="backup_db_to_point_in_time_incremental_export",
        dynamodb_table_name=table_name,
        s3_bucket_name=bucket_name,
        point_in_time_export=True,
        s3_key_prefix=f"{S3_KEY_PREFIX}-4-",
        export_table_to_point_in_time_kwargs={
            "ExportType": "INCREMENTAL_EXPORT",
            "IncrementalExportSpecification": {
                "ExportFromTime": export_time,
                "ExportToTime": end_time_task,
                "ExportViewType": "NEW_AND_OLD_IMAGES",
            },
        },
    )
    # [END howto_transfer_dynamodb_to_s3_in_some_point_in_time_incremental_export]
    backup_db_to_point_in_time_incremental_export.max_attempts = 90

    should_run_incremental_task = should_run_incremental_export(start_time=export_time, end_time=end_time_task)

    chain(
        # TEST SETUP
        create_table_task,
        create_bucket,
        wait_for_bucket(s3_bucket_name=bucket_name),
        # TEST BODY
        backup_db,
        backup_db_segment_1,
        backup_db_segment_2,
        export_time,
        backup_db_to_point_in_time_full_export,
        end_time_task,
        should_run_incremental_task,
        backup_db_to_point_in_time_incremental_export,
        # TEST TEARDOWN
        delete_table_task,
        delete_bucket,
    )
