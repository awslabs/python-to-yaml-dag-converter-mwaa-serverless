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

import os
from datetime import datetime

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.operators.rds import (
    RdsCancelExportTaskOperator,
    RdsCreateDbInstanceOperator,
    RdsCreateDbSnapshotOperator,
    RdsDeleteDbInstanceOperator,
    RdsDeleteDbSnapshotOperator,
    RdsStartExportTaskOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.rds import RdsExportTaskExistenceSensor, RdsSnapshotExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_rds_export"


@task
def get_snapshot_arn(snapshot_name: str) -> str:
    result = RdsHook().conn.describe_db_snapshots(DBSnapshotIdentifier=snapshot_name)
    return result["DBSnapshots"][0]["DBSnapshotArn"]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    kms_key_id = "test-kms-key-id"
    role_arn = "test-role-arn"

    bucket_name: str = f"{env_id}-bucket"

    rds_db_name: str = f"{env_id}_db"
    rds_instance_name: str = f"{env_id}-instance"
    rds_snapshot_name: str = f"{env_id}-snapshot"
    rds_export_task_id: str = f"{env_id}-export-task"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    create_db_instance = RdsCreateDbInstanceOperator(
        task_id="create_db_instance",
        db_instance_identifier=rds_instance_name,
        db_instance_class="db.t4g.micro",
        engine="postgres",
        rds_kwargs={
            "MasterUsername": "rds_username",
            # NEVER store your production password in plaintext in a DAG like this.
            # Use Airflow Secrets or a secret manager for this in production.
            "MasterUserPassword": "rds_password",
            "AllocatedStorage": 20,
            "DBName": rds_db_name,
            "PubliclyAccessible": False,
        },
    )

    create_snapshot = RdsCreateDbSnapshotOperator(
        task_id="create_snapshot",
        db_type="instance",
        db_identifier=rds_instance_name,
        db_snapshot_identifier=rds_snapshot_name,
    )

    await_snapshot = RdsSnapshotExistenceSensor(
        task_id="snapshot_sensor",
        db_type="instance",
        db_snapshot_identifier=rds_snapshot_name,
        target_statuses=["available"],
    )

    snapshot_arn = get_snapshot_arn(rds_snapshot_name)

    # [START howto_operator_rds_start_export_task]
    start_export = RdsStartExportTaskOperator(
        task_id="start_export",
        export_task_identifier=rds_export_task_id,
        source_arn=snapshot_arn,
        s3_bucket_name=bucket_name,
        s3_prefix="rds-test",
        iam_role_arn=role_arn,
        kms_key_id=kms_key_id,
    )
    # [END howto_operator_rds_start_export_task]

    # RdsStartExportTaskOperator waits by default, setting as False to test the Sensor below.
    start_export.wait_for_completion = False

    # [START howto_operator_rds_cancel_export]
    cancel_export = RdsCancelExportTaskOperator(
        task_id="cancel_export",
        export_task_identifier=rds_export_task_id,
    )
    # [END howto_operator_rds_cancel_export]
    cancel_export.check_interval = 10
    cancel_export.max_attempts = 120

    # [START howto_sensor_rds_export_task_existence]
    export_sensor = RdsExportTaskExistenceSensor(
        task_id="export_sensor",
        export_task_identifier=rds_export_task_id,
        target_statuses=["canceled"],
    )
    # [END howto_sensor_rds_export_task_existence]

    delete_snapshot = RdsDeleteDbSnapshotOperator(
        task_id="delete_snapshot",
        db_type="instance",
        db_snapshot_identifier=rds_snapshot_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_instance_name,
        rds_kwargs={"SkipFinalSnapshot": True},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        create_bucket,
        create_db_instance,
        create_snapshot,
        await_snapshot,
        snapshot_arn,
        # TEST BODY
        start_export,
        cancel_export,
        export_sensor,
        # TEST TEARDOWN
        delete_snapshot,
        delete_bucket,
        delete_db_instance,
    )
