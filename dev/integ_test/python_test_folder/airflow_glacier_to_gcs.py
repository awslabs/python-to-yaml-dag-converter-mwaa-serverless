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

import boto3
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glacier import (
    GlacierCreateJobOperator,
    GlacierUploadArchiveOperator,
)
from airflow.providers.amazon.aws.sensors.glacier import GlacierJobOperationSensor
from airflow.providers.amazon.aws.transfers.glacier_to_gcs import GlacierToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_glacier_to_gcs"


@task
def create_vault(vault_name):
    boto3.client("glacier").create_vault(vaultName=vault_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_vault(vault_name):
    boto3.client("glacier").delete_vault(vaultName=vault_name)


with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    vault_name = f"{env_id}-vault"
    gcs_bucket_name = f"{env_id}-bucket"
    gcs_object_name = f"{env_id}-object"

    # [START howto_operator_glacier_create_job]
    create_glacier_job = GlacierCreateJobOperator(task_id="create_glacier_job", vault_name=vault_name)
    JOB_ID = '{{ task_instance.xcom_pull("create_glacier_job")["jobId"] }}'
    # [END howto_operator_glacier_create_job]

    # [START howto_sensor_glacier_job_operation]
    wait_for_operation_complete = GlacierJobOperationSensor(
        vault_name=vault_name,
        job_id=JOB_ID,
        task_id="wait_for_operation_complete",
    )
    # [END howto_sensor_glacier_job_operation]

    # [START howto_operator_glacier_upload_archive]
    upload_archive_to_glacier = GlacierUploadArchiveOperator(
        task_id="upload_data_to_glacier", vault_name=vault_name, body=b"Test Data"
    )
    # [END howto_operator_glacier_upload_archive]

    # [START howto_transfer_glacier_to_gcs]
    transfer_archive_to_gcs = GlacierToGCSOperator(
        task_id="transfer_archive_to_gcs",
        vault_name=vault_name,
        bucket_name=gcs_bucket_name,
        object_name=gcs_object_name,
        gzip=False,
        # Override to match your needs
        # If chunk size is bigger than actual file size
        # then whole file will be downloaded
        chunk_size=1024,
    )
    # [END howto_transfer_glacier_to_gcs]

    chain(
        # TEST SETUP
        create_vault(vault_name),
        # TEST BODY
        create_glacier_job,
        wait_for_operation_complete,
        upload_archive_to_glacier,
        transfer_archive_to_gcs,
        # TEST TEARDOWN
        delete_vault(vault_name),
    )
