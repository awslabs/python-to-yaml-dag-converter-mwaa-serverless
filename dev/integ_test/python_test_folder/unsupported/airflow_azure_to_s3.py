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

from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.azure_blob_to_s3 import AzureBlobStorageToS3Operator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_azure_blob_to_s3"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    s3_bucket = f"{env_id}-azure_blob-to-s3-bucket"
    s3_key = f"{env_id}-azure_blob-to-s3-key"
    s3_key_url = f"s3://{s3_bucket}/{s3_key}"
    azure_container_name = f"{env_id}-azure_blob-to-s3-container"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    # [START howto_transfer_azure_blob_to_s3]
    azure_blob_to_s3 = AzureBlobStorageToS3Operator(
        task_id="azure_blob_to_s3",
        container_name=azure_container_name,
        dest_s3_key=s3_key_url,
    )
    # [END howto_transfer_azure_blob_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        create_s3_bucket,
        # TEST BODY
        azure_blob_to_s3,
        # TEST TEARDOWN
        delete_s3_bucket,
    )
