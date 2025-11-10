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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_s3_to_sftp"

with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    s3_bucket = f"{env_id}-s3-to-sftp-bucket"
    s3_key = f"{env_id}-s3-to-sftp-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    # [START howto_transfer_s3_to_sftp]
    create_s3_to_sftp_job = S3ToSFTPOperator(
        task_id="create_s3_to_sftp_job",
        sftp_path="sftp_path",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
    )
    # [END howto_transfer_s3_to_sftp]

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
        create_s3_to_sftp_job,
        # TEST TEARDOWN
        delete_s3_bucket,
    )
