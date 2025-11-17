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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from airflow.utils.timezone import datetime
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_mongo_to_s3"

with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    mongo_database = "test-mongo-database"
    mongo_collection = "test-mongo-collection"

    s3_bucket = f"{env_id}-mongo-to-s3-bucket"
    s3_key = f"{env_id}-mongo-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    # [START howto_transfer_mongo_to_s3]
    mongo_to_s3_job = MongoToS3Operator(
        task_id="mongo_to_s3_job",
        mongo_collection=mongo_collection,
        # Mongo query by matching values
        # Here returns all documents which have "OK" as value for the key "status"
        mongo_query={"status": "OK"},
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        mongo_db=mongo_database,
        replace=True,
    )
    # [END howto_transfer_mongo_to_s3]

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
        mongo_to_s3_job,
        # TEST TEARDOWN
        delete_s3_bucket,
    )
