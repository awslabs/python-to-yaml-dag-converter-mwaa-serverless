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

from airflow import settings
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_http_to_s3"

cmd = """
#!/bin/bash

echo 'foo' > /tmp/test_file

cd /tmp

nohup python3 -m http.server 8083 > /dev/null 2>&1 &

echo $!
sleep 2
exit 0
"""


@task
def create_connection(conn_id_name: str):
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="http",
        host="localhost",
        port=8083,
    )
    session = settings.Session()
    session.add(conn)
    session.commit()


with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    conn_id_name = f"{env_id}-conn-id"
    s3_bucket = f"{env_id}-http-to-s3-bucket"
    s3_key = f"{env_id}-http-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    set_up_connection = create_connection(conn_id_name)

    start_server = BashOperator(bash_command=cmd, task_id="start_server")

    # [START howto_transfer_http_to_s3]
    http_to_s3_task = HttpToS3Operator(
        task_id="http_to_s3_task",
        http_conn_id=conn_id_name,
        endpoint="/test_file",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
    )
    # [END howto_transfer_http_to_s3]

    stop_server = BashOperator(
        task_id="stop_simple_http_server",
        bash_command='kill {{ti.xcom_pull(task_ids="start_server")}}',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        create_s3_bucket,
        set_up_connection,
        start_server,
        # TEST BODY
        http_to_s3_task,
        # TEST TEARDOWN
        stop_server,
        delete_s3_bucket,
    )
