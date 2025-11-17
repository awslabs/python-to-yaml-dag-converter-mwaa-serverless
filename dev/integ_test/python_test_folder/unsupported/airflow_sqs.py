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
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.operators.sqs import SqsPublishOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_sqs"


@task
def create_queue(queue_name) -> str:
    return SqsHook().create_queue(queue_name=queue_name)["QueueUrl"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_queue(queue_url):
    SqsHook().conn.delete_queue(QueueUrl=queue_url)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    sns_queue_name = f"{env_id}-example-queue"

    sqs_queue = create_queue(sns_queue_name)

    # [START howto_operator_sqs]
    publish_to_queue_1 = SqsPublishOperator(
        task_id="publish_to_queue_1",
        sqs_queue=sqs_queue,
        message_content="{{ task_instance }}",
    )
    publish_to_queue_2 = SqsPublishOperator(
        task_id="publish_to_queue_2",
        sqs_queue=sqs_queue,
        message_content="{{ task_instance }}",
    )
    # [END howto_operator_sqs]

    # [START howto_sensor_sqs]
    read_from_queue = SqsSensor(
        task_id="read_from_queue",
        sqs_queue=sqs_queue,
    )
    # Retrieve multiple batches of messages from SQS.
    # The SQS API only returns a maximum of 10 messages per poll.
    read_from_queue_in_batch = SqsSensor(
        task_id="read_from_queue_in_batch",
        sqs_queue=sqs_queue,
        # Get maximum 10 messages each poll
        max_messages=10,
        # Combine 3 polls before returning results
        num_batches=3,
    )
    # [END howto_sensor_sqs]

    chain(
        # TEST SETUP
        sqs_queue,
        # TEST BODY
        publish_to_queue_1,
        read_from_queue,
        publish_to_queue_2,
        read_from_queue_in_batch,
        # TEST TEARDOWN
        delete_queue(sqs_queue),
    )
