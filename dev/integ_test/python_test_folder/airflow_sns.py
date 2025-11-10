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
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_sns"


@task
def create_topic(topic_name) -> str:
    return boto3.client("sns").create_topic(Name=topic_name)["TopicArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_topic(topic_arn) -> None:
    boto3.client("sns").delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    sns_topic_name = f"{env_id}-test-topic"

    create_sns_topic = create_topic(sns_topic_name)

    # [START howto_operator_sns_publish_operator]
    publish_message = SnsPublishOperator(
        task_id="publish_message",
        target_arn=create_sns_topic,
        message="This is a sample message sent to SNS via an Apache Airflow DAG task.",
    )
    # [END howto_operator_sns_publish_operator]

    chain(
        # TEST SETUP
        create_sns_topic,
        # TEST BODY
        publish_message,
        # TEST TEARDOWN
        delete_topic(create_sns_topic),
    )
