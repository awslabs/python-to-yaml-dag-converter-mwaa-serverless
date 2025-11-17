#
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
from airflow.providers.amazon.aws.operators.rds import (
    RdsCreateDbInstanceOperator,
    RdsCreateEventSubscriptionOperator,
    RdsDeleteDbInstanceOperator,
    RdsDeleteEventSubscriptionOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_rds_event"


@task
def create_sns_topic(env_id) -> str:
    return boto3.client("sns").create_topic(Name=f"{env_id}-topic")["TopicArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_sns_topic(topic_arn) -> None:
    boto3.client("sns").delete_topic(TopicArn=topic_arn)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    rds_db_name = f"{env_id}_db"
    rds_instance_name = f"{env_id}-instance"
    rds_subscription_name = f"{env_id}-subscription"

    sns_topic = create_sns_topic(env_id)

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

    # [START howto_operator_rds_create_event_subscription]
    create_subscription = RdsCreateEventSubscriptionOperator(
        task_id="create_subscription",
        subscription_name=rds_subscription_name,
        sns_topic_arn=sns_topic,
        source_type="db-instance",
        source_ids=[rds_instance_name],
        event_categories=["availability"],
    )
    # [END howto_operator_rds_create_event_subscription]

    # [START howto_operator_rds_delete_event_subscription]
    delete_subscription = RdsDeleteEventSubscriptionOperator(
        task_id="delete_subscription",
        subscription_name=rds_subscription_name,
    )
    # [END howto_operator_rds_delete_event_subscription]
    delete_subscription.trigger_rule = TriggerRule.ALL_DONE

    delete_db_instance = RdsDeleteDbInstanceOperator(
        task_id="delete_db_instance",
        db_instance_identifier=rds_instance_name,
        rds_kwargs={"SkipFinalSnapshot": True},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        sns_topic,
        create_db_instance,
        # TEST BODY
        create_subscription,
        delete_subscription,
        # TEST TEARDOWN
        delete_db_instance,
        delete_sns_topic(sns_topic),
    )
