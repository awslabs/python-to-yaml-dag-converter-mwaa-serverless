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
from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# TODO: FIXME The argument types here seems somewhat tricky to fix
# mypy: disable-error-code="arg-type"

DAG_ID = "example_dynamodb"

PK_ATTRIBUTE_NAME = "PK"
SK_ATTRIBUTE_NAME = "SK"
TABLE_ATTRIBUTES = [
    {"AttributeName": PK_ATTRIBUTE_NAME, "AttributeType": "S"},
    {"AttributeName": SK_ATTRIBUTE_NAME, "AttributeType": "S"},
]
TABLE_KEY_SCHEMA = [
    {"AttributeName": "PK", "KeyType": "HASH"},
    {"AttributeName": "SK", "KeyType": "RANGE"},
]
TABLE_THROUGHPUT = {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10}


@task
def create_table(table_name: str):
    ddb = boto3.resource("dynamodb")
    table = ddb.create_table(
        AttributeDefinitions=TABLE_ATTRIBUTES,
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        ProvisionedThroughput=TABLE_THROUGHPUT,
    )
    boto3.client("dynamodb").get_waiter("table_exists").wait(TableName=table_name)
    table.put_item(Item={"PK": "Test", "SK": "2022-07-12T11:11:25-0400", "Value": "Testing"})


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_table(table_name: str):
    client = boto3.client("dynamodb")
    client.delete_table(TableName=table_name)
    client.get_waiter("table_not_exists").wait(TableName=table_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    table_name = f"{env_id}-dynamodb-table"
    create_table_task = create_table(table_name=table_name)
    delete_table_task = delete_table(table_name)

    # [START howto_sensor_dynamodb_value]
    dynamodb_sensor = DynamoDBValueSensor(
        task_id="waiting_for_dynamodb_item_value",
        table_name=table_name,
        partition_key_name=PK_ATTRIBUTE_NAME,
        partition_key_value="Test",
        sort_key_name=SK_ATTRIBUTE_NAME,
        sort_key_value="2022-07-12T11:11:25-0400",
        attribute_name="Value",
        attribute_value="Testing",
    )
    # [END howto_sensor_dynamodb_value]

    # [START howto_sensor_dynamodb_any_value]
    dynamodb_sensor_any_value = DynamoDBValueSensor(
        task_id="waiting_for_dynamodb_item_any_value",
        table_name=table_name,
        partition_key_name=PK_ATTRIBUTE_NAME,
        partition_key_value="Test",
        sort_key_name=SK_ATTRIBUTE_NAME,
        sort_key_value="2022-07-12T11:11:25-0400",
        attribute_name="Value",
        attribute_value=["Foo", "Testing", "Bar"],
    )
    # [END howto_sensor_dynamodb_any_value]

    chain(
        # TEST SETUP
        create_table_task,
        # TEST BODY
        dynamodb_sensor,
        dynamodb_sensor_any_value,
        # TEST TEARDOWN
        delete_table_task,
    )
