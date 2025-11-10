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
"""
This DAG will not work unless you create an Amazon EMR cluster running
Apache Hive and copy data into it following steps 1-4 (inclusive) here:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EMRforDynamoDB.Tutorial.html
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.transfers.hive_to_dynamodb import HiveToDynamoDBOperator
from airflow.utils import db
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_hive_to_dynamodb"

# These values assume you set up the Hive data source following the link above.
DYNAMODB_TABLE_HASH_KEY = "feature_id"
HIVE_SQL = "SELECT feature_id, feature_name, feature_class, state_alpha FROM hive_features"


@task
def create_dynamodb_table(table_name):
    client = DynamoDBHook(client_type="dynamodb").conn
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": DYNAMODB_TABLE_HASH_KEY, "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": DYNAMODB_TABLE_HASH_KEY, "AttributeType": "N"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 20, "WriteCapacityUnits": 20},
    )

    # DynamoDB table creation is nearly, but not quite, instantaneous.
    # Wait for the table to be active to avoid race conditions writing to it.
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name, WaiterConfig={"Delay": 1})


@task
def get_dynamodb_item_count(table_name):
    """
    A DynamoDB table has an ItemCount value, but it is only updated every six hours.
    To verify this DAG worked, we will scan the table and count the items manually.
    """
    table = DynamoDBHook(resource_type="dynamodb").conn.Table(table_name)

    response = table.scan(Select="COUNT")
    item_count = response["Count"]

    while "LastEvaluatedKey" in response:
        response = table.scan(Select="COUNT", ExclusiveStartKey=response["LastEvaluatedKey"])
        item_count += response["Count"]

    print(f"DynamoDB table contains {item_count} items.")


# Included for sample purposes only; in production you wouldn't delete
# the table you just backed your data up to.  Using 'all_done' so even
# if an intermediate step fails, the DAG will clean up after itself.
@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dynamodb_table(table_name):
    DynamoDBHook(client_type="dynamodb").conn.delete_table(TableName=table_name)


# Included for sample purposes only; in production this should
# be configured in the environment and not be part of the DAG.
# Note: The 'hiveserver2_default' connection will not work if Hive
# is hosted on EMR.  You must set the host name of the connection
# to match your EMR cluster's hostname.
@task
def configure_hive_connection(connection_id, hostname):
    db.merge_conn(
        Connection(
            conn_id=connection_id,
            conn_type="hiveserver2",
            host=hostname,
            port=10000,
        )
    )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    dynamodb_table_name = f"{env_id}-hive_to_dynamo"
    hive_connection_id = "test-hive-connection-id"
    hive_hostname = "test-hive-hostname"

    # [START howto_transfer_hive_to_dynamodb]
    backup_to_dynamodb = HiveToDynamoDBOperator(
        task_id="backup_to_dynamodb",
        hiveserver2_conn_id=hive_connection_id,
        sql=HIVE_SQL,
        table_name=dynamodb_table_name,
        table_keys=[DYNAMODB_TABLE_HASH_KEY],
    )
    # [END howto_transfer_hive_to_dynamodb]

    chain(
        # TEST SETUP
        configure_hive_connection(hive_connection_id, hive_hostname),
        create_dynamodb_table(dynamodb_table_name),
        # TEST BODY
        backup_to_dynamodb,
        get_dynamodb_item_count(dynamodb_table_name),
        # TEST TEARDOWN
        delete_dynamodb_table(dynamodb_table_name),
    )
