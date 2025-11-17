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
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_ecs_fargate"


@task
def create_cluster(cluster_name: str) -> None:
    """Creates an ECS cluster."""
    boto3.client("ecs").create_cluster(clusterName=cluster_name)


@task
def register_task_definition(task_name: str, container_name: str) -> str:
    """Creates a Task Definition."""
    response = boto3.client("ecs").register_task_definition(
        family=task_name,
        # CPU and Memory are required for Fargate and are set to the lowest currently allowed values.
        cpu="256",
        memory="512",
        containerDefinitions=[
            {
                "name": container_name,
                "image": "ubuntu",
                "workingDirectory": "/usr/bin",
                "entryPoint": ["sh", "-c"],
                "command": ["ls"],
            }
        ],
        requiresCompatibilities=["FARGATE"],
        networkMode="awsvpc",
    )

    return response["taskDefinition"]["taskDefinitionArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_task_definition(task_definition_arn: str) -> None:
    """Deletes the Task Definition."""
    boto3.client("ecs").deregister_task_definition(taskDefinition=task_definition_arn)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_cluster(cluster_name: str) -> None:
    """Deletes the ECS cluster."""
    boto3.client("ecs").delete_cluster(cluster=cluster_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"

    cluster_name = f"{env_id}-test-cluster"
    container_name = f"{env_id}-test-container"
    task_definition_name = f"{env_id}-test-definition"

    create_task_definition_task = register_task_definition(task_definition_name, container_name)

    # [START howto_operator_ecs]
    hello_world = EcsRunTaskOperator(
        task_id="hello_world",
        cluster=cluster_name,
        task_definition=task_definition_name,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": ["echo", "hello", "world"],
                },
            ],
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ["subnet-12345678", "subnet-87654321"],
                "securityGroups": ["sg-12345678"],
                "assignPublicIp": "ENABLED",
            },
        },
    )
    # [END howto_operator_ecs]

    # EcsRunTaskOperator waits by default, setting as False to test the Sensor below.
    hello_world.wait_for_completion = False

    # [START howto_sensor_ecs_task_state]
    # By default, EcsTaskStateSensor waits until the task has started, but the
    # demo task runs so fast that the sensor misses it.  This sensor instead
    # demonstrates how to wait until the ECS Task has completed by providing
    # the target_state and failure_states parameters.
    await_task_finish = EcsTaskStateSensor(
        task_id="await_task_finish",
        cluster=cluster_name,
        task=hello_world.output["ecs_task_arn"],
        target_state=EcsTaskStates.STOPPED,
        failure_states={EcsTaskStates.NONE},
    )
    # [END howto_sensor_ecs_task_state]

    chain(
        # TEST SETUP
        create_cluster(cluster_name),
        create_task_definition_task,
        # TEST BODY
        hello_world,
        await_task_finish,
        # TEST TEARDOWN
        delete_task_definition(create_task_definition_task),
        delete_cluster(cluster_name),
    )
