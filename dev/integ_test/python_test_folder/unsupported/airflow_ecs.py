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
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates
from airflow.providers.amazon.aws.operators.ecs import (
    EcsCreateClusterOperator,
    EcsDeleteClusterOperator,
    EcsDeregisterTaskDefinitionOperator,
    EcsRegisterTaskDefinitionOperator,
    EcsRunTaskOperator,
)
from airflow.providers.amazon.aws.sensors.ecs import (
    EcsClusterStateSensor,
    EcsTaskDefinitionStateSensor,
)
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_ecs"


@task
def get_region():
    return boto3.session.Session().region_name


@task(trigger_rule=TriggerRule.ALL_DONE)
def clean_logs(group_name: str):
    client = boto3.client("logs")
    client.delete_log_group(logGroupName=group_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    existing_cluster_name = "test-cluster"
    existing_cluster_subnets = ["subnet-12345678", "subnet-87654321"]

    new_cluster_name = f"{env_id}-cluster"
    container_name = f"{env_id}-container"
    family_name = f"{env_id}-task-definition"
    asg_name = f"{env_id}-asg"

    aws_region_task = get_region()
    log_group_name = f"/ecs_test/{env_id}"

    # [START howto_operator_ecs_create_cluster]
    create_cluster = EcsCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_operator_ecs_create_cluster]

    # EcsCreateClusterOperator waits by default, setting as False to test the Sensor below.
    create_cluster.wait_for_completion = False

    # [START howto_sensor_ecs_cluster_state]
    await_cluster = EcsClusterStateSensor(
        task_id="await_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_sensor_ecs_cluster_state]

    # [START howto_operator_ecs_register_task_definition]
    register_task = EcsRegisterTaskDefinitionOperator(
        task_id="register_task",
        family=family_name,
        container_definitions=[
            {
                "name": container_name,
                "image": "ubuntu",
                "workingDirectory": "/usr/bin",
                "entryPoint": ["sh", "-c"],
                "command": ["ls"],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": log_group_name,
                        "awslogs-region": aws_region_task,
                        "awslogs-create-group": "true",
                        "awslogs-stream-prefix": "ecs",
                    },
                },
            },
        ],
        register_task_kwargs={
            "cpu": "256",
            "memory": "512",
            "networkMode": "awsvpc",
        },
    )
    # [END howto_operator_ecs_register_task_definition]

    # [START howto_sensor_ecs_task_definition_state]
    await_task_definition = EcsTaskDefinitionStateSensor(
        task_id="await_task_definition",
        task_definition=register_task.output,
    )
    # [END howto_sensor_ecs_task_definition_state]

    # [START howto_operator_ecs_run_task]
    run_task = EcsRunTaskOperator(
        task_id="run_task",
        cluster=existing_cluster_name,
        task_definition=register_task.output,
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": ["echo hello world"],
                },
            ],
        },
        network_configuration={"awsvpcConfiguration": {"subnets": existing_cluster_subnets}},
        # [START howto_awslogs_ecs]
        awslogs_group=log_group_name,
        awslogs_region=aws_region_task,
        awslogs_stream_prefix=f"ecs/{container_name}",
        # [END howto_awslogs_ecs]
    )
    # [END howto_operator_ecs_run_task]

    # [START howto_operator_ecs_deregister_task_definition]
    deregister_task = EcsDeregisterTaskDefinitionOperator(
        task_id="deregister_task",
        task_definition=register_task.output,
    )
    # [END howto_operator_ecs_deregister_task_definition]
    deregister_task.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_ecs_delete_cluster]
    delete_cluster = EcsDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=new_cluster_name,
    )
    # [END howto_operator_ecs_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    # EcsDeleteClusterOperator waits by default, setting as False to test the Sensor below.
    delete_cluster.wait_for_completion = False

    # [START howto_operator_ecs_delete_cluster]
    await_delete_cluster = EcsClusterStateSensor(
        task_id="await_delete_cluster",
        cluster_name=new_cluster_name,
        target_state=EcsClusterStates.INACTIVE,
    )
    # [END howto_operator_ecs_delete_cluster]

    chain(
        # TEST SETUP
        aws_region_task,
        # TEST BODY
        create_cluster,
        await_cluster,
        register_task,
        await_task_definition,
        run_task,
        deregister_task,
        delete_cluster,
        await_delete_cluster,
        clean_logs(log_group_name),
    )
