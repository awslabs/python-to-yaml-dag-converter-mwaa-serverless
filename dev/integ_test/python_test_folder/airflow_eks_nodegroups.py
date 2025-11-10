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
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateNodegroupOperator,
    EksDeleteClusterOperator,
    EksDeleteNodegroupOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_eks_with_nodegroups"


@task
def create_launch_template(template_name: str):
    # This launch template enables IMDSv2.
    boto3.client("ec2").create_launch_template(
        LaunchTemplateName=template_name,
        LaunchTemplateData={
            "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_launch_template(template_name: str):
    boto3.client("ec2").delete_launch_template(LaunchTemplateName=template_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    role_arn = "test-role-arn"
    subnets = ["subnet-12345ab", "subnet-67890cd"]

    cluster_name = f"{env_id}-cluster"
    nodegroup_name = f"{env_id}-nodegroup"
    launch_template_name = f"{env_id}-launch-template"

    # [START howto_operator_eks_create_cluster]
    # Create an Amazon EKS Cluster control plane without attaching compute service.
    create_cluster = EksCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=cluster_name,
        cluster_role_arn=role_arn,
        resources_vpc_config={"subnetIds": subnets},
        compute=None,
    )
    # [END howto_operator_eks_create_cluster]

    # [START howto_sensor_eks_cluster]
    await_create_cluster = EksClusterStateSensor(
        task_id="await_create_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.ACTIVE,
    )
    # [END howto_sensor_eks_cluster]

    # [START howto_operator_eks_create_nodegroup]
    create_nodegroup = EksCreateNodegroupOperator(
        task_id="create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        nodegroup_subnets=subnets,
        nodegroup_role_arn=role_arn,
    )
    # [END howto_operator_eks_create_nodegroup]
    # The launch template enforces IMDSv2 and is required for internal compliance
    # when running these system tests on AWS infrastructure.  It is not required
    # for the operator to work, so I'm placing it outside the demo snippet.
    create_nodegroup.create_nodegroup_kwargs = {"launchTemplate": {"name": launch_template_name}}

    # [START howto_sensor_eks_nodegroup]
    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="await_create_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
    )
    # [END howto_sensor_eks_nodegroup]
    await_create_nodegroup.poke_interval = 10

    # [START howto_operator_eks_delete_nodegroup]
    delete_nodegroup = EksDeleteNodegroupOperator(
        task_id="delete_nodegroup",
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
    )
    # [END howto_operator_eks_delete_nodegroup]
    delete_nodegroup.trigger_rule = TriggerRule.ALL_DONE

    await_delete_nodegroup = EksNodegroupStateSensor(
        task_id="await_delete_nodegroup",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.NONEXISTENT,
    )

    # [START howto_operator_eks_delete_cluster]
    delete_cluster = EksDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=cluster_name,
    )
    # [END howto_operator_eks_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    await_delete_cluster = EksClusterStateSensor(
        task_id="await_delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        target_state=ClusterStates.NONEXISTENT,
        poke_interval=10,
    )

    chain(
        # TEST SETUP
        create_launch_template(launch_template_name),
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_nodegroup,
        await_create_nodegroup,
        # TEST TEARDOWN
        delete_nodegroup,  # part of the test AND teardown
        await_delete_nodegroup,
        delete_cluster,  # part of the test AND teardown
        await_delete_cluster,
        delete_launch_template(launch_template_name),
    )
