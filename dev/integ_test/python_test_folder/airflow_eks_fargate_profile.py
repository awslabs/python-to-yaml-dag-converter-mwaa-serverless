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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, FargateProfileStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateFargateProfileOperator,
    EksDeleteClusterOperator,
    EksDeleteFargateProfileOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksFargateProfileStateSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_eks_with_fargate_profile"

SELECTORS = [{"namespace": "default"}]

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    cluster_role_arn = "test-cluster-role-arn"
    fargate_pod_role_arn = "test-fargate-pod-role-arn"
    subnets = ["subnet-12345ab", "subnet-67890cd"]

    cluster_name = f"{env_id}-cluster"
    fargate_profile_name = f"{env_id}-profile"
    test_name = f"{env_id}_{DAG_ID}"

    # Create an Amazon EKS Cluster control plane without attaching a compute service.
    create_cluster = EksCreateClusterOperator(
        task_id="create_eks_cluster",
        cluster_name=cluster_name,
        cluster_role_arn=cluster_role_arn,
        resources_vpc_config={
            "subnetIds": subnets,
            "endpointPublicAccess": True,
            "endpointPrivateAccess": False,
        },
        compute=None,
    )

    await_create_cluster = EksClusterStateSensor(
        task_id="wait_for_create_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.ACTIVE,
    )

    # [START howto_operator_eks_create_fargate_profile]
    create_fargate_profile = EksCreateFargateProfileOperator(
        task_id="create_eks_fargate_profile",
        cluster_name=cluster_name,
        pod_execution_role_arn=fargate_pod_role_arn,
        fargate_profile_name=fargate_profile_name,
        selectors=SELECTORS,
    )
    # [END howto_operator_eks_create_fargate_profile]

    # [START howto_sensor_eks_fargate]
    await_create_fargate_profile = EksFargateProfileStateSensor(
        task_id="wait_for_create_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
        target_state=FargateProfileStates.ACTIVE,
    )
    # [END howto_sensor_eks_fargate]

    # [START howto_operator_eks_delete_fargate_profile]
    delete_fargate_profile = EksDeleteFargateProfileOperator(
        task_id="delete_eks_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
    )
    # [END howto_operator_eks_delete_fargate_profile]
    delete_fargate_profile.trigger_rule = TriggerRule.ALL_DONE

    await_delete_fargate_profile = EksFargateProfileStateSensor(
        task_id="wait_for_delete_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
        target_state=FargateProfileStates.NONEXISTENT,
        trigger_rule=TriggerRule.ALL_DONE,
        poke_interval=10,
    )

    delete_cluster = EksDeleteClusterOperator(
        task_id="delete_eks_cluster",
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    await_delete_cluster = EksClusterStateSensor(
        task_id="wait_for_delete_cluster",
        cluster_name=cluster_name,
        target_state=ClusterStates.NONEXISTENT,
        poke_interval=10,
    )

    chain(
        # TEST BODY
        create_cluster,
        await_create_cluster,
        create_fargate_profile,
        await_create_fargate_profile,
        # TEARDOWN
        delete_fargate_profile,
        await_delete_fargate_profile,
        delete_cluster,
        await_delete_cluster,
    )
