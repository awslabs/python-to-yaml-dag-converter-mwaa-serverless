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
    EksDeleteClusterOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksFargateProfileStateSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_eks_with_fargate_in_one_step"

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

    # [START howto_operator_eks_create_cluster_with_fargate_profile]
    # Create an Amazon EKS cluster control plane and an AWS Fargate compute platform in one step.
    create_cluster_and_fargate_profile = EksCreateClusterOperator(
        task_id="create_eks_cluster_and_fargate_profile",
        cluster_name=cluster_name,
        cluster_role_arn=cluster_role_arn,
        resources_vpc_config={
            "subnetIds": subnets,
            "endpointPublicAccess": True,
            "endpointPrivateAccess": False,
        },
        compute="fargate",
        fargate_profile_name=fargate_profile_name,
        # Opting to use the same ARN for the cluster and the pod here,
        # but a different ARN could be configured and passed if desired.
        fargate_pod_execution_role_arn=fargate_pod_role_arn,
    )
    # [END howto_operator_eks_create_cluster_with_fargate_profile]

    await_create_fargate_profile = EksFargateProfileStateSensor(
        task_id="await_create_fargate_profile",
        cluster_name=cluster_name,
        fargate_profile_name=fargate_profile_name,
        target_state=FargateProfileStates.ACTIVE,
    )

    # An Amazon EKS cluster can not be deleted with attached resources such as nodegroups or Fargate profiles.
    # Setting the `force` to `True` will delete any attached resources before deleting the cluster.
    delete_cluster_and_fargate_profile = EksDeleteClusterOperator(
        task_id="delete_fargate_profile_and_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        force_delete_compute=True,
    )

    await_delete_cluster = EksClusterStateSensor(
        task_id="await_delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
        cluster_name=cluster_name,
        target_state=ClusterStates.NONEXISTENT,
        poke_interval=10,
    )

    chain(
        # TEST BODY
        create_cluster_and_fargate_profile,
        await_create_fargate_profile,
        # TEST TEARDOWN
        delete_cluster_and_fargate_profile,
        await_delete_cluster,
    )
