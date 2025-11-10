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
from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.mwaa import MwaaTriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.mwaa import MwaaDagRunSensor

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_mwaa"


@task
def unpause_dag(env_name: str, dag_id: str):
    mwaa_hook = MwaaHook()
    response = mwaa_hook.invoke_rest_api(
        env_name=env_name, path=f"/dags/{dag_id}", method="PATCH", body={"is_paused": False}
    )
    return not response["RestApiResponse"]["is_paused"]


# This task in the system test verifies that the MwaaHook's IAM fallback mechanism continues to work with
# the live MWAA API. This fallback depends on parsing a specific error message from the MWAA API, so we
# want to ensure we find out if the API response format ever changes. Unit tests cover this with mocked
# responses, but this system test validates against the real API.
@task
def test_iam_fallback(role_to_assume_arn, mwaa_env_name):
    assumed_role = StsHook().conn.assume_role(RoleArn=role_to_assume_arn, RoleSessionName="MwaaSysTestIamFallback")

    credentials = assumed_role["Credentials"]
    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    mwaa_hook = MwaaHook()
    mwaa_hook.conn = session.client("mwaa")
    response = mwaa_hook.invoke_rest_api(env_name=mwaa_env_name, path="/dags", method="GET")
    return "dags" in response["RestApiResponse"]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_name = "test-environment-name"
    trigger_dag_id = "test-dag-id"
    restricted_role_arn = "test-restricted-role-arn"

    # [START howto_operator_mwaa_trigger_dag_run]
    trigger_dag_run = MwaaTriggerDagRunOperator(
        task_id="trigger_dag_run",
        env_name=env_name,
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=True,
    )
    # [END howto_operator_mwaa_trigger_dag_run]

    # [START howto_sensor_mwaa_dag_run]
    wait_for_dag_run = MwaaDagRunSensor(
        task_id="wait_for_dag_run",
        external_env_name=env_name,
        external_dag_id=trigger_dag_id,
        external_dag_run_id="{{ task_instance.xcom_pull(task_ids='trigger_dag_run')['RestApiResponse']['dag_run_id'] }}",
        poke_interval=5,
    )
    # [END howto_sensor_mwaa_dag_run]

    chain(
        # TEST BODY
        unpause_dag(env_name, trigger_dag_id),
        trigger_dag_run,
        wait_for_dag_run,
        test_iam_fallback(restricted_role_arn, env_name),
    )
