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

import json
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionGetExecutionOutputOperator,
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_step_functions"

STATE_MACHINE_DEFINITION = {
    "StartAt": "Wait",
    "States": {"Wait": {"Type": "Wait", "Seconds": 7, "Next": "Success"}, "Success": {"Type": "Succeed"}},
}


@task
def create_state_machine(env_id, role_arn):
    # Create a Step Functions State Machine and return the ARN for use by
    # downstream tasks.
    return (
        StepFunctionHook()
        .get_conn()
        .create_state_machine(
            name=f"{DAG_ID}_{env_id}",
            definition=json.dumps(STATE_MACHINE_DEFINITION),
            roleArn=role_arn,
        )["stateMachineArn"]
    )


@task
def delete_state_machine(state_machine_arn):
    StepFunctionHook().get_conn().delete_state_machine(stateMachineArn=state_machine_arn)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    role_arn = "test-role-arn"

    state_machine_arn = create_state_machine(env_id, role_arn)

    # [START howto_operator_step_function_start_execution]
    start_execution = StepFunctionStartExecutionOperator(task_id="start_execution", state_machine_arn=state_machine_arn)
    # [END howto_operator_step_function_start_execution]

    execution_arn = start_execution.output

    # [START howto_sensor_step_function_execution]
    wait_for_execution = StepFunctionExecutionSensor(task_id="wait_for_execution", execution_arn=execution_arn)
    # [END howto_sensor_step_function_execution]
    wait_for_execution.poke_interval = 1

    # [START howto_operator_step_function_get_execution_output]
    get_execution_output = StepFunctionGetExecutionOutputOperator(
        task_id="get_execution_output", execution_arn=execution_arn
    )
    # [END howto_operator_step_function_get_execution_output]

    chain(
        # TEST SETUP
        state_machine_arn,
        # TEST BODY
        start_execution,
        wait_for_execution,
        get_execution_output,
        # TEST TEARDOWN
        delete_state_machine(state_machine_arn),
    )
