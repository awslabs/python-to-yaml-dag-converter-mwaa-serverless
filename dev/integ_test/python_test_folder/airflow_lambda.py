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
import zipfile
from datetime import datetime
from io import BytesIO

import boto3
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaCreateFunctionOperator,
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_lambda"

CODE_CONTENT = """
def test(*args):
    print('Hello')
"""


# Create a zip file containing one file "lambda_function.py" to deploy to the lambda function
def create_zip(content: str):
    with BytesIO() as zip_output:
        with zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED) as zip_file:
            info = zipfile.ZipInfo("lambda_function.py")
            info.external_attr = 0o777 << 16
            zip_file.writestr(info, content)
        zip_output.seek(0)
        return zip_output.read()


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_lambda(function_name: str):
    client = boto3.client("lambda")
    client.delete_function(
        FunctionName=function_name,
    )


with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    lambda_function_name: str = f"{env_id}-function"
    role_arn = "test-role-arn"

    # [START howto_operator_create_lambda_function]
    create_lambda_function = LambdaCreateFunctionOperator(
        task_id="create_lambda_function",
        function_name=lambda_function_name,
        runtime="python3.9",
        role=role_arn,
        handler="lambda_function.test",
        code={
            "ZipFile": create_zip(CODE_CONTENT),
        },
    )
    # [END howto_operator_create_lambda_function]

    # [START howto_sensor_lambda_function_state]
    wait_lambda_function_state = LambdaFunctionStateSensor(
        task_id="wait_lambda_function_state",
        function_name=lambda_function_name,
    )
    # [END howto_sensor_lambda_function_state]
    wait_lambda_function_state.poke_interval = 1

    # [START howto_operator_invoke_lambda_function]
    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name=lambda_function_name,
        payload=json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}}),
    )
    # [END howto_operator_invoke_lambda_function]

    chain(
        # TEST BODY
        create_lambda_function,
        wait_lambda_function_state,
        invoke_lambda_function,
        # TEST TEARDOWN
        delete_lambda(lambda_function_name),
    )
