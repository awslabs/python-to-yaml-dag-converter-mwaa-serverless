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
import time
from datetime import datetime

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookSensor,
)

from airflow.sdk import DAG, chain, task

"""
Prerequisites: The account which runs this test must have the following:
1. A SageMaker Unified Studio Domain (with default VPC and roles)
2. A project within the SageMaker Unified Studio Domain
3. Two notebook assets registered in the project:
   - NOTEBOOK_ID: A notebook that produces output variables (e.g., name, age)
   - NOTEBOOK_B_ID: A notebook that accepts parameters and uses them

This test calls the DataZone StartNotebookRun / GetNotebookRun APIs directly
via boto3 using standard IAM credentials. No MWAA environment emulation is performed.
"""

DAG_ID = "example_sagemaker_unified_studio_notebook"

# Externally fetched variables:
ENV_ID = "ENV_ID"
DOMAIN_ID = "DOMAIN_ID"
PROJECT_ID = "PROJECT_ID"
NOTEBOOK_ID = "NOTEBOOK_ID"
NOTEBOOK_B_ID = "NOTEBOOK_B_ID"
DATAZONE_ROLE_ARN = "DATAZONE_ROLE_ARN"

DATAZONE_CONN_ID = "aws_datazone_notebook"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    test_env_id = ENV_ID
    domain_id = DOMAIN_ID
    project_id = PROJECT_ID
    notebook_id = NOTEBOOK_ID
    notebook_b_id = NOTEBOOK_B_ID

    # [START howto_operator_sagemaker_unified_studio_notebook]
    client_token = f"idempotency-token-{int(time.time())}"

    run_notebook = SageMakerUnifiedStudioNotebookOperator(
        task_id="notebook-task",
        aws_conn_id=DATAZONE_CONN_ID,
        notebook_identifier=notebook_id,
        domain_identifier=domain_id,
        owning_project_identifier=project_id,
        client_token=client_token,  # optional
        notebook_parameters={
            "param1": "value1",
            "param2": "value2",
        },  # optional
        compute_configuration={"instanceType": "sc.m5.large"},  # optional
        timeout_configuration={"runTimeoutInMinutes": 1440},  # optional
        wait_for_completion=True,  # optional
        waiter_delay=30,  # optional
        deferrable=False,  # optional
    )
    # [END howto_operator_sagemaker_unified_studio_notebook]

    # [START howto_sensor_sagemaker_unified_studio_notebook]
    run_sensor = SageMakerUnifiedStudioNotebookSensor(
        task_id="notebook-sensor-task",
        aws_conn_id=DATAZONE_CONN_ID,
        domain_identifier=domain_id,
        owning_project_identifier=project_id,
        notebook_identifier=notebook_id,
        notebook_run_id=run_notebook.output["notebook_run_id"],
    )
    # [END howto_sensor_sagemaker_unified_studio_notebook]

    # [START howto_operator_sagemaker_unified_studio_notebook_pass_outputs]
    # Notebook A produces outputs (e.g., name, age) that are pushed to xcom.
    # Notebook B consumes those outputs via Jinja templating in notebook_parameters.
    run_notebook_a = SageMakerUnifiedStudioNotebookOperator(
        task_id="notebook-a-task",
        aws_conn_id=DATAZONE_CONN_ID,
        notebook_identifier=notebook_id,
        domain_identifier=domain_id,
        owning_project_identifier=project_id,
        wait_for_completion=True,
        deferrable=False,
    )

    run_notebook_b = SageMakerUnifiedStudioNotebookOperator(
        task_id="notebook-b-task",
        aws_conn_id=DATAZONE_CONN_ID,
        notebook_identifier=notebook_b_id,
        domain_identifier=domain_id,
        owning_project_identifier=project_id,
        notebook_parameters={
            "employee_name": "{{ task_instance.xcom_pull(task_ids='notebook-a-task', key='NOTEBOOK_OUTPUT.name') }}",
            "employee_age": "{{ task_instance.xcom_pull(task_ids='notebook-a-task', key='NOTEBOOK_OUTPUT.age') }}",
        },
        wait_for_completion=True,
        deferrable=False,
    )
    # [END howto_operator_sagemaker_unified_studio_notebook_pass_outputs]

    chain(
        run_notebook,
        run_sensor,
        run_notebook_a,
        run_notebook_b,
    )
