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

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import (
    SageMakerNotebookOperator,
)

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_sagemaker_unified_studio"


def get_mwaa_environment_params(
    domain_id: str,
    project_id: str,
    environment_id: str,
    s3_path: str,
    region_name: str,
):
    AIRFLOW_PREFIX = "AIRFLOW__WORKFLOWS__"

    parameters = {}
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_ID"] = domain_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_PROJECT_ID"] = project_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_ENVIRONMENT_ID"] = environment_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_SCOPE_NAME"] = "dev"
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_STAGE"] = "prod"
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_ENDPOINT"] = f"https://datazone.{region_name}.api.aws"
    parameters[f"{AIRFLOW_PREFIX}PROJECT_S3_PATH"] = s3_path
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_REGION"] = region_name
    return parameters


@task
def mock_mwaa_environment(parameters: dict):
    """
    Sets several environment variables in the container to emulate an MWAA environment provisioned
    within SageMaker Unified Studio. When running in the ECSExecutor, this is a no-op.
    """
    import os

    for key, value in parameters.items():
        os.environ[key] = value


with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    test_env_id = "test"
    domain_id = "test-domain-id"
    project_id = "test-project-id"
    environment_id = "test-environment-id"
    s3_path = "test-s3-path"
    region_name = "us-east-1"

    mock_mwaa_environment_params = get_mwaa_environment_params(
        domain_id,
        project_id,
        environment_id,
        s3_path,
        region_name,
    )

    setup_mwaa_environment = mock_mwaa_environment(mock_mwaa_environment_params)

    # [START howto_operator_sagemaker_unified_studio_notebook]
    notebook_path = (
        "test_notebook.ipynb"  # This should be the path to your .ipynb, .sqlnb, or .vetl file in your project.
    )

    run_notebook = SageMakerNotebookOperator(
        task_id="run-notebook",
        input_config={"input_path": notebook_path, "input_params": {}},
        output_config={"output_formats": ["NOTEBOOK"]},  # optional
        compute={
            "instance_type": "ml.m5.large",
            "volume_size_in_gb": 30,
        },  # optional
        termination_condition={"max_runtime_in_seconds": 600},  # optional
        tags={},  # optional
        wait_for_completion=True,  # optional
        waiter_delay=5,  # optional
        deferrable=False,  # optional
        executor_config={  # optional
            "overrides": {"containerOverrides": {"environment": mock_mwaa_environment_params}}
        },
    )
    # [END howto_operator_sagemaker_unified_studio_notebook]

    chain(
        # TEST SETUP
        setup_mwaa_environment,
        # TEST BODY
        run_notebook,
    )
