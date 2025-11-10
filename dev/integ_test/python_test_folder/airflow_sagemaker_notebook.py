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
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerCreateNotebookOperator,
    SageMakerDeleteNotebookOperator,
    SageMakerStartNoteBookOperator,
    SageMakerStopNotebookOperator,
)

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_sagemaker_notebook"

with DAG(
    DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    instance_name: str = f"{env_id}-test-notebook"
    role_arn = "test-role-arn"

    # [START howto_operator_sagemaker_notebook_create]
    instance = SageMakerCreateNotebookOperator(
        task_id="create_instance",
        instance_name=instance_name,
        instance_type="ml.t3.medium",
        role_arn=role_arn,
        wait_for_completion=True,
    )
    # [END howto_operator_sagemaker_notebook_create]

    # [START howto_operator_sagemaker_notebook_stop]
    stop_instance = SageMakerStopNotebookOperator(
        task_id="stop_instance",
        instance_name=instance_name,
    )
    # [END howto_operator_sagemaker_notebook_stop]

    # [START howto_operator_sagemaker_notebook_start]
    start_instance = SageMakerStartNoteBookOperator(
        task_id="start_instance",
        instance_name=instance_name,
    )

    # [END howto_operator_sagemaker_notebook_start]

    # Instance must be stopped before it can be deleted.
    stop_instance_before_delete = SageMakerStopNotebookOperator(
        task_id="stop_instance_before_delete",
        instance_name=instance_name,
    )
    # [START howto_operator_sagemaker_notebook_delete]
    delete_instance = SageMakerDeleteNotebookOperator(task_id="delete_instance", instance_name=instance_name)
    # [END howto_operator_sagemaker_notebook_delete]

    chain(
        # create a new instance
        instance,
        # stop the instance
        stop_instance,
        # restart the instance
        start_instance,
        # must stop before deleting
        stop_instance_before_delete,
        # delete the instance
        delete_instance,
    )
