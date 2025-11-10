#
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
from airflow.providers.amazon.aws.operators.emr import (
    EmrStartNotebookExecutionOperator,
    EmrStopNotebookExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrNotebookExecutionSensor

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_emr_notebook"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    editor_id = "test-editor-id"
    cluster_id = "test-cluster-id"

    # [START howto_operator_emr_start_notebook_execution]
    start_execution = EmrStartNotebookExecutionOperator(
        task_id="start_execution",
        editor_id=editor_id,
        cluster_id=cluster_id,
        relative_path="EMR-System-Test.ipynb",
        service_role="EMR_Notebooks_DefaultRole",
    )
    # [END howto_operator_emr_start_notebook_execution]

    notebook_execution_id_1 = start_execution.output

    # [START howto_sensor_emr_notebook_execution]
    wait_for_execution_start = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_start",
        notebook_execution_id=notebook_execution_id_1,
        target_states={"RUNNING"},
        poke_interval=5,
    )
    # [END howto_sensor_emr_notebook_execution]

    # [START howto_operator_emr_stop_notebook_execution]
    stop_execution = EmrStopNotebookExecutionOperator(
        task_id="stop_execution",
        notebook_execution_id=notebook_execution_id_1,
    )
    # [END howto_operator_emr_stop_notebook_execution]

    wait_for_execution_stop = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_stop",
        notebook_execution_id=notebook_execution_id_1,
        target_states={"STOPPED"},
        poke_interval=5,
    )
    finish_execution = EmrStartNotebookExecutionOperator(
        task_id="finish_execution",
        editor_id=editor_id,
        cluster_id=cluster_id,
        relative_path="EMR-System-Test.ipynb",
        service_role="EMR_Notebooks_DefaultRole",
    )
    notebook_execution_id_2 = finish_execution.output
    wait_for_execution_finish = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_finish",
        notebook_execution_id=notebook_execution_id_2,
        poke_interval=5,
    )

    chain(
        # TEST BODY
        start_execution,
        wait_for_execution_start,
        stop_execution,
        wait_for_execution_stop,
        finish_execution,
        # TEST TEARDOWN
        wait_for_execution_finish,
    )
