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

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityOperator,
    GlueDataQualityRuleSetEvaluationRunOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueDataQualityRuleSetEvaluationRunSensor
from airflow.utils.trigger_rule import TriggerRule

# Set AWS region for testing
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

DAG_ID = "example_glue_data_quality"
SAMPLE_DATA = """"Alice",20
    "Bob",25
    "Charlie",30"""
SAMPLE_FILENAME = "airflow_sample.csv"

RULE_SET = """
Rules = [
    RowCount between 2 and 8,
    IsComplete "name",
    Uniqueness "name" > 0.95,
    ColumnLength "name" between 3 and 14,
    ColumnValues "age" between 19 and 31
]
"""


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_ruleset(ruleset_name):
    hook = GlueDataQualityHook()
    hook.conn.delete_data_quality_ruleset(Name=ruleset_name)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    env_id = "test"
    role_arn = "test-role-arn"

    rule_set_name = f"{env_id}-system-test-ruleset"
    s3_bucket = f"{env_id}-glue-dq-athena-bucket"
    athena_table = f"{env_id}_test_glue_dq_table"
    athena_database = f"{env_id}_glue_dq_default"

    query_create_database = f"CREATE DATABASE IF NOT EXISTS {athena_database}"
    query_create_table = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {athena_database}.{athena_table}
            ( `name` string, `age` int )
            ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            WITH SERDEPROPERTIES ( "serialization.format" = ",", "field.delim" = "," )
            LOCATION "s3://{s3_bucket}//{athena_table}"
            TBLPROPERTIES ("has_encrypted_data"="false")
            """
    query_read_table = f"SELECT * from {athena_database}.{athena_table}"
    query_drop_table = f"DROP TABLE IF EXISTS {athena_database}.{athena_table}"
    query_drop_database = f"DROP DATABASE IF EXISTS {athena_database}"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=s3_bucket,
        s3_key=f"{athena_table}/{SAMPLE_FILENAME}",
        data=SAMPLE_DATA,
        replace=True,
    )

    create_database = AthenaOperator(
        task_id="create_database",
        query=query_create_database,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        sleep_time=1,
    )

    create_table = AthenaOperator(
        task_id="create_table",
        query=query_create_table,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        sleep_time=1,
    )

    drop_table = AthenaOperator(
        task_id="drop_table",
        query=query_drop_table,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        trigger_rule=TriggerRule.ALL_DONE,
        sleep_time=1,
    )

    drop_database = AthenaOperator(
        task_id="drop_database",
        query=query_drop_database,
        database=athena_database,
        output_location=f"s3://{s3_bucket}/",
        trigger_rule=TriggerRule.ALL_DONE,
        sleep_time=1,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Flattened task group tasks
    create_rule_set = GlueDataQualityOperator(
        task_id="create_rule_set",
        name=rule_set_name,
        ruleset=RULE_SET,
        data_quality_ruleset_kwargs={
            "TargetTable": {
                "TableName": athena_table,
                "DatabaseName": athena_database,
            }
        },
    )

    start_evaluation_run = GlueDataQualityRuleSetEvaluationRunOperator(
        task_id="start_evaluation_run",
        datasource={
            "GlueTable": {
                "TableName": athena_table,
                "DatabaseName": athena_database,
            }
        },
        role=role_arn,
        rule_set_names=[rule_set_name],
    )
    start_evaluation_run.wait_for_completion = False

    await_evaluation_run_sensor = GlueDataQualityRuleSetEvaluationRunSensor(
        task_id="await_evaluation_run_sensor",
        evaluation_run_id=start_evaluation_run.output,
    )

    chain(
        # TEST SETUP
        create_s3_bucket,
        upload_sample_data,
        create_database,
        create_table,
        # TEST BODY
        create_rule_set,
        start_evaluation_run,
        await_evaluation_run_sensor,
        # TEST TEARDOWN
        delete_ruleset(rule_set_name),
        drop_table,
        drop_database,
        delete_s3_bucket,
    )
