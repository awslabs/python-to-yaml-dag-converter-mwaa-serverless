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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.sensors.glue_catalog_partition import GlueCatalogPartitionSensor
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_glue"
# Externally fetched variables:
# Role needs S3 putobject/getobject access as well as the glue service role,
# see docs here: https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
ROLE_ARN_KEY = "ROLE_ARN"
# Example csv data used as input to the example AWS Glue Job.
EXAMPLE_CSV = """product,value
apple,0.5
milk,2.5
bread,4.0
"""
# Example Spark script to operate on the above sample csv data.
EXAMPLE_SCRIPT = """
from pyspark.context import SparkContext
from awsglue.context import GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
datasource = glueContext.create_dynamic_frame.from_catalog(
             database='{db_name}', table_name='input')
print('There are %s items in the table' % datasource.count())
datasource.toDF().write.format('csv').mode("append").save('s3://{bucket_name}/output')
"""

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    default_args={"start_date": "2026-07-15"},
) as dag:
    env_id = "py2yml-unique-id-112413"
    role_arn = "arn:aws:iam::590183871800:role/WorkflowExecutionRole"
    glue_crawler_name = f"{env_id}_crawler"
    glue_db_name = f"{env_id}_glue_db"
    glue_job_name = f"{env_id}_glue_job"
    bucket_name = f"{env_id}-bucket"
    role_name = "WorkflowExecutionRole"
    glue_crawler_config = {
        "Name": glue_crawler_name,
        "Role": role_arn,
        "DatabaseName": glue_db_name,
        "Targets": {"S3Targets": [{"Path": f"{bucket_name}/input"}]},
    }
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )
    upload_csv = S3CreateObjectOperator(
        task_id="upload_csv",
        s3_bucket=bucket_name,
        s3_key="input/category=mixed/input.csv",
        data=EXAMPLE_CSV,
        replace=True,
    )
    upload_script = S3CreateObjectOperator(
        task_id="upload_script",
        s3_bucket=bucket_name,
        s3_key="etl_script.py",
        data=EXAMPLE_SCRIPT.format(db_name=glue_db_name, bucket_name=bucket_name),
        replace=True,
    )
    # [START howto_operator_glue_crawler]
    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
        wait_for_completion=False,
    )
    # [END howto_operator_glue_crawler]
    # [START howto_sensor_glue_crawler]
    wait_for_crawl = GlueCrawlerSensor(
        task_id="wait_for_crawl",
        crawler_name=glue_crawler_name,
        timeout=500,
    )
    # [END howto_sensor_glue_crawler]
    # [START howto_sensor_glue_catalog_partition]
    wait_for_catalog_partition = GlueCatalogPartitionSensor(
        task_id="wait_for_catalog_partition",
        table_name="input",
        database_name=glue_db_name,
        expression="category='mixed'",
    )
    # [END howto_sensor_glue_catalog_partition]
    # [START howto_operator_glue]
    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        job_name=glue_job_name,
        script_location=f"s3://{bucket_name}/etl_script.py",
        s3_bucket=bucket_name,
        iam_role_name=role_name,
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
        wait_for_completion=False,
    )
    # [END howto_operator_glue]
    # [START howto_sensor_glue]
    wait_for_job = GlueJobSensor(
        task_id="wait_for_job",
        job_name=glue_job_name,
        # Job ID extracted from previous Glue Job Operator task
        run_id=submit_glue_job.output,
        verbose=True,  # prints glue job logs in airflow logs
        poke_interval=5,
    )
    # [END howto_sensor_glue]
    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )
    chain(
        # TEST SETUP
        create_bucket,
        upload_csv,
        upload_script,
        # TEST BODY
        crawl_s3,
        wait_for_crawl,
        wait_for_catalog_partition,
        submit_glue_job,
        wait_for_job,
        # TEST TEARDOWN
        delete_bucket,
    )
