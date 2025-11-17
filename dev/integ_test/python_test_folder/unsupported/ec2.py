from __future__ import annotations

from datetime import datetime, timedelta
from operator import itemgetter

import boto3
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2HibernateInstanceOperator,
    EC2RebootInstanceOperator,
    EC2StartInstanceOperator,
    EC2StopInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_ec2"


def sys_test_context_task():
    """Get system test context"""
    return "py2yml-test4819382"


def get_latest_ami_id():
    """Returns the AMI ID of the most recently-created Amazon Linux image"""

    # Amazon is retiring AL2 in 2023 and replacing it with Amazon Linux 2022.
    # This image prefix should be futureproof, but may need adjusting depending
    # on how they name the new images.  This page should have AL2022 info when
    # it comes available: https://aws.amazon.com/linux/amazon-linux-2022/faqs/
    image_prefix = "Amazon Linux*"
    root_device_name = "/dev/xvda"

    images = boto3.client("ec2").describe_images(
        Filters=[
            {"Name": "description", "Values": [image_prefix]},
            {
                "Name": "architecture",
                "Values": ["x86_64"],
            },  # t3 instances are only compatible with x86 architecture
            {
                "Name": "root-device-type",
                "Values": ["ebs"],
            },  # instances which are capable of hibernation need to use an EBS-backed AMI
            {"Name": "root-device-name", "Values": [root_device_name]},
        ],
        Owners=["amazon"],
    )
    # Sort on CreationDate
    return max(images["Images"], key=itemgetter("CreationDate"))["ImageId"]


def create_key_pair(key_name: str):
    client = boto3.client("ec2")

    key_pair_id = client.create_key_pair(KeyName=key_name)["KeyName"]
    # Creating the key takes a very short but measurable time, preventing race condition:
    client.get_waiter("key_pair_exists").wait(KeyNames=[key_pair_id])

    return key_pair_id


def parse_response(instance_ids: list):
    return instance_ids[0]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    default_args=default_args,
) as dag:
    env_id = PythonOperator(
        task_id="env_id",
        python_callable=sys_test_context_task,
        dag=dag,
    )
    instance_name = f"{env_id.output}-instance"
    key_name = PythonOperator(
        task_id="key_name",
        python_callable=create_key_pair,
        op_kwargs={"key_name": f"{env_id.output}_key_pair"},
        dag=dag,
    )
    image_id = PythonOperator(
        task_id="image_id",
        python_callable=get_latest_ami_id,
        dag=dag,
    )

    config = {
        "InstanceType": "t3.micro",
        "KeyName": key_name.output,
        "TagSpecifications": [{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}],
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        "HibernationOptions": {"Configured": True},
        "BlockDeviceMappings": [{"DeviceName": "/dev/xvda", "Ebs": {"Encrypted": True, "DeleteOnTermination": True}}],
    }

    # EC2CreateInstanceOperator creates and starts the EC2 instances. To test the EC2StartInstanceOperator,
    # we will stop the instance, then start them again before terminating them.

    # [START howto_operator_ec2_create_instance]
    create_instance = EC2CreateInstanceOperator(
        task_id="create_instance",
        image_id=image_id.output,
        max_count=1,
        min_count=1,
        config=config,
        wait_for_completion=True,
    )
    # [END howto_operator_ec2_create_instance]
    instance_id = PythonOperator(
        task_id="instance_id",
        python_callable=parse_response,
        op_args=[create_instance.output],
        dag=dag,
    )
    # [START howto_operator_ec2_stop_instance]
    stop_instance = EC2StopInstanceOperator(
        task_id="stop_instance",
        instance_id=instance_id.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_ec2_stop_instance]

    # [START howto_operator_ec2_start_instance]
    start_instance = EC2StartInstanceOperator(
        task_id="start_instance",
        instance_id=instance_id.output,
    )
    # [END howto_operator_ec2_start_instance]

    # [START howto_sensor_ec2_instance_state]
    await_instance = EC2InstanceStateSensor(
        task_id="await_instance",
        instance_id=instance_id.output,
        target_state="running",
    )
    # [END howto_sensor_ec2_instance_state]

    # [START howto_operator_ec2_reboot_instance]
    reboot_instance = EC2RebootInstanceOperator(
        task_id="reboot_instace",
        instance_ids=instance_id.output,
        wait_for_completion=True,
    )
    # [END howto_operator_ec2_reboot_instance]

    # [START howto_operator_ec2_hibernate_instance]
    hibernate_instance = EC2HibernateInstanceOperator(
        task_id="hibernate_instace",
        instance_ids=instance_id.output,
        wait_for_completion=True,
        poll_interval=60,
        max_attempts=40,
    )
    # [END howto_operator_ec2_hibernate_instance]

    # [START howto_operator_ec2_terminate_instance]
    terminate_instance = EC2TerminateInstanceOperator(
        task_id="terminate_instance",
        instance_ids=instance_id.output,
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_ec2_terminate_instance]

    chain(
        # TEST SETUP
        env_id,
        key_name,
        image_id,
        # TEST BODY
        create_instance,
        instance_id,
        stop_instance,
        start_instance,
        await_instance,
        reboot_instance,
        hibernate_instance,
        terminate_instance,
    )
