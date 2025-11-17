import importlib
import os
import time
from datetime import datetime, timedelta, timezone
from importlib.metadata import version

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

"""
This DAG tests the constraint forcing feature of the MWAA. Constraint forcing should only apply for versions after
2.7.2.
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": "2026-01-01",
}

constraint_log_line = "WARNING: Constraints should be specified for requirements.txt."
ATTEMPTS_FOR_LOG_QUERY = 6

normal_packages = ["boto3"]

provider_package_map = {
    "airflow.providers.mysql": "apache-airflow-providers-mysql",
    "airflow.providers.amazon": "apache-airflow-providers-amazon",
    "airflow.providers.google": "apache-airflow-providers-google",
    "airflow.providers.snowflake": "apache-airflow-providers-snowflake",
    "airflow.providers.databricks": "apache-airflow-providers-databricks",
    "airflow.providers.ssh": "apache-airflow-providers-ssh",
    "airflow.providers.postgres": "apache-airflow-providers-postgres",
}

version_mismatches = []


# Returns True if any log in the given group matches
# the pattern provided
def matching_log_event_exists(client, log_group_name, pattern, minutes_ago=60):
    print("Checking for logs in group: " + log_group_name)
    query = f"""\
fields @message
| filter @message like {pattern}\
    """
    now = datetime.now(timezone.utc)
    minutes_ago_in_seconds = (now - timedelta(minutes=minutes_ago)).timestamp()

    start_response = client.start_query(
        logGroupName=log_group_name,
        queryString=query,
        startTime=int(minutes_ago_in_seconds),
        endTime=int(now.timestamp()),
        limit=20,
    )

    if "queryId" not in start_response:
        raise RuntimeError(f"Log insights query was unsuccessful for log group {log_group_name} and query:\n{query}")
    results = get_log_query_result(client, start_response["queryId"], ATTEMPTS_FOR_LOG_QUERY)
    if results:
        return True
    print("The query has completed, but no logs matched the query.")
    return False


"""
Attempts to retrive query results for the provided query ID. If query is in transcient state "SCHEDULED" or "RUNNING"
function will wait with backoff strategy backoff_i = backoff_(i-1) * 2. Total number of requests equals
to number of attempts supplied.
"""


def get_log_query_result(client, query_id, attempts, backoff=1):
    response = client.get_query_results(queryId=query_id)
    status = response["status"]

    if status == "Scheduled" or status == "Running":
        attempts -= 1
        if attempts <= 0:
            raise TimeoutError(f"Log insights query {query_id} timed-out. No more retries.")
        print(f"Query has not completed. Sleeping for {backoff} seconds. Remaining attempts: {attempts}")
        time.sleep(backoff)
        return get_log_query_result(client, query_id, attempts, backoff * 2)
    if status == "Failed" or status == "Cancelled" or status == "Unknown":
        raise RuntimeError(f"Log insights query {query_id} reached undesirable terminal state: {status}")
    if status == "Complete":
        return response["results"]


def does_constraint_warning_exist():
    worker_log_group_name = os.environ.get("AIRFLOW_WORKER_LOG_GROUP_ARN").strip().rsplit(":", 1)[1]
    client = boto3.client("logs")

    return matching_log_event_exists(client, worker_log_group_name, f'"{constraint_log_line}"')


def check_version_mismatch(name, v1, v2):
    if v1 != v2:
        version_mismatches.append(f"Expected version of {name} to be {v2}, but is actually {v1}\n")


def get_constraints_map():
    constraint_map = {}
    default_constraints_path = "/constraints.txt"
    with open(default_constraints_path) as f:
        for line in f:
            if "==" in line:
                s = line.strip().split("==")
                constraint_map[s[0]] = s[1]

    return constraint_map


def check_imports(**kwargs):
    print("Checking for constraint warning.")
    if not does_constraint_warning_exist():
        raise RuntimeError("Expected to find constraints warning logs, but they were not present.")

    constraints_map = get_constraints_map()
    # check apache-airflow is pinned
    check_version_mismatch("airflow", version("apache-airflow"), os.environ.get("AIRFLOW_VERSION"))

    print("Importing and checking different module versions comply with constraints")

    for package in normal_packages:
        importlib.import_module(package)
        check_version_mismatch(package, version(package), constraints_map[package])

    for module_name, package_name in provider_package_map.items():
        importlib.import_module(module_name)
        check_version_mismatch(package_name, version(package_name), constraints_map[package_name])

    if version_mismatches:
        print("Found version mismatches between installs and constraints:\n" + "\n".join(version_mismatches))
        raise RuntimeError("Constraints check failed")


with DAG(
    "constraints_forcing_test",
    default_args=default_args,
    description="A test dag for checking constraints forcing",
    schedule="@once",
    is_paused_upon_creation=False,
) as dag:
    test_import = PythonOperator(task_id="check_imports", python_callable=check_imports)
