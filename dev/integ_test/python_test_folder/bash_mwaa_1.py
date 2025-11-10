from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="check_aws_cli_functionality",
    schedule="@once",
    default_args={"start_date": "2026-01-01"},
    dagrun_timeout=timedelta(minutes=5),
) as dag:
    # Just for data, display the CLI version
    display_cli_version = BashOperator(task_id="display_cli_version", bash_command="aws --version")

    check_json_output = BashOperator(
        task_id="check_json_output", bash_command="aws mwaa list-environments --region $AWS_REGION --output json"
    )

    check_yaml_output = BashOperator(
        task_id="check_yaml_output", bash_command="aws mwaa list-environments --region $AWS_REGION --output yaml"
    )

    display_cli_version >> check_json_output >> check_yaml_output

if __name__ == "__main__":
    dag.cli()
