from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# Define the DAG
with DAG(
    dag_id="aws_athena_queries",
    schedule="@daily",
    default_args={"start_date": datetime(2026, 1, 1)},
) as dag:
    # Define a list of query configurations
    query_configs = [
        "SELECT * FROM sales WHERE region = 'us-east-1'",
        "SELECT * FROM customers WHERE signup_date > '2023-01-01'",
        "SELECT * FROM products WHERE inventory < 100",
    ]

    # Run Athena queries using dynamic task mapping
    run_queries = AthenaOperator.partial(
        task_id="run_athena_query",
        aws_conn_id="aws_default",
        database="analytics",
        output_location="s3://my-athena-results/",
        sleep_time=30,
    ).expand(query=query_configs)
