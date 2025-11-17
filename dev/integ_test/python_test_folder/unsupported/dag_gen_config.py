import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configuration file path
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "dependencies/dag_configs.yaml")

# Default arguments for all DAGs
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Load configuration from YAML file
def load_dag_configs():
    with open(CONFIG_PATH) as file:
        return yaml.safe_load(file)


# Function to process data
def process_data(data_source, **kwargs):
    print(f"Processing data from {data_source}")
    # Actual processing logic would go here
    return f"Processed data from {data_source}"


# Create DAGs dynamically
def generate_dags():
    dags = {}
    configs = load_dag_configs()

    for config in configs:
        dag_id = f"dynamic_dag_{config['name']}"
        schedule = config.get("schedule", "@daily")

        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule=schedule,
            description=f"Dynamically generated DAG for {config['name']}",
        )

        # Create tasks based on configuration
        with dag:
            # Start task
            start = BashOperator(
                task_id="start",
                bash_command=f"echo 'Starting processing for {config['name']}'",
            )

            # Create processing tasks for each data source
            processing_tasks = []
            for i, source in enumerate(config["data_sources"]):
                task = PythonOperator(
                    task_id=f'process_{source["name"]}',
                    python_callable=process_data,
                    op_kwargs={"data_source": source["path"]},
                )
                processing_tasks.append(task)

                # Set dependencies
                start >> task

            # End task that depends on all processing tasks
            end = BashOperator(
                task_id="end",
                bash_command=f"echo 'Completed processing for {config['name']}'",
            )

            # Set dependencies for end task
            for task in processing_tasks:
                task >> end

        # Add DAG to dictionary
        dags[dag_id] = dag

    return dags


# Generate all DAGs
globals().update(generate_dags())
