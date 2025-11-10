from datetime import datetime

from airflow.sdk import DAG, task

# Define the DAG
with DAG(
    dag_id="example_partial_with_args_and_kwargs", schedule="@daily", default_args={"start_date": datetime(2026, 1, 1)}
) as dag:
    # Task that generates a list of items to process
    @task
    def get_items():
        return ["apple", "banana", "cherry"]

    # Task that processes each item with fixed parameters
    @task
    def process_item(item, prefix, suffix="", capitalize=False):
        """
        Process an item with various transformations

        Args:
            item: The item to process
            prefix: Prefix to add to the item
            suffix: Optional suffix to add (default: "")
            capitalize: Whether to capitalize the item (default: False)
        """
        result = item
        if capitalize:
            result = result.upper()
        return f"{prefix}-{result}{suffix}"

    # Task that aggregates results
    @task
    def aggregate_results(results):
        return f"Processed items: {', '.join(results)}"

    # Get the items
    items = get_items()

    # Process each item with fixed keyword arguments
    # All arguments must be passed as keyword arguments
    processed = process_item.partial(
        prefix="item",  # Fixed keyword argument for prefix
        capitalize=True,  # Fixed keyword argument for capitalize
    ).expand(
        item=items  # Mapped argument
    )

    # Aggregate the results
    final_result = aggregate_results(processed)
