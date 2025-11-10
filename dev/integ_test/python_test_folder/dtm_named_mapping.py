from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


# Define the functions
def generate_data():
    # Return a list of dictionaries with city names and temperatures
    return [
        [{"city": "New York", "temp": 72}],
        [{"city": "San Francisco", "temp": 65}],
        [{"city": "Miami", "temp": 85}],
        [{"city": "Chicago", "temp": 68}],
    ]


def process_city_data(city_data):
    # Get the city data from args
    city_name = city_data["city"]
    temperature = city_data["temp"]

    # Add the city name to the context for the map_index_template
    context = get_current_context()
    context["city_name"] = city_name

    # Process the data
    result = f"Temperature in {city_name} is {temperature}°F"
    print(result)
    return result


def summarize_results(results):
    return f"Processed {len(results)} cities: {', '.join(results)}"


# Create the DAG
dag = DAG(dag_id="named_mapping_example", default_args={"start_date": datetime(2026, 1, 1)}, schedule="@daily")

# Generate city data
city_data = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)

# Process each city with named mapping
process_cities = PythonOperator.partial(
    task_id="process_city",
    python_callable=process_city_data,
    dag=dag,
    # Use map_index_template to name each task instance with the city name
    map_index_template="{{ city_name }}",
).expand(op_args=city_data.output)

# Summarize the results
summary = PythonOperator(
    task_id="summarize",
    python_callable=summarize_results,
    op_kwargs={"results": process_cities.output},
    dag=dag,
)

# Set dependencies
city_data >> process_cities >> summary
