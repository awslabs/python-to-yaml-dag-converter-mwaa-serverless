import tempfile
from pathlib import Path

from airflow.models.dag import DAG

from dag_converter.console_config import print_error
from dag_converter.dag_comparator import compare_dags
from dag_converter.dagbag import get_dag_factory_object


def validate_yaml_with_dagbag(initial_dag_object: DAG | None, dag_yaml: str, input_file_path: Path):
    """Validate the yaml using DagFactory and DagBag"""

    try:
        # Create temporary file directory
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                dags_dir = Path(temp_dir) / "dags"
                configs_dir = dags_dir / "configs"
                configs_dir.mkdir(parents=True, exist_ok=True)

                yaml_path = configs_dir / "temp_dag.yaml"
                with open(yaml_path, "w") as f:
                    f.write(str(dag_yaml))

                # Use new temporary directory to avoid caching issues
                loader_path = dags_dir / "load_yaml_dags.py"
                # Get the path to python_input directory
                python_input_dir = input_file_path.parent

                # Writes loader script for dag-factory. Loader script for each yaml is
                # required in order to load converted YAML into Python DAG object for
                # validation. https://www.astronomer.io/docs/learn/dag-factory#step-5-implement-the-generator-script
                with open(loader_path, "w") as f:
                    f.write(f"""
import os
from pathlib import Path
import sys

# Add python_input directory to Python path
sys.path.insert(0, r"{python_input_dir}")

# The following import is here so Airflow parses this file
from airflow import DAG
from dagfactory import load_yaml_dags


try:
    # Use the absolute path to the YAML file
    config_file = "{yaml_path}"  # Use the actual path to the YAML file
    print(f"Loading DAG from: {{config_file}}")

    load_yaml_dags(globals_dict=globals(), config_filepath=config_file)
    
    # Print loaded DAGs
    dag_ids = [key for key in globals().keys()
               if not key.startswith('__') and isinstance(globals().get(key), DAG)]
    print(f"Successfully loaded DAGs: {{dag_ids}}")
    
except Exception as e:
    print(f"Error loading DAGs: {{str(e)}}")
    raise e
""")
                try:
                    final_dag_object = get_dag_factory_object(dags_dir)
                except Exception as e:
                    print_error(str(e))
                    return False, "No DAGs were loaded from the YAML file. Check for missing required parameters."

                # Log Dag Object comparison information
                if initial_dag_object:
                    compare_dags(initial_dag_object, final_dag_object)

                return True, "YAML is valid and DAGs were successfully loaded"

            except Exception as e:
                return False, f"Error during validation setup: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
