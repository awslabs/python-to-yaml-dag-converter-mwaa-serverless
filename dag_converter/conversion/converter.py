import inspect
from pathlib import Path

from airflow.models.dag import DAG

from dag_converter.conversion.default_args import convert_default_args
from dag_converter.conversion.schedule import convert_schedule
from dag_converter.conversion.tasks import convert_tasks
from dag_converter.schema_parser import ArgumentValidator
from dag_converter.taskflow_parser import TaskFlowAnalyzer


def get_dag_default_values() -> dict:
    """
    Returns a dictionary of default values for DAG class.
    
    Returns:
        dict: Dictionary of parameter names and their default values
    """
    defaults = {}
    
    try:
        sig = inspect.signature(DAG.__init__)
        for name, param in sig.parameters.items():
            if name not in ["self", "args", "kwargs"] and param.default is not inspect.Parameter.empty:
                defaults[name] = param.default
    except Exception:
        pass
    
    return defaults


def is_dag_default_value(key: str, value, dag_defaults: dict) -> bool:
    """
    Check if a DAG-level value is a default that should be excluded from output.
    """
    if key not in dag_defaults:
        return False
    
    default_value = dag_defaults[key]
    
    # Only filter out values that are still NOTHING (truly not set)
    if hasattr(value, 'name') and value.name == 'NOTHING':
        return True
    
    # For regular defaults, filter if values match
    return value == default_value


def get_converted_format(
    taskflow_parser: TaskFlowAnalyzer, dag_object: DAG, dag_file_path: Path, validator: ArgumentValidator
):
    """Return the DagFactory-compatible format of the Dag in dictionary format"""
    dag_id = getattr(dag_object, "_dag_id", None)
    # Taskflow may store dag_id as internal parameter with leading '_'
    if not dag_id:
        dag_id = getattr(dag_object, "dag_id", None)

    # Extract values from the Objects in the 'params' field
    params_obj = getattr(dag_object, "params", None)
    params_dict = {}
    if params_obj and hasattr(params_obj, "items"):
        for key, param in params_obj.items():
            if hasattr(param, "value"):
                params_dict[key] = param.value
            else:
                params_dict[key] = param

    # Convert Dag information
    converted_dag = {
        dag_id: {
            "dag_id": dag_id,
            "params": params_dict,
            "default_args": convert_default_args(getattr(dag_object, "default_args", {}), validator),
            "schedule": convert_schedule(dag_object),
            "tasks": {},
        }
    }

    # Get DAG default values to filter them out
    dag_defaults = get_dag_default_values()

    # Filter out extraneous Dag fields
    for key in dir(dag_object):
        value = getattr(dag_object, key, None)
        # Only keep non-internal fields and valid fields
        if key not in converted_dag[dag_id] and not key.startswith("_") and validator.validate_field("dag", key, value):
            # Skip keywords that were not set by user (None values or default values)
            if value is None or is_dag_default_value(key, value, dag_defaults):
                pass
            else:
                converted_dag[dag_id][key] = value

    # Convert tasks
    converted_dag[dag_id]["tasks"] = convert_tasks(taskflow_parser, dag_object, dag_file_path, validator)

    return converted_dag
