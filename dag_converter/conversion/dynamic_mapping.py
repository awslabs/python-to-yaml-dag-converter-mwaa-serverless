import re
from typing import Any

from airflow.models.mappedoperator import MappedOperator

from dag_converter.cleanup import get_cleanup_dag
from dag_converter.schema_parser import ArgumentValidator


def handle_dynamic_task_mapping(
    tasks_dict: dict[str, Any], task_types: dict[str, str], task: MappedOperator, validator: ArgumentValidator
):
    """Handle dynamic task mapping"""
    if hasattr(task, "op_kwargs_expand_input"):
        tasks_dict["expand"] = get_cleanup_dag(task.op_kwargs_expand_input.value)
        reformat(tasks_dict["expand"], task_types)
    elif hasattr(task, "expand_input"):
        tasks_dict["expand"] = get_cleanup_dag(task.expand_input.value)
        reformat(tasks_dict["expand"], task_types)

        # Handle special case when 'op_kwargs' is in the 'expand' dictionary
        if "op_kwargs" in tasks_dict["expand"]:
            reformat(tasks_dict["expand"]["op_kwargs"], task_types)

    if hasattr(task, "partial_kwargs"):
        tasks_dict["partial"] = get_cleanup_dag(task.partial_kwargs)

        # Handle Python Operator that uses DTM
        # The 'python_callable' will be set to be a string representation of the actual function
        # TODO: need better way to create the python_callable
        if "python_callable" in tasks_dict["partial"]:
            tasks_dict["python_callable"] = tasks_dict["partial"]["python_callable"]
            del tasks_dict["partial"]["python_callable"]

        # Cleanup extra fields
        keys_to_delete = []
        for key, value in tasks_dict["partial"].items():
            if not validator.validate_field("task", key, value) or value is None or key in tasks_dict:
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del tasks_dict["partial"][key]


def reformat(expand_dict: dict | list | tuple, task_types: dict[str, str]):
    """Reformat XComs to turn unserializable dictionary format into just the task id"""
    if isinstance(expand_dict, dict):
        iterator = expand_dict.items()
    elif isinstance(expand_dict, list | tuple):
        iterator = enumerate(expand_dict)
    print(expand_dict)

    for k, v in iterator:
        # Handle expansion through task XComs
        if "task_instance.xcom_pull" in v:
            # TODO: this is an attempt at handling DTM while mixing TaskFlow and regular operators
            # Currently, this does not work, and DagFactory does not seem to support a solution
            task_id_match = re.search(r"task_ids=['\"]([^'\"]*)['\"]", v)
            task_id = str(task_id_match.group(1) if task_id_match else None)

            if task_types[task_id] == "_PythonDecoratedOperator":
                expand_dict[k] = f"+{task_id}"
            else:
                expand_dict[k] = f"{task_id}.output"
