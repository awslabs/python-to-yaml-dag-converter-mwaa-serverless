import re
from typing import Any

from dag_converter.taskflow_parser import TaskFlowAnalyzer


def handle_python_decorator(task_dict: dict[str, Any], taskflow_parser: TaskFlowAnalyzer):
    """Handle special conversion for taskflow python decorators"""
    del task_dict["operator"]
    task_dict["decorator"] = "airflow.decorators.task"

    # Should be of the form <file_name>.<function_name>
    function_name = task_dict["python_callable"].split(".", 1)[1]
    parameters = taskflow_parser.task_functions[function_name]

    # DagFactory does not support having the parameters within 'op_args' or 'op_kwargs'
    # Move the parameters within 'op_args' and 'op_kwargs' up one level
    if "op_args" in task_dict and "op_kwargs" in task_dict:
        op_args = extract_op_args(task_dict["op_args"])
        op_kwargs = extract_op_kwargs(task_dict["op_kwargs"])

        # Iterate through positional arguments first
        for i in range(len(op_args)):
            task_dict[parameters[i]] = op_args[i]
        for k, v in op_kwargs.items():
            task_dict[k] = v
        del task_dict["op_args"]
        del task_dict["op_kwargs"]

    # Handle 'partial' during DTM specifically for TaskFlow
    # Move parameters up one level
    if "partial" in task_dict:
        op_args = extract_op_args(task_dict["partial"].get("op_args", []))
        op_kwargs = extract_op_kwargs(task_dict["partial"].get("op_kwargs", {}))
        for i in range(len(op_args)):
            task_dict["partial"][parameters[i]] = op_args[i]
        for k, v in op_kwargs.items():
            task_dict["partial"][k] = v
        if "op_args" in task_dict["partial"]:
            del task_dict["partial"]["op_args"]
        if "op_kwargs" in task_dict["partial"]:
            del task_dict["partial"]["op_kwargs"]


def extract_op_args(op_args_str):
    """Clean up and return op_args"""
    result = []
    for arg in op_args_str:
        if "task_instance.xcom_pull" in str(arg):
            result.append(cleanup_xcom(str(arg)))
        else:
            result.append(arg)
    return result


def extract_op_kwargs(op_kwargs_str):
    """Clean up and return op_kwargs"""
    result = {}
    for arg, value in op_kwargs_str.items():
        if "task_instance.xcom_pull" in str(value):
            result[arg] = cleanup_xcom(str(value))
        else:
            result[arg] = value
    return result


def cleanup_xcom(xcom_obj_str):
    # Extract task_id
    task_id_match = re.search(r"task_ids=['\"]([^'\"]*)['\"]", xcom_obj_str)
    task_id = task_id_match.group(1) if task_id_match else None

    # Extract key
    key_match = re.search(r"key=['\"]([^'\"]*)['\"]", xcom_obj_str)
    key = key_match.group(1) if key_match else "return_value"  # Default to return_value if not specified

    if key == "return_value":
        return f"+{task_id}"

    return xcom_obj_str
