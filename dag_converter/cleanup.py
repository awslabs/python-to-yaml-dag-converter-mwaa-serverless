"""Module for cleaning up data structures before serialization."""

import re
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any

from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.definitions.xcom_arg import PlainXComArg


def get_cleanup_dag(obj: Any) -> Any:
    """Recursively clean data structure of non-serializable objects.

    Args:
        obj: Any Python object to clean up

    Returns:
        Cleaned up version of the object suitable for YAML serialization

    The function handles:
    - Encoded values with __var and __type
    - Basic Python containers (dict, list, tuple, set)
    - Custom objects (converted to Dag factory YAML object format)
    - Enums (converted to Dag factory YAML object format)
    - Date/time objects (converted to custom Python object)
    - None values
    - Nested structures
    """
    if obj is None:
        return None

    if isinstance(obj, str) and "xcom_pull" in obj:
        pattern = r',\s*dag_id=[\'"][^\'"]*[\'"]'
        print("xcom arg:" + str(obj))
        return re.sub(pattern, "", obj)

    if isinstance(obj, str):
        return str(obj)

    if is_primitive(obj):
        return obj

    if isinstance(obj, PlainXComArg):
        pattern = r',\s*dag_id=[\'"][^\'"]*[\'"]'
        # Remove the dag_id parameter
        print("plain xcom arg:" + str(obj))
        return re.sub(pattern, "", str(obj))

    if isinstance(obj, ParamsDict):
        return obj.dump()

    if isinstance(obj, Enum):
        return {"__type__": obj.__module__ + "." + obj.__class__.__name__, "__args__": [obj.value]}

    if isinstance(obj, dict):
        # Handle encoded values
        if "__var" in obj and "__type" in obj:
            # Special handling for None
            if obj["__var"] is None:
                return None
            return get_cleanup_dag(obj["__var"])

        # Regular dictionary
        return {k: get_cleanup_dag(v) for k, v in obj.items()}

    if isinstance(obj, list | tuple | set):
        return [get_cleanup_dag(item) for item in obj]

    if isinstance(obj, datetime):
        custom_object = {
            "__type__": "datetime.datetime",
            "year": obj.year,
            "month": obj.month,
            "day": obj.day,
            "hour": obj.hour,
            "minute": obj.minute,
            "second": obj.second,
            "microsecond": obj.microsecond,
        }
        return {key: value for key, value in custom_object.items() if value}

    if isinstance(obj, date):
        return obj.isoformat()

    if isinstance(obj, time):
        return obj.strftime("%H:%M:%S")

    if isinstance(obj, timedelta):
        return {"__type__": "datetime.timedelta", "seconds": obj.total_seconds()}

    if callable(obj) and hasattr(obj, "__name__"):
        function_name = obj.__name__

        if hasattr(obj, "__module__"):
            module_name = obj.__module__
            # Clean up unusual prefixes and extract actual module name
            if "unusual_prefix_" in module_name:
                parts = module_name.split("_")
                input_file_name = parts[3]
                for i in range(4, len(parts)):
                    input_file_name += f"_{parts[i]}"
            else:
                input_file_name = module_name
        else:
            input_file_name = "unknown"

        return f"{input_file_name}.{function_name}"

    if hasattr(obj, "__dict__"):  # Handle custom objects
        return {"__type__": obj.__module__ + "." + obj.__class__.__name__, **get_cleanup_dag(obj.__dict__)}

    return str(obj)


def is_primitive(obj: Any) -> bool:
    """Check if an object is NOT a primitive type."""
    primitive_types = (int, float, bool, bytes, type(None))
    return isinstance(obj, primitive_types)
