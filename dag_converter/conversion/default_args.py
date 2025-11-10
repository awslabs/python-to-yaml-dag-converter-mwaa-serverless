from typing import Any

from dag_converter.conversion.exceptions import InvalidDefaultArg
from dag_converter.schema_parser import ArgumentValidator


def convert_default_args(args: dict[str, Any], validator: ArgumentValidator) -> dict[str, Any]:
    converted = {}

    for key, value in args.items():
        if key == "start_date":  # Handle reformatting of date
            if hasattr(value, "strftime"):  # Handle pendulum.DateTime and other date objects
                converted[key] = value.strftime("%Y-%m-%d")
            else:
                converted[key] = value
        # Validate default args
        elif validator.validate_field("dag", "default_args", key):
            converted[key] = value
        else:
            raise InvalidDefaultArg(f"Default Argument '{key}' is not supported")

    return converted
