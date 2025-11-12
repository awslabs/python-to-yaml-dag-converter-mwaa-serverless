from types import NoneType
from typing import Any

import yaml


def build_yaml(cleaned_data: dict[str, Any]) -> str:
    """Build the Yaml from the dictionary"""

    class CustomDumper(yaml.SafeDumper):
        pass

    def str_presenter(dumper: yaml.SafeDumper, data: str) -> yaml.ScalarNode:
        """Handle string presentation in YAML."""
        if "\n" in data:
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
        if "'" in data:
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    def dict_presenter(dumper: yaml.SafeDumper, data: dict) -> yaml.MappingNode:
        """Handle dictionary presentation in YAML."""
        return dumper.represent_dict(data)

    def represent_none(dumper, data: None):
        """Handle NoneType. Use None instead of null"""
        return dumper.represent_scalar("tag:yaml.org,2002:str", "None")

    CustomDumper.add_representer(str, str_presenter)
    CustomDumper.add_representer(dict, dict_presenter)
    CustomDumper.add_representer(NoneType, represent_none)

    try:
        return yaml.dump(cleaned_data, Dumper=CustomDumper, default_flow_style=False, sort_keys=False)
    except Exception as e:
        raise Exception("Failed to build yaml " + str(e)) from e
