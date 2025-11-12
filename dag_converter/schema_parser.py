import re
from pathlib import Path
from typing import Any

import yaml


class ArgumentValidator:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.allowed_fields = self.extract_allowed_fields(self.file_path)

    def extract_allowed_fields(self, schema_file: Path) -> dict[str, dict[str, Any]]:
        """Extract allowed fields from validator schema YAML file."""

        with open(schema_file) as f:
            content = f.read()

        sections = content.split("---")

        allowed_fields = {}

        for section in sections:
            if not section.strip():
                continue

            try:
                schema_data = yaml.safe_load(section)
                if not schema_data or not isinstance(schema_data, dict):
                    continue

                for key, value in schema_data.items():
                    if isinstance(value, dict):
                        allowed_fields[key] = value

            except yaml.YAMLError as e:
                print(e)
                continue

        return allowed_fields

    def validate_field(self, section_key: str, field_key: str, field_value: Any | None, no_value_check=False) -> bool:
        """Verify that 'field_key' and corresponding 'field_value" are accepted"""
        allowed_fields = self.allowed_fields

        if section_key not in allowed_fields or field_key not in allowed_fields[section_key]:
            return False

        # Flag to skip the value check
        if no_value_check:
            return True

        check_value = allowed_fields[section_key][field_key]
        # Handle case where value is an enum that contains a list
        # TODO: need a better solution here, 'check_value' may just be a type check
        check_value = self.parse_enum_from_string(check_value) if "enum" in check_value else [check_value]  # noqa: E501

        for cur_acceptable in check_value:
            # Handle regex matching
            if "regex(" in cur_acceptable:
                match = re.search(r'regex\([\'"]([^\'"]+)[\'"]\)', cur_acceptable)
                if match:
                    pattern = match.group(1)
                    # Unescape the backslashes in the pattern
                    pattern = re.sub(r"\\\\", r"\\", pattern)
                    if bool(re.match(pattern, str(field_value))):
                        return True
            # Case where check_value is just a type check
            # We will not be doing a type check here, since DagBag should handle most of
            # that during loading
            # TODO: may run into trouble if enum also only had one value
            elif len(check_value) == 1 or str(field_value) == cur_acceptable:
                return True
        return False

    def get_allowed_dag_args(self) -> list[str]:
        """Get allowed dag fields from schema."""
        allowed_fields = self.allowed_fields

        if "dag" in allowed_fields:
            allowed_list = []
            for key in allowed_fields["dag"]:
                allowed_list.append(key)
            return allowed_list

        return []

    def get_allowed_default_args(self) -> list[str]:
        """Get allowed default_args fields from schema."""
        allowed_fields = self.allowed_fields

        if "dag" in allowed_fields and "default_args" in allowed_fields["dag"]:
            return self.parse_enum_from_string(allowed_fields["dag"]["default_args"])

        return []

    def get_allowed_task_fields(self) -> list[str]:
        """Get allowed task fields from schema."""
        allowed_fields = self.allowed_fields

        if "task" in allowed_fields:
            allowed_list = []
            for key in allowed_fields["task"]:
                allowed_list.append(key)
            return allowed_list

        return []

    def get_allowed_operators(self) -> list[str]:
        """Extract allowed operators from schema."""
        allowed_fields = self.allowed_fields

        if "task" in allowed_fields and "operator" in allowed_fields["task"]:
            return self.parse_enum_from_string(allowed_fields["task"]["operator"])

        return []

    def parse_enum_from_string(self, enum_string) -> list[str]:
        """Turn 'enum()' format into a list"""
        match = re.search(r"enum\((.*?)\)", enum_string)
        if match:
            quoted_values = re.findall(r'"([^"]*)"', match.group(1))
            return quoted_values
        return []
