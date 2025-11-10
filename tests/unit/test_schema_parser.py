"""Tests for schema_parser module."""

import os
import tempfile
import unittest
from pathlib import Path

from dag_converter.schema_parser import ArgumentValidator


class TestDagBag(unittest.TestCase):
    def _create_file(self, content):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(content)
            return Path(f.name)

    def test_extract_allowed_fields_basic(self):
        """Test extraction of basic allowed fields."""
        content = """
        dag:
            dag_id: any(required=True)
            schedule_interval: any(required=False)
        task:
            task_id: any(required=True)
            operator: enum("PythonOperator", "BashOperator")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertIn("dag", validator.allowed_fields)
            self.assertIn("task", validator.allowed_fields)
            self.assertIn("dag_id", validator.allowed_fields["dag"])
            self.assertIn("operator", validator.allowed_fields["task"])
        finally:
            os.unlink(file_path)

    def test_extract_allowed_fields_multiple_sections(self):
        """Test extraction from multiple YAML sections."""
        content = """
        dag:
            dag_id: any(required=True)
        ---
        task:
            task_id: any(required=True)
            operator: enum("PythonOperator")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertIn("dag", validator.allowed_fields)
            self.assertIn("task", validator.allowed_fields)
        finally:
            os.unlink(file_path)

    def test_validate_field_existing_field(self):
        """Test validation of existing field."""
        content = """
        dag:
            dag_id: any(required=True)
            schedule_interval: any(required=False)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertTrue(validator.validate_field("dag", "dag_id", "test_dag"))
            self.assertFalse(validator.validate_field("dag", "nonexistent", "value"))
        finally:
            os.unlink(file_path)

    def test_validate_field_no_value_check(self):
        """Test validation with no_value_check flag."""
        content = """
        dag:
            dag_id: any(required=True)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertTrue(validator.validate_field("dag", "dag_id", None, no_value_check=True))
            self.assertFalse(validator.validate_field("dag", "nonexistent", None, no_value_check=True))
        finally:
            os.unlink(file_path)

    def test_validate_field_enum_values(self):
        """Test validation of enum values."""
        content = """
        task:
            operator: enum("PythonOperator", "BashOperator")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertTrue(validator.validate_field("task", "operator", "PythonOperator"))
            self.assertTrue(validator.validate_field("task", "operator", "BashOperator"))
            self.assertFalse(validator.validate_field("task", "operator", "InvalidOperator"))
        finally:
            os.unlink(file_path)

    def test_validate_field_regex(self):
        """Test validation with regex patterns."""
        content = """
        dag:
            dag_id: regex("^[a-z_]+$")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertTrue(validator.validate_field("dag", "dag_id", "valid_dag_id"))
            self.assertFalse(validator.validate_field("dag", "dag_id", "invalid-dag-id"))
        finally:
            os.unlink(file_path)

    def test_get_allowed_dag_args(self):
        """Test getting allowed DAG arguments."""
        content = """
        dag:
            dag_id: any(required=True)
            schedule_interval: any(required=False)
            start_date: any(required=True)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            dag_args = validator.get_allowed_dag_args()
            self.assertEqual(set(dag_args), {"dag_id", "schedule_interval", "start_date"})
        finally:
            os.unlink(file_path)

    def test_get_allowed_dag_args_no_dag_section(self):
        """Test getting DAG args when no dag section exists."""
        content = """
        task:
            task_id: any(required=True)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            dag_args = validator.get_allowed_dag_args()
            self.assertEqual(dag_args, [])
        finally:
            os.unlink(file_path)

    def test_get_allowed_default_args(self):
        """Test getting allowed default_args."""
        content = """
        dag:
            default_args: enum("owner", "depends_on_past", "retries")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            default_args = validator.get_allowed_default_args()
            self.assertEqual(set(default_args), {"owner", "depends_on_past", "retries"})
        finally:
            os.unlink(file_path)

    def test_get_allowed_task_fields(self):
        """Test getting allowed task fields."""
        content = """
        task:
            task_id: any(required=True)
            operator: enum("PythonOperator")
            depends_on_past: any(required=False)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            task_fields = validator.get_allowed_task_fields()
            self.assertEqual(set(task_fields), {"task_id", "operator", "depends_on_past"})
        finally:
            os.unlink(file_path)

    def test_get_allowed_operators(self):
        """Test getting allowed operators."""
        content = """
        task:
            operator: enum("PythonOperator", "BashOperator", "S3Sensor")
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            operators = validator.get_allowed_operators()
            self.assertEqual(set(operators), {"PythonOperator", "BashOperator", "S3Sensor"})
        finally:
            os.unlink(file_path)

    def test_parse_enum_from_string(self):
        """Test parsing enum strings."""
        validator = ArgumentValidator.__new__(ArgumentValidator)  # Create without __init__

        # Test basic enum
        result = validator.parse_enum_from_string('enum("value1", "value2", "value3")')
        self.assertEqual(result, ["value1", "value2", "value3"])

        # Test single value enum
        result = validator.parse_enum_from_string('enum("single_value")')
        self.assertEqual(result, ["single_value"])

        # Test empty enum
        result = validator.parse_enum_from_string("enum()")
        self.assertEqual(result, [])

        # Test non-enum string
        result = validator.parse_enum_from_string("any(required=True)")
        self.assertEqual(result, [])

    def test_empty_yaml_file(self):
        """Test handling of empty YAML file."""
        content = ""
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            self.assertEqual(validator.allowed_fields, {})
            self.assertEqual(validator.get_allowed_dag_args(), [])
            self.assertEqual(validator.get_allowed_operators(), [])
        finally:
            os.unlink(file_path)

    def test_invalid_yaml_sections(self):
        """Test handling of invalid YAML sections."""
        content = """
        dag:
            dag_id: any(required=True)
        ---
        invalid yaml content: [
        ---
        task:
            task_id: any(required=True)
        """
        file_path = self._create_file(content)
        try:
            validator = ArgumentValidator(file_path)
            # Should still parse valid sections
            self.assertIn("dag", validator.allowed_fields)
            self.assertIn("task", validator.allowed_fields)
        finally:
            os.unlink(file_path)
