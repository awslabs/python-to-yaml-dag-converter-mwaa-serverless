"""Tests for static_validator module."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from dag_converter.conversion.exceptions import (
    InvalidOperatorError,
)
from dag_converter.static_validator import (
    extract_dag_params,
    extract_operator_types,
    validate_file,
)


class TestStaticValidator(unittest.TestCase):
    """Test cases for static_validator module."""

    def _create_file(self, content):
        """Create a temporary Python file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(content)
            return Path(f.name)

    def test_extract_operator_types_basic(self):
        """Test extraction of basic operator types."""
        content = """
from airflow.operators.python import PythonOperator
from airflow.sensors.s3 import S3KeySensor

task1 = PythonOperator(task_id="test")
task2 = S3KeySensor(task_id="sensor")
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertEqual(operators, {"PythonOperator", "S3KeySensor"})
        finally:
            os.unlink(file_path)

    def test_extract_operator_types_dynamic_mapping(self):
        """Test extraction of operators with dynamic mapping methods."""
        content = """
task = PythonOperator.partial(task_id="base")
task2 = BashOperator.expand(bash_command=["cmd1", "cmd2"])
task3 = S3Sensor.expand_kwargs([{"bucket": "b1"}, {"bucket": "b2"}])
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertEqual(operators, {"PythonOperator", "BashOperator", "S3Sensor"})
        finally:
            os.unlink(file_path)

    def test_extract_operator_types_no_operators(self):
        """Test file with no operators."""
        content = """
def regular_function():
    pass

variable = "test"
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertEqual(operators, set())
        finally:
            os.unlink(file_path)

    def test_extract_operator_types_mixed_calls(self):
        """Test file with operators and non-operator calls."""
        content = """
task = PythonOperator(task_id="test")
result = some_function()
data = MyClass()
sensor = FileSensor(task_id="file")
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertEqual(operators, {"PythonOperator", "FileSensor"})
        finally:
            os.unlink(file_path)

    def test_extract_dag_params_basic(self):
        """Test extraction of basic DAG parameters."""
        content = """
from airflow import DAG

dag = DAG(
    dag_id="test_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
"""
        file_path = self._create_file(content)
        try:
            params = extract_dag_params(file_path)
            self.assertEqual(set(params), {"dag_id", "schedule_interval", "start_date", "catchup"})
        finally:
            os.unlink(file_path)

    def test_extract_dag_params_attribute_access(self):
        """Test extraction of DAG parameters with attribute access."""
        content = """
import airflow

dag = airflow.DAG(
    dag_id="test_dag",
    description="Test DAG"
)
"""
        file_path = self._create_file(content)
        try:
            params = extract_dag_params(file_path)
            self.assertEqual(set(params), {"dag_id", "description"})
        finally:
            os.unlink(file_path)

    def test_extract_dag_params_no_dag(self):
        """Test file with no DAG instantiation."""
        content = """
def some_function():
    pass

task = PythonOperator(task_id="test")
"""
        file_path = self._create_file(content)
        try:
            params = extract_dag_params(file_path)
            self.assertEqual(params, [])
        finally:
            os.unlink(file_path)

    def test_extract_dag_params_multiple_dags(self):
        """Test file with multiple DAG instantiations."""
        content = """
dag1 = DAG(dag_id="dag1", schedule_interval="@daily")
dag2 = DAG(dag_id="dag2", start_date=datetime.now(), catchup=True)
"""
        file_path = self._create_file(content)
        try:
            params = extract_dag_params(file_path)
            # Should capture parameters from both DAGs
            self.assertIn("dag_id", params)
            self.assertIn("schedule_interval", params)
            self.assertIn("start_date", params)
            self.assertIn("catchup", params)
        finally:
            os.unlink(file_path)

    def test_validate_file_valid_operators(self):
        """Test validate_file with valid operators."""
        content = """
task = PythonOperator(task_id="test")
dag = DAG(dag_id="test_dag")
"""
        file_path = self._create_file(content)

        mock_validator = Mock()
        mock_validator.get_allowed_operators.return_value = ["PythonOperator", "BashOperator"]
        mock_validator.validate_field.return_value = True

        try:
            # Should not raise any exception
            validate_file(file_path, mock_validator)
        finally:
            os.unlink(file_path)

    def test_validate_file_invalid_operator(self):
        """Test validate_file with invalid operator."""
        content = """
task = UnsupportedOperator(task_id="test")
"""
        file_path = self._create_file(content)

        mock_validator = Mock()
        mock_validator.get_allowed_operators.return_value = ["PythonOperator", "BashOperator"]

        try:
            with self.assertRaises(InvalidOperatorError) as context:
                validate_file(file_path, mock_validator)
            self.assertIn("UnsupportedOperator is not supported", str(context.exception))
        finally:
            os.unlink(file_path)

    def test_validate_file_empty_file(self):
        """Test validate_file with empty file."""
        content = ""
        file_path = self._create_file(content)

        mock_validator = Mock()
        mock_validator.get_allowed_operators.return_value = []

        try:
            # Should not raise any exception
            validate_file(file_path, mock_validator)
        finally:
            os.unlink(file_path)
