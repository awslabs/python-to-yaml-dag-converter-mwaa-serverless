import unittest
from unittest.mock import Mock, patch

from dag_converter.conversion.converter import get_converted_format, get_dag_default_values, is_dag_default_value


class TestGetConvertedFormat(unittest.TestCase):
    def setUp(self):
        self.mock_validator = Mock()
        self.mock_taskflow_parser = Mock()

    @patch("dag_converter.conversion.converter.convert_tasks")
    @patch("dag_converter.conversion.converter.convert_schedule")
    @patch("dag_converter.conversion.converter.convert_default_args")
    def test_basic_dag_conversion(self, mock_convert_default, mock_convert_schedule, mock_convert_tasks):
        """Test basic DAG conversion"""

        # Setup mocks
        mock_convert_default.return_value = {"retries": 3}
        mock_convert_schedule.return_value = "@daily"
        mock_convert_tasks.return_value = {"task1": {"operator": "BashOperator"}}
        self.mock_validator.validate_field.return_value = True

        # Create mock DAG object
        dag_object = Mock()
        dag_object.dag_id = "test_dag"
        dag_object.default_args = {"retries": 3}
        dag_object.params = {}
        dag_object.description = "Test DAG"
        # Ensure _dag_id doesn't exist
        del dag_object._dag_id

        with patch("builtins.dir", return_value=["dag_id", "default_args", "params", "description"]):
            result = get_converted_format(self.mock_taskflow_parser, dag_object, "test.py", self.mock_validator)

        expected = {
            "test_dag": {
                "dag_id": "test_dag",
                "params": {},
                "default_args": {"retries": 3},
                "schedule": "@daily",
                "tasks": {"task1": {"operator": "BashOperator"}},
                "description": "Test DAG",
            }
        }
        self.assertEqual(result, expected)

    def test_get_dag_default_values(self):
        """Test that DAG default values are extracted"""
        defaults = get_dag_default_values()
        
        # Check that common defaults exist
        self.assertIn('fail_fast', defaults)
        self.assertIn('max_active_runs', defaults)
        self.assertEqual(defaults['fail_fast'], False)
        self.assertEqual(defaults['max_active_runs'], 16)

    def test_is_dag_default_value(self):
        """Test default value filtering logic"""
        dag_defaults = {
            'fail_fast': False,
            'max_active_runs': 16,
            'auto_register': True
        }
        
        # Should filter defaults
        self.assertTrue(is_dag_default_value('fail_fast', False, dag_defaults))
        self.assertTrue(is_dag_default_value('max_active_runs', 16, dag_defaults))
        
        # Should not filter non-defaults
        self.assertFalse(is_dag_default_value('fail_fast', True, dag_defaults))
        self.assertFalse(is_dag_default_value('max_active_runs', 8, dag_defaults))
        self.assertFalse(is_dag_default_value('unknown_field', 'value', dag_defaults))

    @patch("dag_converter.conversion.converter.convert_tasks")
    @patch("dag_converter.conversion.converter.convert_schedule") 
    @patch("dag_converter.conversion.converter.convert_default_args")
    @patch("dag_converter.conversion.converter.get_dag_default_values")
    def test_default_values_filtered_out(self, mock_get_defaults, mock_convert_default, mock_convert_schedule, mock_convert_tasks):
        """Test that default values are excluded from output"""
        
        # Setup mocks
        mock_get_defaults.return_value = {'fail_fast': False, 'max_active_runs': 16}
        mock_convert_default.return_value = {}
        mock_convert_schedule.return_value = "@daily"
        mock_convert_tasks.return_value = {}
        self.mock_validator.validate_field.return_value = True

        # Create mock DAG with default values
        dag_object = Mock()
        dag_object.dag_id = "test_dag"
        dag_object.params = {}
        dag_object.fail_fast = False  # Default value
        dag_object.max_active_runs = 16  # Default value
        dag_object.catchup = True  # Non-default value
        del dag_object._dag_id

        with patch("builtins.dir", return_value=["dag_id", "params", "fail_fast", "max_active_runs", "catchup"]):
            result = get_converted_format(self.mock_taskflow_parser, dag_object, "test.py", self.mock_validator)

        dag_data = result["test_dag"]
        
        # Default values should be filtered out
        self.assertNotIn('fail_fast', dag_data)
        self.assertNotIn('max_active_runs', dag_data)
        
        # Non-default values should be included
        self.assertIn('catchup', dag_data)
        self.assertEqual(dag_data['catchup'], True)

    @patch("dag_converter.conversion.converter.convert_tasks")
    @patch("dag_converter.conversion.converter.convert_schedule") 
    @patch("dag_converter.conversion.converter.convert_default_args")
    @patch("dag_converter.conversion.converter.get_dag_default_values")
    def test_non_default_values_included(self, mock_get_defaults, mock_convert_default, mock_convert_schedule, mock_convert_tasks):
        """Test that non-default values like fail_fast=True are included in output"""
        
        # Setup mocks
        mock_get_defaults.return_value = {'fail_fast': False, 'max_active_runs': 16}
        mock_convert_default.return_value = {}
        mock_convert_schedule.return_value = "@daily"
        mock_convert_tasks.return_value = {}
        self.mock_validator.validate_field.return_value = True

        # Create mock DAG with non-default values
        dag_object = Mock()
        dag_object.dag_id = "test_dag"
        dag_object.params = {}
        dag_object.fail_fast = True  # Non-default (default is False)
        dag_object.max_active_runs = 8  # Non-default (default is 16)
        del dag_object._dag_id

        with patch("builtins.dir", return_value=["dag_id", "params", "fail_fast", "max_active_runs"]):
            result = get_converted_format(self.mock_taskflow_parser, dag_object, "test.py", self.mock_validator)

        dag_data = result["test_dag"]
        
        # Non-default values should be included
        self.assertIn('fail_fast', dag_data)
        self.assertIn('max_active_runs', dag_data)
        self.assertEqual(dag_data['fail_fast'], True)
        self.assertEqual(dag_data['max_active_runs'], 8)


if __name__ == "__main__":
    unittest.main()
    unittest.main()
