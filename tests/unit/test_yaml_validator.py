import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from dag_converter.yaml_validator import validate_yaml_with_dagbag


class TestYamlValidator(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_dag_path = Path("tests/unit/samples/aws_s3.py")

        # Create a simple test DAG
        self.test_dag = DAG(dag_id="test_dag", start_date=datetime(2024, 1, 1), schedule="@daily")

        # Add a simple task to the DAG
        self.test_task = EmptyOperator(task_id="test_task", dag=self.test_dag)

        # Load YAML content from external files
        self.valid_yaml_path = Path("tests/unit/samples/valid_test_dag.yaml")
        self.invalid_yaml_path = Path("tests/unit/samples/invalid_test_dag.yaml")

        with open(self.valid_yaml_path) as f:
            self.valid_yaml = f.read()

        with open(self.invalid_yaml_path) as f:
            self.invalid_yaml = f.read()

    def test_validate_yaml_with_dagbag_success(self):
        """Test successful validation with valid YAML and DAG."""
        mock_dag = MagicMock()
        mock_dag.is_dagfactory_auto_generated = True

        with (
            patch("dag_converter.yaml_validator.get_dag_factory_object", return_value=mock_dag),
            patch("dag_converter.yaml_validator.compare_dags") as mock_compare,
        ):
            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertTrue(result)
            self.assertEqual(message, "YAML is valid and DAGs were successfully loaded")
            mock_compare.assert_called_once_with(self.test_dag, mock_dag)

    def test_validate_yaml_with_dagbag_dagfactory_failure(self):
        """Test validation when dagfactory fails to load DAGs."""
        with (
            patch(
                "dag_converter.yaml_validator.get_dag_factory_object",
                side_effect=Exception("Failed to generate dag object"),
            ),
            patch("dag_converter.yaml_validator.print_error") as mock_print_error,
        ):
            result, message = validate_yaml_with_dagbag(self.test_dag, self.invalid_yaml, self.sample_dag_path)

            self.assertFalse(result)
            self.assertEqual(message, "No DAGs were loaded from the YAML file. Check for missing required parameters.")
            mock_print_error.assert_called_once_with("Failed to generate dag object")

    def test_validate_yaml_with_dagbag_temp_file_creation(self):
        """Test that temporary files are created correctly."""
        mock_dag = MagicMock()
        mock_dag.is_dagfactory_auto_generated = True

        with (
            patch("dag_converter.yaml_validator.get_dag_factory_object", return_value=mock_dag),
            patch("tempfile.TemporaryDirectory") as mock_temp_dir,
            patch("builtins.open", mock_open()) as mock_file,
        ):
            # Mock the temporary directory
            mock_temp_dir.return_value.__enter__.return_value = "/tmp/test_dir"

            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertTrue(result)
            # Verify that files were written (YAML and Python loader)
            self.assertEqual(mock_file.call_count, 2)

    def test_validate_yaml_with_dagbag_file_write_error(self):
        """Test handling of file write errors."""
        with (
            patch("tempfile.TemporaryDirectory") as mock_temp_dir,
            patch("builtins.open", side_effect=OSError("Permission denied")),
        ):
            mock_temp_dir.return_value.__enter__.return_value = "/tmp/test_dir"

            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertFalse(result)
            self.assertIn("Error during validation setup", message)

    def test_validate_yaml_with_dagbag_dagfactory_called(self):
        """Test that dagfactory validation is properly called."""
        mock_dag = MagicMock()
        mock_dag.is_dagfactory_auto_generated = True

        with patch("dag_converter.yaml_validator.get_dag_factory_object", return_value=mock_dag) as mock_get_dag:
            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertTrue(result)
            # Verify that get_dag_factory_object was called, which means dagfactory was invoked
            mock_get_dag.assert_called_once()

    def test_validate_yaml_with_dagbag_compare_dags_called(self):
        """Test that compare_dags is called when initial DAG is provided."""
        mock_dag = MagicMock()
        mock_dag.is_dagfactory_auto_generated = True

        with (
            patch("dag_converter.yaml_validator.get_dag_factory_object", return_value=mock_dag),
            patch("dag_converter.yaml_validator.compare_dags") as mock_compare,
        ):
            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertTrue(result)
            mock_compare.assert_called_once_with(self.test_dag, mock_dag)

    def test_validate_yaml_with_dagbag_exception_in_validation_setup(self):
        """Test handling of exceptions during validation setup."""
        with patch("tempfile.TemporaryDirectory") as mock_temp_dir:
            # Make the context manager raise an exception
            mock_temp_dir.return_value.__enter__.side_effect = Exception("Setup error")

            result, message = validate_yaml_with_dagbag(self.test_dag, self.valid_yaml, self.sample_dag_path)

            self.assertFalse(result)
            self.assertIn("Unexpected error", message)
            self.assertIn("Setup error", message)


if __name__ == "__main__":
    unittest.main()
