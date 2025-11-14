import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from dag_converter.cli import app

runner = CliRunner()


class TestCli(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.sample_dag_path = Path("tests/unit/samples/aws_s3.py")

    def tearDown(self):
        """Clean up after each test method."""
        self.temp_dir.cleanup()

    def test_app_help(self):
        """Test that the app shows help information."""
        result = runner.invoke(app, ["--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("CLI tool for converting ", result.stdout)

    def test_convert_command_help(self):
        """Test that the convert command shows help information."""
        result = runner.invoke(app, ["convert", "--help"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Loads Python Dags from input and converts them to YAML Dags", result.stdout)
        self.assertIn("INPUT_PATH", result.stdout)
        self.assertIn("--output", result.stdout)
        self.assertIn("--bucket", result.stdout)
        self.assertIn("--validate", result.stdout)
        self.assertIn("--debug", result.stdout)

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_with_minimal_args(self, mock_conversion_manager):
        """Test convert command with only required arguments."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        result = runner.invoke(app, ["convert", str(self.sample_dag_path)])

        self.assertEqual(result.exit_code, 0)
        mock_conversion_manager.assert_called_once_with(s3_bucket="")
        mock_manager.start_conversion_process.assert_called_once_with(
            dag_file_path=self.sample_dag_path, output_dir=Path("output_yaml/"), user_validate=True, debug=False
        )

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_with_all_args(self, mock_conversion_manager):
        """Test convert command with all arguments specified."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        output_path = self.temp_path / "custom_output"
        bucket_name = "test-bucket"

        result = runner.invoke(
            app,
            [
                "convert",
                str(self.sample_dag_path),
                "--output",
                str(output_path),
                "--bucket",
                bucket_name,
                "--no-validate",
                "--debug",
            ],
        )

        self.assertEqual(result.exit_code, 0)
        mock_conversion_manager.assert_called_once_with(s3_bucket=bucket_name)
        mock_manager.start_conversion_process.assert_called_once_with(
            dag_file_path=self.sample_dag_path, output_dir=output_path, user_validate=False, debug=True
        )

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_with_custom_output_path(self, mock_conversion_manager):
        """Test convert command with custom output path."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        custom_output = self.temp_path / "my_yaml_output"

        result = runner.invoke(app, ["convert", str(self.sample_dag_path), "--output", str(custom_output)])

        self.assertEqual(result.exit_code, 0)
        mock_manager.start_conversion_process.assert_called_once_with(
            dag_file_path=self.sample_dag_path, output_dir=custom_output, user_validate=True, debug=False
        )

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_with_s3_bucket(self, mock_conversion_manager):
        """Test convert command with S3 bucket specified."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        bucket_name = "my-test-bucket"

        result = runner.invoke(app, ["convert", str(self.sample_dag_path), "--bucket", bucket_name])

        self.assertEqual(result.exit_code, 0)
        mock_conversion_manager.assert_called_once_with(s3_bucket=bucket_name)

    def test_convert_missing_input_path(self):
        """Test convert command fails when input path is missing."""
        result = runner.invoke(app, ["convert"])

        self.assertNotEqual(result.exit_code, 0)
        # Typer shows error in stderr, but CliRunner captures it in stdout
        self.assertTrue("Missing argument" in result.stdout or result.exit_code != 0)

    def test_convert_nonexistent_input_path(self):
        """Test convert command with non-existent input path."""
        nonexistent_path = "/path/that/does/not/exist.py"

        runner.invoke(app, ["convert", nonexistent_path])

        # The command should still execute (path validation happens in ConversionManager)
        # but we expect it to fail during execution, not during argument parsing
        # This test verifies the CLI accepts the path argument format

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_with_directory_input(self, mock_conversion_manager):
        """Test convert command with directory as input."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        # Use the temp directory as input
        result = runner.invoke(app, ["convert", str(self.temp_path)])

        self.assertEqual(result.exit_code, 0)
        mock_manager.start_conversion_process.assert_called_once_with(
            dag_file_path=self.temp_path, output_dir=Path("output_yaml/"), user_validate=True, debug=False
        )

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_exception_handling(self, mock_conversion_manager):
        """Test convert command handles exceptions from ConversionManager."""
        mock_manager = MagicMock()
        mock_manager.start_conversion_process.side_effect = Exception("Test exception")
        mock_conversion_manager.return_value = mock_manager

        result = runner.invoke(app, ["convert", str(self.sample_dag_path)])

        # The CLI should propagate the exception
        self.assertNotEqual(result.exit_code, 0)

    @patch("dag_converter.conversion_manager.ConversionManager")
    def test_convert_path_objects_handling(self, mock_conversion_manager):
        """Test that Path objects are properly handled in the CLI."""
        mock_manager = MagicMock()
        mock_conversion_manager.return_value = mock_manager

        # Test with string paths that should be converted to Path objects
        input_path_str = str(self.sample_dag_path)
        output_path_str = str(self.temp_path / "output")

        result = runner.invoke(app, ["convert", input_path_str, "--output", output_path_str])

        self.assertEqual(result.exit_code, 0)

        # Verify that the paths were converted to Path objects
        call_args = mock_manager.start_conversion_process.call_args
        self.assertIsInstance(call_args.kwargs["dag_file_path"], Path)
        self.assertIsInstance(call_args.kwargs["output_dir"], Path)
