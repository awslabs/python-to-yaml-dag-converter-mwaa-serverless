import os
import shutil
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

from dag_converter.conversion_manager import ConversionManager


class TestConversionManager(unittest.TestCase):
    """Test cases for ConversionManager class"""

    def setUp(self):
        print("path", sys.path)
        """Set up test fixtures before each test method."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.temp_output_dir = Path(tempfile.mkdtemp())

        # Reference to sample files in the samples folder
        samples_dir = Path(__file__).parent.parent / "samples"

        # Copy sample files to temp directory for testing
        self.sample_dag_file = self.temp_dir / "sample_dag.py"
        python_operator_sample = samples_dir / "python_operator_test.py"
        shutil.copy2(python_operator_sample, self.sample_dag_file)

        # Copy multiple DAGs sample file for batch testing
        self.sample_dag2_file = self.temp_dir / "bash_dag.py"
        multiple_dags_sample = samples_dir / "multiple_dags_test.py"
        shutil.copy2(multiple_dags_sample, self.sample_dag2_file)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.temp_output_dir)

    @patch("dag_converter.conversion_manager.get_dag_object")
    @patch("dag_converter.conversion_manager.get_converted_format")
    @patch("dag_converter.conversion_manager.get_cleanup_dag")
    @patch("dag_converter.conversion_manager.build_yaml")
    @patch("dag_converter.conversion_manager.ArgumentValidator")
    @patch("dag_converter.conversion_manager.TaskFlowAnalyzer")
    def test_single_file_conversion_success(
        self,
        mock_taskflow,
        mock_validator_class,
        mock_build_yaml,
        mock_cleanup,
        mock_convert,
        mock_get_dag,
    ):
        """Test successful conversion of a single Python DAG file to YAML"""

        # Setup mocks
        mock_dag = Mock()
        mock_dag.dag_id = "sample_dag"
        mock_dag.tasks = []
        mock_get_dag.return_value = [mock_dag]

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        mock_convert.return_value = {"sample_dag": {"dag_id": "sample_dag"}}
        mock_cleanup.return_value = {"sample_dag": {"dag_id": "sample_dag"}}
        mock_build_yaml.return_value = "dag_id: sample_dag\n"

        # Create ConversionManager instance
        manager = ConversionManager(s3_bucket=None)

        # Test single file conversion
        manager.start_conversion_process(
            dag_file_path=self.sample_dag_file, output_dir=self.temp_output_dir, user_validate=False
        )

        # Verify the conversion process was called
        mock_get_dag.assert_called_once()
        mock_convert.assert_called_once()
        mock_cleanup.assert_called_once()
        mock_build_yaml.assert_called_once()

        # Verify output file was created
        output_file = self.temp_output_dir / "sample_dag.yaml"
        self.assertTrue(output_file.exists())

        # Verify file content
        with open(output_file) as f:
            content = f.read()
            self.assertEqual(content, mock_build_yaml.return_value)

    @patch("dag_converter.conversion_manager.get_dag_object")
    @patch("dag_converter.conversion_manager.get_converted_format")
    @patch("dag_converter.conversion_manager.get_cleanup_dag")
    @patch("dag_converter.conversion_manager.build_yaml")
    @patch("dag_converter.conversion_manager.ArgumentValidator")
    @patch("dag_converter.conversion_manager.TaskFlowAnalyzer")
    def test_batch_conversion_success(
        self,
        mock_taskflow,
        mock_validator_class,
        mock_build_yaml,
        mock_cleanup,
        mock_convert,
        mock_get_dag,
    ):
        """Test successful batch conversion of multiple Python DAG files to YAML"""

        # Setup mocks for first DAG
        mock_dag1 = Mock()
        mock_dag1.dag_id = "sample_dag"
        mock_dag1.tasks = []

        # Setup mocks for second DAG
        mock_dag2 = Mock()
        mock_dag2.dag_id = "bash_dag"
        mock_dag2.tasks = []

        # Mock get_dag_object to return different DAGs for different files
        def mock_get_dag_side_effect(file_path):
            if "sample_dag.py" in str(file_path):
                return [mock_dag1]
            elif "bash_dag.py" in str(file_path):
                return [mock_dag2]
            return []

        mock_get_dag.side_effect = mock_get_dag_side_effect

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Mock conversion results
        def mock_convert_side_effect(*args):
            dag_obj = args[1]  # Second argument is dag_object
            if dag_obj.dag_id == "sample_dag":
                return {"sample_dag": {"dag_id": "sample_dag"}}
            elif dag_obj.dag_id == "bash_dag":
                return {"bash_dag": {"dag_id": "bash_dag"}}
            return {}

        mock_convert.side_effect = mock_convert_side_effect
        mock_cleanup.side_effect = lambda x: x  # Return input unchanged

        def mock_build_yaml_side_effect(dag_dict):
            dag_id = list(dag_dict.keys())[0]
            return f"dag_id: {dag_id}\n"

        mock_build_yaml.side_effect = mock_build_yaml_side_effect

        # Create ConversionManager instance
        manager = ConversionManager(s3_bucket=None)

        # Test batch conversion (directory input)
        manager.start_conversion_process(
            dag_file_path=self.temp_dir, output_dir=self.temp_output_dir, user_validate=False
        )

        # Verify both files were processed
        self.assertEqual(mock_get_dag.call_count, 2)
        self.assertEqual(mock_convert.call_count, 2)

        # Verify output files were created
        output_file1 = self.temp_output_dir / "sample_dag.yaml"
        output_file2 = self.temp_output_dir / "bash_dag.yaml"

        self.assertTrue(output_file1.exists())
        self.assertTrue(output_file2.exists())

        # Verify file contents
        with open(output_file1) as f:
            content1 = f.read()
            self.assertEqual(content1, "dag_id: sample_dag\n")

        with open(output_file2) as f:
            content2 = f.read()
            self.assertEqual(content2, "dag_id: bash_dag\n")

    def test_file_not_found_error(self):
        """Test that FileNotFoundError is raised when input file doesn't exist"""

        manager = ConversionManager(s3_bucket=None)

        with self.assertRaises(FileNotFoundError):
            manager.start_conversion_process(
                dag_file_path=Path("/nonexistent/file.py"), output_dir=self.temp_output_dir
            )

    def test_invalid_output_directory_error(self):
        """Test that ValueError is raised when output path is a file instead of directory"""

        # Create a file instead of directory for output
        output_file = self.temp_dir / "output.txt"
        with open(output_file, "w") as f:
            f.write("test")

        manager = ConversionManager(s3_bucket=None)

        with self.assertRaises(ValueError):
            manager.start_conversion_process(dag_file_path=self.sample_dag_file, output_dir=output_file)

    @patch("dag_converter.conversion_manager.boto3")
    @patch("dag_converter.conversion_manager.get_dag_object")
    @patch("dag_converter.conversion_manager.get_converted_format")
    @patch("dag_converter.conversion_manager.get_cleanup_dag")
    @patch("dag_converter.conversion_manager.build_yaml")
    @patch("dag_converter.conversion_manager.ArgumentValidator")
    @patch("dag_converter.conversion_manager.TaskFlowAnalyzer")
    def test_s3_upload_success(
        self,
        mock_taskflow,
        mock_validator_class,
        mock_build_yaml,
        mock_cleanup,
        mock_convert,
        mock_get_dag,
        mock_boto3,
    ):
        """Test successful S3 upload functionality"""

        # Setup mocks
        mock_s3_client = Mock()
        mock_boto3.client.return_value = mock_s3_client

        mock_dag = Mock()
        mock_dag.dag_id = "sample_dag"
        mock_dag.tasks = []
        mock_get_dag.return_value = [mock_dag]

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        mock_convert.return_value = {"sample_dag": {"dag_id": "sample_dag"}}
        mock_cleanup.return_value = {"sample_dag": {"dag_id": "sample_dag"}}
        mock_build_yaml.return_value = "dag_id: sample_dag\n"

        # Create ConversionManager instance with S3 bucket
        manager = ConversionManager(s3_bucket="test-bucket")

        # Test conversion with S3 upload
        manager.start_conversion_process(
            dag_file_path=self.sample_dag_file, output_dir=self.temp_output_dir, user_validate=False
        )

        # Verify S3 upload was called
        mock_s3_client.upload_file.assert_called_once()

        # Verify the upload call arguments
        call_args = mock_s3_client.upload_file.call_args[0]
        self.assertTrue(str(call_args[0]).endswith("sample_dag.yaml"))  # source file
        self.assertEqual(call_args[1], "test-bucket")  # bucket name
        self.assertEqual(call_args[2], "sample_dag.yaml")  # destination key

    @patch("dag_converter.conversion_manager.get_dag_object")
    @patch("dag_converter.conversion_manager.get_converted_format")
    @patch("dag_converter.conversion_manager.get_cleanup_dag")
    @patch("dag_converter.conversion_manager.build_yaml")
    @patch("dag_converter.conversion_manager.ArgumentValidator")
    @patch("dag_converter.conversion_manager.TaskFlowAnalyzer")
    def test_multiple_dags_in_single_file(
        self,
        mock_taskflow,
        mock_validator_class,
        mock_build_yaml,
        mock_cleanup,
        mock_convert,
        mock_get_dag,
    ):
        """Test conversion of multiple DAGs in a single Python file"""

        # Setup mocks for multiple DAGs in one file
        mock_dag1 = Mock()
        mock_dag1.dag_id = "dag_1"
        mock_dag1.tasks = []

        mock_dag2 = Mock()
        mock_dag2.dag_id = "dag_2"
        mock_dag2.tasks = []

        mock_get_dag.return_value = [mock_dag1, mock_dag2]

        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator

        # Mock conversion results for each DAG
        def mock_convert_side_effect(*args):
            dag_obj = args[1]  # Second argument is dag_object
            return {dag_obj.dag_id: {"dag_id": dag_obj.dag_id}}

        mock_convert.side_effect = mock_convert_side_effect
        mock_cleanup.side_effect = lambda x: x  # Return input unchanged

        def mock_build_yaml_side_effect(dag_dict):
            dag_id = list(dag_dict.keys())[0]
            return f"dag_id: {dag_id}\n"

        mock_build_yaml.side_effect = mock_build_yaml_side_effect

        # Create ConversionManager instance
        manager = ConversionManager(s3_bucket=None)

        # Test conversion
        manager.start_conversion_process(
            dag_file_path=self.sample_dag_file, output_dir=self.temp_output_dir, user_validate=False
        )

        self.assertEqual(mock_convert.call_count, 2)
        self.assertEqual(mock_build_yaml.call_count, 2)

        # Verify output files were created for both DAGs
        output_file1 = self.temp_output_dir / "dag_1.yaml"
        output_file2 = self.temp_output_dir / "dag_2.yaml"

        self.assertTrue(output_file1.exists())
        self.assertTrue(output_file2.exists())

        # Verify file contents
        with open(output_file1) as f:
            content1 = f.read()
            self.assertEqual(content1, "dag_id: dag_1\n")

        with open(output_file2) as f:
            content2 = f.read()
            self.assertEqual(content2, "dag_id: dag_2\n")


class TestConversionManagerPackage(unittest.TestCase):
    """Real conversions with code packages and validation (third-party packages are not installed)"""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name) / "dags"
        (self.root / "helpers_cm").mkdir(parents=True)
        (self.root / "helpers_cm" / "__init__.py").write_text("")
        (self.root / "helpers_cm" / "mod.py").write_text("def func():\n    return 1\n")
        self.output = Path(self.temp_dir.name) / "out"
        self.old_sys_path = list(sys.path)
        self.old_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)  # the project root, so ./dags is the Dag folder

    def tearDown(self):
        os.chdir(self.old_cwd)
        for name in [n for n in sys.modules if n.startswith("helpers_cm")]:
            del sys.modules[name]
        self.temp_dir.cleanup()

    def convert(self, path):
        with patch("dag_converter.code_package.third_party_distributions", return_value={}):
            ConversionManager(s3_bucket=None).start_conversion_process(
                dag_file_path=path, output_dir=self.output, user_validate=True
            )

    def test_multiple_dags_in_one_file_validate(self):
        (self.root / "two_cm.py").write_text(
            "from datetime import datetime\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.python import PythonOperator\n"
            "from helpers_cm.mod import func\n"
            "for i in (1, 2):\n"
            "    with DAG(f'cm_d{i}', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "        PythonOperator(task_id='t', python_callable=func)\n"
            "    globals()[f'dag{i}'] = dag\n"
        )
        self.convert(self.root)
        for dag_id in ("cm_d1", "cm_d2"):
            self.assertIn("python_callable: helpers_cm.mod.func", (self.output / f"{dag_id}.yaml").read_text())
            names = zipfile.ZipFile(self.output / f"{dag_id}.zip").namelist()
            self.assertEqual(sorted(names), ["helpers_cm/__init__.py", "helpers_cm/mod.py"])
        self.assertNotIn("helpers_cm.mod", sys.modules)

    def test_no_code_package_needed(self):
        (self.root / "plain_cm.py").write_text(
            "from datetime import datetime\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.bash import BashOperator\n"
            "with DAG('plain_cm', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "    BashOperator(task_id='t', bash_command='echo hi')\n"
        )
        self.output.mkdir()
        (self.output / "plain_cm.zip").write_bytes(b"stale")
        with (
            patch("dag_converter.code_package.stage_distributions") as stage,
            patch("dag_converter.code_package.print_info") as info,
            patch("dag_converter.conversion_manager.print_success") as success,
            patch("dag_converter.conversion_manager.validate_yaml_with_dagbag", return_value=(True, "")) as validate,
        ):
            self.convert(self.root / "plain_cm.py")
        stage.assert_not_called()
        self.assertFalse((self.output / "plain_cm.zip").exists())
        self.assertIn("No code package needed for plain_cm", [c.args[0] for c in info.call_args_list])
        self.assertIsNone(validate.call_args.kwargs["import_path"])
        self.assertIn("YAML validation successful, no errors found", [c.args[0] for c in success.call_args_list])
        self.assertIn("bash_command: echo hi", (self.output / "plain_cm.yaml").read_text())

    def test_local_helper_gets_a_code_package(self):
        (self.root / "helper_cm.py").write_text(
            "from datetime import datetime\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.python import PythonOperator\n"
            "from helpers_cm.mod import func\n"
            "with DAG('helper_cm', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "    PythonOperator(task_id='t', python_callable=func)\n"
        )
        self.convert(self.root / "helper_cm.py")
        names = zipfile.ZipFile(self.output / "helper_cm.zip").namelist()
        self.assertEqual(sorted(names), ["helpers_cm/__init__.py", "helpers_cm/mod.py"])
        # The Dag folder is only on the import path during the conversion
        self.assertEqual(sys.path, self.old_sys_path)

    def test_script_path_is_packaged_and_rewritten(self):
        (self.root / "scripts").mkdir()
        (self.root / "scripts" / "run.sh").write_text("echo hi\n")
        (self.root / "script_cm.py").write_text(
            "from datetime import datetime\n"
            "from pathlib import Path\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.bash import BashOperator\n"
            "with DAG('script_cm', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "    BashOperator(task_id='t', bash_command=f'bash {Path(__file__).parent}/scripts/run.sh ')\n"
        )
        self.convert(Path("dags") / "script_cm.py")  # relative to the project root
        self.assertIn(
            "bash_command: 'bash /usr/local/airflow/dags/scripts/run.sh '", (self.output / "script_cm.yaml").read_text()
        )
        self.assertEqual(zipfile.ZipFile(self.output / "script_cm.zip").namelist(), ["scripts/run.sh"])

    def test_config_file_read_while_parsing_is_packaged(self):
        (self.root / "configs").mkdir()
        (self.root / "configs" / "names.txt").write_text("a\nb\n")
        (self.root / "config_cm.py").write_text(
            "from datetime import datetime\n"
            "from pathlib import Path\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.python import PythonOperator\n"
            "NAMES = (Path(__file__).parent / 'configs' / 'names.txt').read_text().split()\n"
            "def hello():\n"
            "    return 1\n"
            "for name in NAMES:\n"
            "    with DAG(f'config_cm_{name}', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "        PythonOperator(task_id='t', python_callable=hello)\n"
            "    globals()[name] = dag\n"
        )
        # The callable imports the Dag file from the zip at task time, which reads the config again
        self.convert(self.root / "config_cm.py")
        for name in ("a", "b"):
            names = zipfile.ZipFile(self.output / f"config_cm_{name}.zip").namelist()
            self.assertEqual(sorted(names), ["config_cm.py", "configs/names.txt"])

    @patch("dag_converter.conversion_manager.boto3")
    def test_bucket_upload_includes_code_package(self, mock_boto3):
        (self.root / "upload_cm.py").write_text(
            "from datetime import datetime\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.python import PythonOperator\n"
            "from helpers_cm.mod import func\n"
            "with DAG('upload_cm', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "    PythonOperator(task_id='t', python_callable=func)\n"
        )
        with patch("dag_converter.code_package.third_party_distributions", return_value={}):
            ConversionManager(s3_bucket="bucket").start_conversion_process(
                dag_file_path=self.root / "upload_cm.py", output_dir=self.output, user_validate=False
            )
        uploads = [c.args for c in mock_boto3.client.return_value.upload_file.call_args_list]
        self.assertEqual(
            uploads,
            [
                (self.output / "upload_cm.yaml", "bucket", "upload_cm.yaml"),
                (self.output / "upload_cm.zip", "bucket", "upload_cm.zip"),
            ],
        )

    def test_subfolder_dag_callable(self):
        (self.root / "sub").mkdir()
        (self.root / "sub" / "sub_cm.py").write_text(
            "from datetime import datetime\n"
            "from airflow import DAG\n"
            "from airflow.providers.standard.operators.python import PythonOperator\n"
            "def hello():\n"
            "    return 1\n"
            "with DAG('sub_cm', start_date=datetime(2024, 1, 1), schedule=None) as dag:\n"
            "    PythonOperator(task_id='t', python_callable=hello)\n"
        )
        self.convert(self.root / "sub" / "sub_cm.py")
        self.assertIn("python_callable: sub.sub_cm.hello", (self.output / "sub_cm.yaml").read_text())
        self.assertEqual(zipfile.ZipFile(self.output / "sub_cm.zip").namelist(), ["sub/sub_cm.py"])


if __name__ == "__main__":
    unittest.main()
