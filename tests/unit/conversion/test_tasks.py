import unittest
from unittest.mock import Mock, patch

from airflow.providers.standard.decorators.python import _PythonDecoratedOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator

from dag_converter.conversion.tasks import convert_tasks, get_operator_parameters


class TestConvertTasks(unittest.TestCase):
    def setUp(self):
        self.mock_validator = Mock()
        self.mock_taskflow_parser = Mock()
        self.mock_dag_object = Mock()

    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_basic_task_conversion(self, mock_get_params, mock_handle_python, mock_cleanup):
        """Test basic task conversion"""
        # Setup mocks
        mock_get_params.return_value = ["param1", "retries"]
        self.mock_validator.validate_field.return_value = True

        # Create mock task
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "BashOperator"
        task.__module__ = "airflow.operators.bash"
        task.upstream_task_ids = ["upstream_task"]
        task._is_mapped = False
        task.param1 = "value1"
        task.retries = 3
        task._BaseOperator__init_kwargs = {"param1": "value1", "retries": 3}
        # Ensure _task_module doesn't exist
        del task._task_module

        # Mock dir() to return our attributes
        with patch("builtins.dir", return_value=["task_id", "task_type", "param1", "retries"]):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        expected = {
            "test_task": {
                "operator": "airflow.operators.bash.BashOperator",
                "param1": "value1",
                "retries": 3,
                "dependencies": ["upstream_task"],
            }
        }
        self.assertEqual(result, expected)

    @unittest.skip("Python operators not supported")
    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_python_decorated_operator(self, mock_get_params, mock_handle_python, mock_cleanup):
        """Test Python decorated operator handling"""
        mock_get_params.return_value = ["python_callable"]
        mock_cleanup.return_value = {
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "python_callable": "test.func",
            "dependencies": [],
        }
        self.mock_validator.validate_field.return_value = True

        task = Mock(spec=_PythonDecoratedOperator)
        task.task_id = "python_task"
        task.task_type = "_PythonDecoratedOperator"
        task.__module__ = "airflow.decorators.python"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.python_callable = "func"
        task._BaseOperator__init_kwargs = {}
        del task._task_module

        with patch("builtins.dir", return_value=["task_id", "task_type", "python_callable"]):
            self.mock_dag_object.tasks = [task]

            convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        mock_cleanup.assert_called()
        mock_handle_python.assert_called()

    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.handle_dynamic_task_mapping")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_dynamic_task_mapping(self, mock_get_params, mock_handle_dtm, mock_handle_python, mock_cleanup):
        """Test dynamic task mapping handling"""
        mock_get_params.return_value = ["param1"]
        self.mock_validator.validate_field.return_value = True

        task = Mock(spec=MappedOperator)
        task.task_id = "mapped_task"
        task.task_type = "BashOperator"
        task.__module__ = "airflow.operators.bash"
        task.upstream_task_ids = []
        task._is_mapped = True
        task.param1 = "value1"
        task._BaseOperator__init_kwargs = {}

        with patch("builtins.dir", return_value=["task_id", "task_type", "param1"]):
            self.mock_dag_object.tasks = [task]

            convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        mock_handle_dtm.assert_called()

    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_task_module_handling(self, mock_get_params, mock_handle_python, mock_cleanup):
        """Test _task_module attribute handling"""
        mock_get_params.return_value = ["param1"]
        self.mock_validator.validate_field.return_value = True

        task = Mock(spec=MappedOperator)
        task.task_id = "dtm_task"
        task.task_type = "BashOperator"
        task._task_module = "airflow.operators.bash"
        task.__module__ = "different.module"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.param1 = "value1"
        task.expand_input.value = {"op_kwargs": {"param1": "value1"}}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}
        task._BaseOperator__init_kwargs = {}

        with patch("builtins.dir", return_value=["task_id", "task_type", "param1"]):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        self.assertEqual(result["dtm_task"]["operator"], "airflow.operators.bash.BashOperator")

    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_special_operator_cleanup(self, mock_get_params, mock_handle_python, mock_cleanup):
        """Test special operator field cleanup"""
        mock_get_params.return_value = ["flow_update", "config"]

        def validate_side_effect(section, key, value):
            return key in ["flow_update", "config"]

        self.mock_validator.validate_field.side_effect = validate_side_effect

        # Test AppFlow operator
        task1 = Mock()
        task1.task_id = "appflow_task"
        task1.task_type = "AppFlowOperator"
        task1.__module__ = "airflow.providers.amazon.aws.operators.appflow"
        task1.upstream_task_ids = []
        task1._is_mapped = False
        task1.flow_update = "update"
        task1._BaseOperator__init_kwargs = {}
        del task1._task_module

        # Test SageMaker operator
        task2 = Mock()
        task2.task_id = "sagemaker_task"
        task2.task_type = "SageMakerStartPipelineOperator"
        task2.__module__ = "airflow.providers.amazon.aws.operators.sagemaker"
        task2.upstream_task_ids = []
        task2._is_mapped = False
        task2.config = "config"
        task2._BaseOperator__init_kwargs = {}
        del task2._task_module

        with patch("builtins.dir", return_value=["task_id", "task_type", "flow_update", "config"]):
            self.mock_dag_object.tasks = [task1, task2]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        # flow_update should be removed from appflow task
        self.assertNotIn("flow_update", result["appflow_task"])
        # config should be removed from sagemaker task
        self.assertNotIn("config", result["sagemaker_task"])

    @patch("dag_converter.conversion.tasks.get_cleanup_dag")
    @patch("dag_converter.conversion.tasks.handle_python_decorator")
    @patch("dag_converter.conversion.tasks.get_operator_parameters")
    def test_skip_dtm_keywords(self, mock_get_params, mock_handle_python, mock_cleanup):
        """Test skipping DTM keywords"""
        mock_get_params.return_value = ["op_kwargs_expand_input", "expand_input", "partial_kwargs", "param1"]
        self.mock_validator.validate_field.return_value = True

        task = Mock()
        task.task_id = "test_task"
        task.task_type = "BashOperator"
        task.__module__ = "airflow.operators.bash"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.op_kwargs_expand_input = "expand"
        task.expand_input = "expand"
        task.partial_kwargs = "partial"
        task.param1 = "value1"
        task._BaseOperator__init_kwargs = {}

        with patch(
            "builtins.dir",
            return_value=["task_id", "task_type", "op_kwargs_expand_input", "expand_input", "partial_kwargs", "param1"],
        ):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        # DTM keywords should not be in result
        self.assertNotIn("op_kwargs_expand_input", result["test_task"])
        self.assertNotIn("expand_input", result["test_task"])
        self.assertNotIn("partial_kwargs", result["test_task"])
        # Regular param should be included
        self.assertIn("param1", result["test_task"])


class TestGetOperatorParameters(unittest.TestCase):
    """Test cases for the get_operator_parameters function"""

    def test_s3_operator_parameters(self):
        """Test that get_operator_parameters returns expected parameters for S3Operator"""
        # Test with S3CreateBucketOperator as an example S3 operator
        operator_string = "airflow.providers.amazon.aws.operators.s3.S3CreateBucketOperator"

        parameters = get_operator_parameters(operator_string)

        # Verify that the function returns a list
        self.assertIsInstance(parameters, list)

        # Verify that common S3 operator parameters are included
        expected_params = ["bucket_name", "aws_conn_id", "region_name"]
        for param in expected_params:
            self.assertIn(param, parameters, f"Parameter '{param}' should be in S3Operator parameters")

        # Verify that basic operator parameters are also included
        basic_params = ["task_id", "retries", "retry_delay"]
        for param in basic_params:
            self.assertIn(param, parameters, f"Basic parameter '{param}' should be in operator parameters")

    def test_empty_operator_parameters(self):
        """Test that get_operator_parameters returns expected parameters for EmptyOperator"""
        operator_string = "airflow.providers.standard.operators.empty.EmptyOperator"

        parameters = get_operator_parameters(operator_string)

        # Verify that the function returns a list
        self.assertIsInstance(parameters, list)

        # Verify that basic operator parameters are included
        expected_params = ["task_id", "retries", "retry_delay", "depends_on_past", "wait_for_downstream"]
        for param in expected_params:
            self.assertIn(param, parameters, f"Parameter '{param}' should be in EmptyOperator parameters")

        # Verify that the list is not empty
        self.assertGreater(len(parameters), 0, "EmptyOperator should have at least some parameters")
