import unittest
from unittest.mock import Mock

from dag_converter.conversion.operators.python_decorator import (
    cleanup_xcom,
    extract_op_args,
    extract_op_kwargs,
    handle_python_decorator,
)


class TestHandlePythonDecorator(unittest.TestCase):
    def setUp(self):
        self.mock_taskflow_parser = Mock()
        self.mock_taskflow_parser.task_functions = {"test_function": ["param1", "param2", "param3"]}

    def test_basic_conversion(self):
        """Test basic python decorator conversion"""
        task_dict = {
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "python_callable": "test_file.test_function",
        }

        handle_python_decorator(task_dict, self.mock_taskflow_parser)

        self.assertNotIn("operator", task_dict)
        self.assertEqual(task_dict["decorator"], "airflow.decorators.task")

    def test_op_args_op_kwargs_handling(self):
        """Test handling of op_args and op_kwargs"""
        task_dict = {
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "python_callable": "test_file.test_function",
            "op_args": ["value1", "value2"],
            "op_kwargs": {"param3": "value3"},
        }

        handle_python_decorator(task_dict, self.mock_taskflow_parser)

        self.assertEqual(task_dict["param1"], "value1")
        self.assertEqual(task_dict["param2"], "value2")
        self.assertEqual(task_dict["param3"], "value3")
        self.assertNotIn("op_args", task_dict)
        self.assertNotIn("op_kwargs", task_dict)

    def test_partial_handling(self):
        """Test handling of partial parameters"""
        task_dict = {
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "python_callable": "test_file.test_function",
            "partial": {"op_args": ["value1"], "op_kwargs": {"param2": "value2"}},
        }

        handle_python_decorator(task_dict, self.mock_taskflow_parser)

        self.assertEqual(task_dict["partial"]["param1"], "value1")
        self.assertEqual(task_dict["partial"]["param2"], "value2")
        self.assertNotIn("op_args", task_dict["partial"])
        self.assertNotIn("op_kwargs", task_dict["partial"])

    def test_partial_without_op_args_kwargs(self):
        """Test partial handling when op_args/op_kwargs don't exist"""
        task_dict = {
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "python_callable": "test_file.test_function",
            "partial": {"other_param": "value"},
        }

        handle_python_decorator(task_dict, self.mock_taskflow_parser)

        self.assertEqual(task_dict["partial"]["other_param"], "value")


class TestExtractOpArgs(unittest.TestCase):
    def test_regular_args(self):
        """Test extraction of regular arguments"""
        op_args = ["value1", "value2", 123]
        result = extract_op_args(op_args)
        self.assertEqual(result, ["value1", "value2", 123])

    def test_xcom_args(self):
        """Test extraction with xcom references"""
        op_args = ["value1", "task_instance.xcom_pull(task_ids='task1')"]
        result = extract_op_args(op_args)
        self.assertEqual(result, ["value1", "+task1"])


class TestExtractOpKwargs(unittest.TestCase):
    def test_regular_kwargs(self):
        """Test extraction of regular keyword arguments"""
        op_kwargs = {"param1": "value1", "param2": 123}
        result = extract_op_kwargs(op_kwargs)
        self.assertEqual(result, {"param1": "value1", "param2": 123})

    def test_xcom_kwargs(self):
        """Test extraction with xcom references"""
        op_kwargs = {"param1": "value1", "param2": "task_instance.xcom_pull(task_ids='task2')"}
        result = extract_op_kwargs(op_kwargs)
        self.assertEqual(result, {"param1": "value1", "param2": "+task2"})


class TestCleanupXcom(unittest.TestCase):
    def test_return_value_key(self):
        """Test cleanup with default return_value key"""
        xcom_str = "task_instance.xcom_pull(task_ids='task1')"
        result = cleanup_xcom(xcom_str)
        self.assertEqual(result, "+task1")

    def test_explicit_return_value_key(self):
        """Test cleanup with explicit return_value key"""
        xcom_str = "task_instance.xcom_pull(task_ids='task1', key='return_value')"
        result = cleanup_xcom(xcom_str)
        self.assertEqual(result, "+task1")

    def test_custom_key(self):
        """Test cleanup with custom key"""
        xcom_str = "task_instance.xcom_pull(task_ids='task1', key='custom_key')"
        result = cleanup_xcom(xcom_str)
        self.assertEqual(result, xcom_str)


if __name__ == "__main__":
    unittest.main()
