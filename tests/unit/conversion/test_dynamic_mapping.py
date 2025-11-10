import unittest
from unittest.mock import Mock, patch

from dag_converter.conversion.dynamic_mapping import handle_dynamic_task_mapping, reformat


class TestHandleDynamicTaskMapping(unittest.TestCase):
    def setUp(self):
        self.mock_validator = Mock()
        self.task_type = {"task1": "_PythonDecoratedOperator", "task2": "BashOperator"}

    def test_op_kwargs_expand_input(self):
        """Test handling op_kwargs_expand_input"""

        task = Mock()
        task.op_kwargs_expand_input.value = {"param1": "value1"}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, self.mock_validator)

        self.assertEqual(tasks_dict["expand"], {"param1": "value1"})

    def test_expand_input(self):
        """Test handling expand_input"""

        task = Mock()
        task.expand_input = Mock()
        task.expand_input.value = {"op_kwargs": {"param1": "value1"}}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}
        del task.op_kwargs_expand_input

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, self.mock_validator)

        self.assertEqual(tasks_dict["expand"], {"op_kwargs": {"param1": "value1"}})

    @patch("dag_converter.conversion.dynamic_mapping.get_cleanup_dag")
    def test_partial_kwargs(self, mock_get_cleanup):
        """Test handling partial_kwargs"""
        mock_get_cleanup.return_value = {"python_callable": "func", "retries": 3}
        self.mock_validator.validate_field.return_value = True

        task = Mock()
        task.partial_kwargs = {"python_callable": "func", "retries": 3}
        del task.op_kwargs_expand_input
        del task.expand_input

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, self.mock_validator)

        self.assertEqual(tasks_dict["python_callable"], "func")
        self.assertEqual(tasks_dict["partial"], {"retries": 3})

    @patch("dag_converter.conversion.dynamic_mapping.get_cleanup_dag")
    def test_partial_kwargs_cleanup(self, mock_get_cleanup):
        """Test cleanup of invalid partial_kwargs"""
        mock_get_cleanup.return_value = {"valid_param": "value", "invalid_param": "value", "none_param": None}

        def validate_side_effect(section, key, value):
            return key == "valid_param"

        self.mock_validator.validate_field.side_effect = validate_side_effect

        task = Mock()
        task.partial_kwargs = {"valid_param": "value", "invalid_param": "value", "none_param": None}
        del task.op_kwargs_expand_input
        del task.expand_input

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, self.mock_validator)

        self.assertEqual(tasks_dict["partial"], {"valid_param": "value"})


class TestReformat(unittest.TestCase):
    def setUp(self):
        self.task_type = {"task1": "_PythonDecoratedOperator", "task2": "BashOperator"}

    def test_reformat_dict_python_decorated(self):
        """Test reformatting dict with PythonDecoratedOperator"""
        expand_dict = {"param": "task_instance.xcom_pull(task_ids='task1')"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "+task1")

    def test_reformat_dict_regular_operator(self):
        """Test reformatting dict with regular operator"""
        expand_dict = {"param": "task_instance.xcom_pull(task_ids='task2')"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "task2.output")

    def test_reformat_list(self):
        """Test reformatting list"""
        expand_dict = ["task_instance.xcom_pull(task_ids='task1')", "task_instance.xcom_pull(task_ids='task2')"]

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict[0], "+task1")
        self.assertEqual(expand_dict[1], "task2.output")

    def test_reformat_no_xcom(self):
        """Test reformatting with no xcom references"""
        expand_dict = {"param": "regular_value"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "regular_value")

    def test_reformat_no_task_id_match(self):
        """Test reformatting when task_id regex doesn't match"""
        expand_dict = {"param": "task_instance.xcom_pull(invalid_format)"}

        # This should raise a KeyError because task_id is None
        with self.assertRaises(KeyError):
            reformat(expand_dict, self.task_type)


if __name__ == "__main__":
    unittest.main()
