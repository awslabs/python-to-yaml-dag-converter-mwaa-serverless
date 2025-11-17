"""Tests for dag_comparator module."""

import unittest
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from dag_converter.dag_comparator import (
    compare_dags,
    compare_task_attributes,
    compare_values,
    has_angle_bracket_notation,
)


class TestDagComparator(unittest.TestCase):
    def setUp(self):
        def task1_func():
            pass

        def task2_func():
            pass

        # First DAG
        self.dag1 = DAG(dag_id="test_dag_1", start_date=datetime(2023, 1, 1), schedule="@daily")

        task1_dag1 = PythonOperator(task_id="python_task", python_callable=task1_func, dag=self.dag1)

        task2_dag1 = BashOperator(task_id="bash_task", bash_command="echo 'test'", dag=self.dag1)

        task1_dag1 >> task2_dag1

        # Second DAG (similar but with differences)
        self.dag2 = DAG(
            dag_id="test_dag_2",
            start_date=datetime(2023, 1, 2),  # Different start date
            schedule="@daily",
        )

        task1_dag2 = PythonOperator(
            task_id="python_task",
            python_callable=task2_func,  # Different function
            dag=self.dag2,
        )

        task2_dag2 = BashOperator(
            task_id="bash_task",
            bash_command="echo 'different'",  # Different command
            dag=self.dag2,
        )

        task1_dag2 >> task2_dag2

    def test_has_angle_bracket_notation(self):
        """Test detection of angle bracket notation."""
        self.assertTrue(has_angle_bracket_notation("<function test>"))
        self.assertTrue(has_angle_bracket_notation("<object>"))
        self.assertFalse(has_angle_bracket_notation("normal text"))
        self.assertFalse(has_angle_bracket_notation("<partial>text"))
        self.assertFalse(has_angle_bracket_notation("text<partial>"))

    def test_compare_values_simple(self):
        """Test comparison of simple values."""
        self.assertEqual(compare_values(1, 1, "test"), [])
        self.assertEqual(compare_values("a", "a", "test"), [])
        self.assertEqual(compare_values(1, 2, "test"), ["1 != 2"])
        self.assertEqual(compare_values("a", "b", "test"), ["a != b"])

    def test_compare_values_angle_brackets(self):
        """Test comparison of values with angle brackets."""
        self.assertEqual(compare_values("<function a>", "<function b>", "test"), [])
        self.assertEqual(compare_values("<object>", "<different_object>", "test"), [])

    def test_compare_values_lists(self):
        """Test comparison of lists."""
        # Same lists
        self.assertEqual(compare_values([1, 2, 3], [1, 2, 3], "test"), [])
        # Different order (should still be equal)
        self.assertEqual(compare_values([1, 2, 3], [3, 1, 2], "test"), [])
        # Different lengths
        self.assertEqual(compare_values([1, 2], [1, 2, 3], "test"), ["Different lengths: 2 vs 3"])
        # Different values
        self.assertEqual(compare_values([1, 2, 3], [1, 2, 4], "test"), ["Index 2: 3 != 4"])

    def test_compare_values_dicts(self):
        """Test comparison of dictionaries."""
        # Same dictionaries
        self.assertEqual(compare_values({"a": 1, "b": 2}, {"a": 1, "b": 2}, "test"), [])
        # Missing keys
        result = compare_values({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 2}, "test")
        self.assertTrue(any("Keys missing in final dict: {'c'}" in diff for diff in result))
        # Different values
        result = compare_values({"a": 1, "b": 2}, {"a": 1, "b": 3}, "test")
        self.assertTrue(any("Key 'b': 2 != 3" in diff for diff in result))

    def test_compare_values_sets(self):
        """Test comparison of sets."""
        # Same sets
        self.assertEqual(compare_values({1, 2, 3}, {1, 2, 3}, "test"), [])
        # Different sizes
        self.assertEqual(compare_values({1, 2}, {1, 2, 3}, "test"), ["Different set sizes: 2 vs 3"])
        # Different values
        result = compare_values({1, 2, 3}, {1, 2, 4}, "test")
        self.assertTrue(any("3 != 4" in diff for diff in result))

    def test_compare_values_tuples(self):
        """Test comparison of tuples."""
        # Same tuples
        self.assertEqual(compare_values((1, 2, 3), (1, 2, 3), "test"), [])
        # Different order (should still be equal since we sort)
        self.assertEqual(compare_values((1, 2, 3), (3, 1, 2), "test"), [])
        # Different lengths
        self.assertEqual(compare_values((1, 2), (1, 2, 3), "test"), ["Different lengths: 2 vs 3"])

    def test_compare_task_attributes(self):
        """Test comparison of task attributes."""
        task1_dag1 = self.dag1.get_task("python_task")
        task1_dag2 = self.dag2.get_task("python_task")

        differences = compare_task_attributes(task1_dag1, task1_dag2, "python_task")

        # Should find differences but ignore callable references
        self.assertTrue(any("start_date" in diff for diff in differences))
        self.assertFalse(any("python_callable" in diff for diff in differences))

    def test_compare_dags(self):
        """Test comparison of entire DAGs."""
        differences = compare_dags(self.dag1, self.dag2)

        # Should find differences in start_date and bash_command
        self.assertTrue(any("start_date" in diff for task_diffs in differences.values() for diff in task_diffs))
        self.assertTrue(any("bash_command" in diff for task_diffs in differences.values() for diff in task_diffs))
        # Should not find differences in python_callable (angle bracket notation)
        self.assertFalse(any("python_callable" in diff for task_diffs in differences.values() for diff in task_diffs))

    def test_compare_dags_missing_task(self):
        """Test comparison of DAGs with missing tasks."""
        # Create DAGs with different tasks
        dag1 = DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1))

        def dummy_func():
            pass

        PythonOperator(task_id="task1", python_callable=dummy_func, dag=dag1)

        dag2 = DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1))

        differences = compare_dags(dag1, dag2)
        self.assertIn("DAG_STRUCTURE", differences)
        self.assertTrue(any("Tasks missing in final Dag: {'task1'}" in diff for diff in differences["DAG_STRUCTURE"]))
