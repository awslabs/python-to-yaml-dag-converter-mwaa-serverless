"""Tests for taskflow_parser module."""

import os
import tempfile
import unittest
from pathlib import Path

from dag_converter.taskflow_parser import TaskFlowAnalyzer


class TestTaskFlowAnalyzer(unittest.TestCase):
    def _create_file(self, content):
        """Create a temporary Python file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(content)
            return Path(f.name)

    def test_simple_task_function(self):
        """Test detection of simple @task decorated function."""
        content = """
@task
def my_task(param1, param2):
    return param1 + param2
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertIn("my_task", analyzer.task_functions)
            self.assertEqual(analyzer.task_functions["my_task"], ["param1", "param2"])
        finally:
            os.unlink(file_path)

    def test_task_function_with_call_decorator(self):
        """Test detection of @task() decorated function."""
        content = """
@task()
def process_data(data, config):
    return data
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertIn("process_data", analyzer.task_functions)
            self.assertEqual(analyzer.task_functions["process_data"], ["data", "config"])
        finally:
            os.unlink(file_path)

    def test_multiple_task_functions(self):
        """Test detection of multiple @task decorated functions."""
        content = """
@task
def task_one(x):
    return x

@task()
def task_two(a, b, c):
    return a + b + c

def regular_function():
    pass
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertEqual(len(analyzer.task_functions), 2)
            self.assertEqual(analyzer.task_functions["task_one"], ["x"])
            self.assertEqual(analyzer.task_functions["task_two"], ["a", "b", "c"])
            self.assertNotIn("regular_function", analyzer.task_functions)
        finally:
            os.unlink(file_path)

    def test_no_task_functions(self):
        """Test file with no @task decorated functions."""
        content = """
def regular_function(param):
    return param

@other_decorator
def decorated_function():
    pass
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertEqual(len(analyzer.task_functions), 0)
        finally:
            os.unlink(file_path)

    def test_task_function_no_parameters(self):
        """Test @task decorated function with no parameters."""
        content = """
@task
def no_params():
    return "hello"
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertIn("no_params", analyzer.task_functions)
            self.assertEqual(analyzer.task_functions["no_params"], [])
        finally:
            os.unlink(file_path)

    def test_mixed_decorators(self):
        """Test functions with multiple decorators including @task."""
        content = """
@some_decorator
@task
@another_decorator
def multi_decorated(param):
    return param

@task
@other_decorator
def task_with_other(x, y):
    return x + y
"""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertEqual(len(analyzer.task_functions), 2)
            self.assertEqual(analyzer.task_functions["multi_decorated"], ["param"])
            self.assertEqual(analyzer.task_functions["task_with_other"], ["x", "y"])
        finally:
            os.unlink(file_path)

    def test_empty_file(self):
        """Test empty Python file."""
        content = ""
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            self.assertEqual(len(analyzer.task_functions), 0)
        finally:
            os.unlink(file_path)

    def test_complex_parameters(self):
        """Test task function with various parameter types."""
        content = """
@task
def complex_task(a, b=None, *args, **kwargs):
    return a
    """
        file_path = self._create_file(content)
        try:
            analyzer = TaskFlowAnalyzer(file_path)
            # Only regular args are captured, not defaults, *args, **kwargs
            self.assertEqual(analyzer.task_functions["complex_task"], ["a", "b"])
        finally:
            os.unlink(file_path)
