"""Tests for cleanup module."""

import unittest

from dag_converter.cleanup import get_cleanup_dag, get_module_name

DAG_FILE_MODULE_PREFIX = "unusual_prefix_2f500f07efb257480a518fd2ae2d10b7c9df3cd2_"


class Config:
    def __init__(self, size):
        self.size = size


class TestCleanup(unittest.TestCase):
    def test_get_module_name_dag_file(self):
        """Test module name of a class or function defined in the Dag file"""
        module_name = f"{DAG_FILE_MODULE_PREFIX}my_dag_file"
        self.assertEqual(get_module_name(module_name), "my_dag_file")

    def test_get_module_name_regular_module(self):
        """Test module name of a regular module is unchanged"""
        module_name = "airflow.providers.amazon.aws.operators.s3"
        self.assertEqual(get_module_name(module_name), module_name)

    def test_custom_object_defined_in_dag_file(self):
        """Test custom object type uses the Dag file name as module"""
        obj = Config(3)
        Config.__module__ = f"{DAG_FILE_MODULE_PREFIX}my_dag"
        try:
            result = get_cleanup_dag(obj)
        finally:
            Config.__module__ = __name__
        self.assertEqual(result, {"__type__": "my_dag.Config", "size": 3})
