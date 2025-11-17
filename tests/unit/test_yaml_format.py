"""Tests for yaml_format module."""

import unittest

import yaml

from dag_converter.yaml_format import build_yaml


class TestYamlFormat(unittest.TestCase):
    def test_build_yaml_simple_dict(self):
        """Test YAML formatting of a simple dictionary."""
        data = {"key": "value", "number": 42}
        result = build_yaml(data)
        self.assertIn("key: value", result)
        expected = yaml.dump(data)
        self.assertEqual(result, expected)

    def test_build_yaml_multiline_string(self):
        """Test YAML formatting of multiline strings."""
        data = {"description": "Line 1\nLine 2\nLine 3"}
        result = build_yaml(data)
        self.assertIn("description: |", result)
        self.assertIn("  Line 1", result)
        self.assertIn("  Line 2", result)
        self.assertIn("  Line 3", result)

    def test_build_yaml_nested_dict(self):
        """Test YAML formatting of nested dictionaries."""
        data = {"outer": {"inner1": "value1", "inner2": "value2"}}
        result = build_yaml(data)
        self.assertIn("outer:", result)
        self.assertIn("  inner1: value1", result)
        self.assertIn("  inner2: value2", result)

    def test_build_yaml_list(self):
        """Test YAML formatting of lists."""
        data = {"items": [1, 2, 3]}
        result = build_yaml(data)
        self.assertIn("items:\n- 1\n- 2\n- 3\n", result)

    def test_build_yaml_complex_structure(self):
        """Test YAML formatting of a complex nested structure."""
        data = {
            "name": "test",
            "config": {"items": [1, 2, 3], "settings": {"enabled": True, "description": "Multi\nline\ntext"}},
        }
        result = build_yaml(data)
        self.assertIn("name: test", result)
        self.assertIn("config:", result)
        self.assertIn("  items:\n  - 1\n  - 2\n  - 3\n", result)
        self.assertIn("  settings:", result)
        self.assertIn("    enabled: true", result)
        self.assertIn("    description: |", result)
        self.assertIn("      Multi", result)
