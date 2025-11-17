import unittest
from unittest.mock import Mock

from dag_converter.conversion.default_args import convert_default_args
from dag_converter.conversion.exceptions import InvalidDefaultArg


class TestConvertDefaultArgs(unittest.TestCase):
    def setUp(self):
        self.mock_validator = Mock()

    def test_start_date_without_strftime(self):
        """Test start_date conversion with string value"""
        args = {"start_date": "2023-05-15"}

        result = convert_default_args(args, self.mock_validator)

        self.assertEqual(result["start_date"], "2023-05-15")

    def test_valid_default_arg(self):
        """Test valid default argument passes validation"""
        self.mock_validator.validate_field.return_value = True
        args = {"retries": 3}

        result = convert_default_args(args, self.mock_validator)

        self.assertEqual(result["retries"], 3)
        self.mock_validator.validate_field.assert_called_with("dag", "default_args", "retries")

    def test_invalid_default_arg_raises_exception(self):
        """Test invalid default argument raises InvalidDefaultArg"""
        self.mock_validator.validate_field.return_value = False
        args = {"invalid_arg": "value"}

        with self.assertRaises(InvalidDefaultArg) as context:
            convert_default_args(args, self.mock_validator)

        self.assertEqual(str(context.exception), "Default Argument 'invalid_arg' is not supported")

    def test_mixed_args(self):
        """Test conversion with mixed valid args"""
        self.mock_validator.validate_field.return_value = True
        args = {"retries": 3, "retry_delay": 300}

        result = convert_default_args(args, self.mock_validator)

        self.assertEqual(result["retries"], 3)
        self.assertEqual(result["retry_delay"], 300)

    def test_empty_args(self):
        """Test conversion with empty args dictionary"""
        args = {}

        result = convert_default_args(args, self.mock_validator)

        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
