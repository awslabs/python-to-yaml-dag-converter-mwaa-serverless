import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.models.dag import DAG

from dag_converter.dagbag import get_dag_factory_object, get_dag_object


class TestDagBag(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_samples_dir = Path(__file__).parent / "samples"
        self.valid_dag_file = self.test_samples_dir / "aws_s3.py"
        self.simple_test_dag_file = self.test_samples_dir / "simple_test_dag.py"
        self.dagfactory_test_dag_file = self.test_samples_dir / "dagfactory_test_dag.py"

        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        """Clean up after each test method."""
        # Clean up temporary files
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_dag_object_happy_path(self):
        """Test get_dag_object with a valid DAG file - happy path case."""
        # Test with the sample DAG file
        result = get_dag_object(self.valid_dag_file)

        # Assertions
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIsInstance(result[0], DAG)
        self.assertEqual(result[0].dag_id, "example_s3")

    def test_get_dag_object_with_simple_dag_happy_path(self):
        """Test get_dag_object with a simple test DAG file - happy path case."""
        result = get_dag_object(self.test_samples_dir)

        # Should find multiple valid DAGs
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)

        # Check that we got DAG objects
        for dag in result:
            self.assertIsInstance(dag, DAG)

        # Check that we have the expected DAG IDs
        dag_ids = [dag.dag_id for dag in result]
        self.assertIn("simple_test_dag", dag_ids)

    def test_get_dag_object_failure_case_no_dags(self):
        """Test get_dag_object with a directory containing no valid DAGs - failure case."""
        # Create an empty directory
        empty_dir = self.temp_path / "empty"
        empty_dir.mkdir()

        with self.assertRaises(Exception) as context:
            get_dag_object(empty_dir)

        self.assertIn("Failed to generate dag object for file", str(context.exception))

    def test_get_dag_object_failure_case_invalid_path(self):
        """Test get_dag_object with non-existent path - failure case."""
        non_existent_path = Path("/non/existent/path")

        with self.assertRaises(Exception) as context:
            get_dag_object(non_existent_path)

        self.assertIn("Failed to generate dag object for file", str(context.exception))

    @patch("dag_converter.dagbag.DagBag")
    def test_get_dag_object_failure_case_dagbag_error(self, mock_dagbag_class):
        """Test get_dag_object when DagBag fails to load DAGs - failure case."""
        # Mock DagBag to return empty dags
        mock_dagbag = MagicMock()
        mock_dagbag.dags = {}
        mock_dagbag_class.return_value = mock_dagbag

        with self.assertRaises(Exception) as context:
            get_dag_object(self.valid_dag_file.parent)

        self.assertIn("Failed to generate dag object for file", str(context.exception))

    def test_get_dag_factory_object_happy_path(self):
        """Test get_dag_factory_object with a DAG that has dagfactory attribute - happy path case."""
        result = get_dag_factory_object(self.dagfactory_test_dag_file)

        # Assertions
        self.assertIsInstance(result, DAG)
        self.assertEqual(result.dag_id, "dagfactory_test_dag")
        self.assertTrue(hasattr(result, "is_dagfactory_auto_generated"))
        self.assertTrue(getattr(result, "is_dagfactory_auto_generated", False))

    def test_get_dag_factory_object_failure_case_no_dagfactory_dag(self):
        """Test get_dag_factory_object when no DAG has dagfactory attribute - failure case."""
        # Create a temporary directory with only the regular DAG (no dagfactory attribute)

        with self.assertRaises(Exception) as context:
            get_dag_factory_object(self.valid_dag_file)

        self.assertIn("Failed to generate dag object for file", str(context.exception))

    def test_get_dag_factory_object_failure_case_empty_dagbag(self):
        """Test get_dag_factory_object with empty DagBag - failure case."""
        # Create an empty directory
        empty_dir = self.temp_path / "empty_factory"
        empty_dir.mkdir()

        with self.assertRaises(Exception) as context:
            get_dag_factory_object(empty_dir)

        self.assertIn("Failed to generate dag object for file", str(context.exception))
