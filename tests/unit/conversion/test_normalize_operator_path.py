import unittest

from dag_converter.conversion.tasks import normalize_operator_path


class TestNormalizeOperatorPath(unittest.TestCase):
    def test_empty_operator_normalized(self):
        """Test EmptyOperator providers.standard path is normalized"""
        result = normalize_operator_path("airflow.providers.standard.operators.empty.EmptyOperator")
        self.assertEqual(result, "airflow.operators.empty.EmptyOperator")

    def test_bash_operator_normalized(self):
        """Test BashOperator providers.standard path is normalized"""
        result = normalize_operator_path("airflow.providers.standard.operators.bash.BashOperator")
        self.assertEqual(result, "airflow.operators.bash.BashOperator")

    def test_python_operator_normalized(self):
        """Test PythonOperator providers.standard path is normalized"""
        result = normalize_operator_path("airflow.providers.standard.operators.python.PythonOperator")
        self.assertEqual(result, "airflow.operators.python.PythonOperator")

    def test_amazon_provider_unchanged(self):
        """Test Amazon provider paths are not modified"""
        path = "airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator"
        result = normalize_operator_path(path)
        self.assertEqual(result, path)

    def test_already_short_form_unchanged(self):
        """Test already-canonical paths are not modified"""
        path = "airflow.operators.empty.EmptyOperator"
        result = normalize_operator_path(path)
        self.assertEqual(result, path)

    def test_cncf_provider_unchanged(self):
        """Test other provider paths are not modified"""
        path = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator"
        result = normalize_operator_path(path)
        self.assertEqual(result, path)
