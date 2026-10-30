"""Conversion of directories and files with several Dags."""

import unittest

from dag_converter.conversion_manager import ConversionManager
from tests.unit.conversion.test_custom_operators import CustomOperatorTestCase


class TestConversionRuns(CustomOperatorTestCase):
    def _dag_file(self, name: str, dag_ids: list[str], failing: str | None = None) -> None:
        tasks = []
        for dag_id in dag_ids:
            task = "Plain(1)" if dag_id == failing else "1"
            tasks.append(
                f"with DAG(dag_id='{dag_id}', start_date=datetime(2026, 1, 1), schedule=None):\n"
                f"    PayloadOperator(task_id='t', payload={task})\n"
            )
        header = f"from datetime import datetime\nfrom airflow import DAG\nfrom {self.ops} import *\n\n"
        source = header + "\n".join(tasks)
        (self.input_dir / name).write_text(source)

    def setUp(self):
        super().setUp()
        self.input_dir = self.work_dir / "dags"
        self.input_dir.mkdir()
        # The operators module next to the Dag files is also a helper module the directory conversion skips
        (self.input_dir / f"{self.ops}.py").write_text((self.work_dir / f"{self.ops}.py").read_text())

    def test_same_dag_id_in_two_files(self):
        """R18: the second file overwrote the first one's YAML"""
        self._dag_file("first.py", [f"{self.dag_id}_shared"])
        self._dag_file("second.py", [f"{self.dag_id}_shared"])

        with self.assertRaisesRegex(Exception, "1 out of 2 files"):
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

    def test_failing_dag_does_not_stop_the_file(self):
        """R19: one failing Dag aborted the remaining Dags of the file"""
        ok_dag, bad_dag = f"{self.dag_id}_ok", f"{self.dag_id}_bad"
        self._dag_file("both.py", [bad_dag, ok_dag], failing=bad_dag)

        with self.assertRaisesRegex(Exception, bad_dag):
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir / "both.py", self.output_dir)

        self.assertTrue((self.output_dir / f"{ok_dag}.yaml").exists())

    def test_helper_modules_in_directory_are_skipped(self):
        self._dag_file("dag.py", [f"{self.dag_id}_dir"])
        (self.input_dir / "helpers.py").write_text("def helper():\n    return 1\n")

        ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

        self.assertTrue((self.output_dir / f"{self.dag_id}_dir.yaml").exists())

    def test_failure_message_has_counts(self):
        self._dag_file("bad.py", [f"{self.dag_id}_bad"], failing=f"{self.dag_id}_bad")

        with self.assertRaises(Exception) as context:
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

        self.assertIn("1 out of 1 files", str(context.exception))
        self.assertNotIn("{", str(context.exception))


if __name__ == "__main__":
    unittest.main()
