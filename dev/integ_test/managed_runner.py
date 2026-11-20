#!/usr/bin/env python3
"""Run test_runner.py inside the managed conversion environment, with the providers the integ test Dags use."""

import sys
import tempfile
from pathlib import Path

import tomllib

repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo))

from dag_converter import managed_env  # noqa: E402


def main(input_path: str, output_folder: str) -> int:
    pyproject = tomllib.loads((repo / "pyproject.toml").read_text())
    dependencies = pyproject["tool"]["hatch"]["envs"]["dev"]["dependencies"]
    with tempfile.TemporaryDirectory() as tmp:
        requirements = Path(tmp, "requirements.txt")
        requirements.write_text("\n".join(dependencies) + "\n")
        runner = Path(__file__).with_name("test_runner.py")
        return managed_env.run_in_env(["python", str(runner), input_path, output_folder], requirements)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python managed_runner.py <input_file_or_folder> <output_folder>")
        sys.exit(1)
    sys.exit(main(sys.argv[1], sys.argv[2]))
