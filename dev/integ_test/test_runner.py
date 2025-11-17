#!/usr/bin/env python3

import sys
from pathlib import Path

# Add parent directory to Python path
src_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(src_path))

from dag_converter.conversion.exceptions import InvalidOperatorError  # noqa: E402
from dag_converter.conversion_manager import ConversionManager  # noqa: E402


def run_tests(input_path, output_folder):
    """
    Run conversion on Python file(s) and report results.

    Args:
        input_path: Path to Python file or folder containing test Python files
        output_folder: Path to folder where YAML outputs will be saved
    """
    input_path = Path(input_path)
    output_path = Path(output_folder)

    if not input_path.exists():
        print(f"Input path {input_path} does not exist")
        return

    # Create output folder if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Determine if input is a file or folder
    if input_path.is_file():
        if input_path.suffix == ".py":
            python_files = [input_path]
        else:
            print(f"Input file {input_path} is not a Python file")
            return
    else:
        # Find all Python files in folder
        python_files = [file for file in input_path.rglob("*.py") if "dependencies" not in str(file)]
        if not python_files:
            print(f"No Python files found in {input_path}")
            return

    passed = []
    unsupported = []
    failed = []

    print(f"Found {len(python_files)} Python files to test\n")

    conversion_manager = ConversionManager(s3_bucket=None)

    for py_file in python_files:
        print(f"Processing {py_file.name}...")

        # Add the parent directory of the Python file to sys.path for imports
        py_file_dir = str(py_file.parent)
        if py_file_dir not in sys.path:
            sys.path.insert(0, py_file_dir)

        try:
            # Run conversion with validation
            conversion_manager.start_conversion_process(
                dag_file_path=py_file,
                output_dir=output_path,
                user_validate=True,
                debug=True,
            )
            passed.append(py_file.name)
            print(f"✅ {py_file.name} - PASSED")
        except InvalidOperatorError as e:
            unsupported.append((py_file.name, str(e)))
            print(f"🟨 {py_file.name} - UNSUPPORTED")
        except Exception as e:
            failed.append((py_file.name, str(e)))
            print(f"❌ {py_file.name} - FAILED: {e}")

        print("-" * 50)

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY: {len(passed)} passed, {len(unsupported)} unsupported, {len(failed)} failed.")
    print(f"{'='*60}")

    if passed:
        print(f"\n✅ PASSED ({len(passed)}):")
        for file in passed:
            print(f"  - {file}")
    if unsupported:
        print(f"\n🟨 UNSUPPORTED ({len(unsupported)}):")
        for file, error in unsupported:
            print(f"  - {file}: {error}")

    if failed:
        print(f"\n❌ FAILED ({len(failed)}):")
        for file, error in failed:
            print(f"  - {file}: {error}")

    print(f"\nOutput files saved to: {output_folder}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test_runner.py <input_file_or_folder> <output_folder>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_folder = sys.argv[2]

    run_tests(input_path, output_folder)
