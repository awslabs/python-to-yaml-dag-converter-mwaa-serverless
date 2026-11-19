import contextlib
import sys
from pathlib import Path

import boto3

from dag_converter import code_package
from dag_converter.cleanup import get_cleanup_dag
from dag_converter.console_config import print_error, print_info, print_progress, print_success
from dag_converter.conversion.converter import get_converted_format
from dag_converter.dagbag import get_dag_object
from dag_converter.schema_parser import ArgumentValidator
from dag_converter.taskflow_parser import TaskFlowAnalyzer
from dag_converter.yaml_format import build_yaml
from dag_converter.yaml_validator import validate_yaml_with_dagbag

YAML_EXTENSION = ".yaml"
VALIDATOR_SCHEMA_FILE = "validator_schema.yaml"
TASK_ATTRIBUTES_FILE = ".initial_task_attributes"
TEMPORARY_DAG_ID = "temp-id"


class ConversionManager:
    def __init__(self, s3_bucket: str | None, includes: list[str] | None = None):
        self.s3_bucket = s3_bucket
        self.includes = includes or []
        self._dag_files: set[Path] = set()
        if s3_bucket:
            self.s3_client = boto3.client("s3")

    def start_conversion_process(self, dag_file_path: Path, output_dir: Path, user_validate=False, debug=False) -> None:
        if not dag_file_path.exists():
            raise FileNotFoundError("Dag file not found")
        if output_dir.exists() and not output_dir.is_dir():
            raise ValueError("Expected directory for output path")
        output_dir.mkdir(parents=True, exist_ok=True)
        self._dag_files = {dag_file_path} if dag_file_path.is_file() else set(dag_file_path.glob("*.py"))
        with code_package.dag_root_on_import_path(code_package.find_dag_root(dag_file_path)):
            self._convert_files(dag_file_path, output_dir, user_validate, debug)

    def _convert_files(self, dag_file_path: Path, output_dir: Path, user_validate: bool, debug: bool) -> None:
        if dag_file_path.is_file():
            self.start_file_conversion_process(
                dag_file_path=dag_file_path, output_dir=output_dir, user_validate=user_validate
            )
            return

        total_errors = 0
        file_count = 0
        for child in dag_file_path.glob("*.py"):
            if child.is_file():
                file_count += 1
                try:
                    self.start_file_conversion_process(
                        dag_file_path=child.absolute(), output_dir=output_dir, user_validate=user_validate, debug=debug
                    )
                except Exception as e:
                    total_errors += 1
                    print_error(f"Failed to convert {child.name}. Error: {e}")

        if total_errors > 0:
            print_error(f"Failed to convert {total_errors} out of {file_count} files from {dag_file_path}")
            raise Exception("Conversion failed for { total_errors} out of {file_count} files from {dag_file_path ")
        else:
            print_success(f"Successfully converted {file_count} files from {dag_file_path}")

    def start_file_conversion_process(
        self, dag_file_path: Path, output_dir: Path, user_validate=False, debug=False
    ) -> list[str]:
        # Absolute, so paths the Dag builds from __file__ are found and rewritten
        dag_file_path = dag_file_path.absolute()
        dag_root = code_package.find_dag_root(dag_file_path)
        try:
            return self._convert_file(dag_file_path, dag_root, output_dir, user_validate, debug)
        finally:
            # The next Dag file re-imports them, so its code package gets them too
            code_package.evict_local_modules(dag_root)

    def _convert_file(
        self, dag_file_path: Path, dag_root: Path, output_dir: Path, user_validate: bool, debug: bool
    ) -> list[str]:
        schema_file_path = Path(__file__).absolute().parent / VALIDATOR_SCHEMA_FILE
        validator = ArgumentValidator(schema_file_path)

        module_snapshot = set(sys.modules)
        with code_package.record_opened_files() as opened:
            dag_object_list = get_dag_object(dag_file_path)
        file_modules = code_package.discover_file_modules(dag_file_path, dag_root, module_snapshot, opened)

        result_output_files: list[Path] = []
        result_output_yaml = []

        # Supports multiple Dags in a single file
        for dag_object in dag_object_list:
            # Output each Dag in a different file
            output_path = output_dir / f"{dag_object.dag_id}{YAML_EXTENSION}"
            result_output_files.append(output_path)

            # Output task attributes to intermediate file for debugging DagBag attributes
            if debug:
                with open(Path(__file__).parent / TASK_ATTRIBUTES_FILE, "w+") as f:
                    for attr_name, attr_value in vars(dag_object).items():
                        f.write(f"{attr_name}: {attr_value}\n")
                    f.write("\n")
                    for task in dag_object.tasks:
                        f.write(f"Task: {task.task_id}\n")
                        for attr_name, attr_value in vars(task).items():
                            f.write(f"  {attr_name}: {attr_value}\n")
                        for attr_name in dir(task):
                            try:
                                attr_value = getattr(task, attr_name)
                                f.write(f"  {attr_name}: {attr_value}\n")
                            except Exception as e:
                                f.write(f"  {attr_name}: <Error accessing attribute: {e}>\n")
                        f.write("\n")

            taskflow_parser = TaskFlowAnalyzer(dag_file_path)
            dag_formatted = get_converted_format(taskflow_parser, dag_object, dag_file_path, validator)
            # Local paths are validated as they are; with a zip, against its extracted copy
            validation_dict = code_package.rewrite_module_refs(get_cleanup_dag(dag_formatted), dag_file_path, dag_root)
            zip_path = code_package.package_dag(
                validation_dict,
                dag_object.dag_id,
                dag_file_path,
                dag_root,
                file_modules,
                self.includes,
                output_dir,
                self._dag_files,
            )
            dag_cleaned = code_package.rewrite_paths(validation_dict, dag_root)
            code_package.warn_local_paths(dag_cleaned, dag_root)
            dag_yaml = build_yaml(dag_cleaned)

            # Validation with Dag Factory
            is_valid = True
            if user_validate:
                print_progress("Validating YAML...")

                # Temporarily rename DAG to avoid conflicts in DagBag
                old_dag_id = next(iter(dag_cleaned.keys()))
                renamed_dict = {TEMPORARY_DAG_ID: validation_dict[old_dag_id]}

                code_context = (
                    code_package.extracted_on_import_path(zip_path, dag_root) if zip_path else contextlib.nullcontext()
                )
                with code_context as code_dir:
                    if code_dir:
                        local_dict = code_package.rewrite_paths(validation_dict, dag_root, str(code_dir))
                        renamed_dict = {TEMPORARY_DAG_ID: local_dict[old_dag_id]}
                    is_valid, message = validate_yaml_with_dagbag(
                        dag_object, build_yaml(renamed_dict), dag_file_path, import_path=code_dir
                    )
                if not is_valid:
                    print_error(f"YAML validation failed: {message}")
                    with open(output_path, "w") as f:
                        f.write(str(dag_yaml))
                    print_info(f"YAML written to {output_path} despite validation failure")
                    raise Exception(f"Validation failed: {message}")
                else:
                    print_success("YAML validation successful, no errors found")

            if is_valid:
                # Output Yaml upon success
                if output_path:
                    with open(output_path, "w") as f:
                        f.write(str(dag_yaml))
                    print_success(f"YAML written to {output_path}")

                if self.s3_bucket:
                    self._upload_to_s3(output_path.name, output_path)
                    if zip_path:
                        self._upload_to_s3(zip_path.name, zip_path)

            print_info("\n\n----------------------------------------------------------------------------------\n\n")
            result_output_yaml.append(dag_yaml)

            print_info(
                f"{len(dag_object_list)} Dag Object(s) found."
                + "Please check the following directory for output: {output_dir}"
            )

        return result_output_yaml

    def _upload_to_s3(self, dag_file_name: str, output_path: Path):
        """
        Upload the DAG YAML or code package to the specified S3 bucket.

        Args:
            output_path (Path): File containing the YAML or code package
            dag_file_name (str): S3 file name
        """
        try:
            print_progress(f"Attempting to upload {output_path.name} to s3://{self.s3_bucket}/{dag_file_name}")
            self.s3_client.upload_file(output_path, self.s3_bucket, dag_file_name)

            print_success(f"Successfully uploaded {output_path.name} to s3://{self.s3_bucket}/{dag_file_name}")
            return f"s3://{self.s3_bucket}/{dag_file_name}"

        except Exception as e:
            print_error(f"Failed to upload {output_path.name} to S3: {e}")
            raise e
