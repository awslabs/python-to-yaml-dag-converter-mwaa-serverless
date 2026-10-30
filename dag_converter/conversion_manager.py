from pathlib import Path

import boto3

from dag_converter.cleanup import get_cleanup_dag
from dag_converter.console_config import print_error, print_info, print_progress, print_success
from dag_converter.conversion.converter import get_converted_format
from dag_converter.dagbag import NoDagFoundError, get_dag_object
from dag_converter.schema_parser import ArgumentValidator
from dag_converter.static_validator import validate_file
from dag_converter.taskflow_parser import TaskFlowAnalyzer
from dag_converter.yaml_format import build_yaml
from dag_converter.yaml_validator import validate_yaml_with_dagbag

YAML_EXTENSION = ".yaml"
VALIDATOR_SCHEMA_FILE = "validator_schema.yaml"
TASK_ATTRIBUTES_FILE = ".initial_task_attributes"
TEMPORARY_DAG_ID = "temp-id"


class ConversionManager:
    def __init__(self, s3_bucket: str | None):
        self.s3_bucket = s3_bucket
        if s3_bucket:
            self.s3_client = boto3.client("s3")

    def start_conversion_process(self, dag_file_path: Path, output_dir: Path, user_validate=False, debug=False) -> None:
        if not dag_file_path.exists():
            raise FileNotFoundError("Dag file not found")
        if output_dir.exists() and not output_dir.is_dir():
            raise ValueError("Expected directory for output path")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Output files written by this run, to detect Dags with the same dag_id overwriting each other
        written_outputs: dict[Path, Path] = {}

        if dag_file_path.is_file():
            self.start_file_conversion_process(
                dag_file_path=dag_file_path,
                output_dir=output_dir,
                user_validate=user_validate,
                debug=debug,
                written_outputs=written_outputs,
            )
            return

        total_errors = 0
        file_count = 0
        for child in sorted(dag_file_path.glob("*.py")):
            if child.is_file():
                try:
                    self.start_file_conversion_process(
                        dag_file_path=child.absolute(),
                        output_dir=output_dir,
                        user_validate=user_validate,
                        debug=debug,
                        written_outputs=written_outputs,
                    )
                except NoDagFoundError:
                    # Helper modules next to the Dag files
                    print_info(f"Skipping {child.name}, it does not define a Dag")
                    continue
                except Exception as e:
                    total_errors += 1
                    print_error(f"Failed to convert {child.name}. Error: {e}")
                file_count += 1

        if total_errors > 0:
            print_error(f"Failed to convert {total_errors} out of {file_count} files from {dag_file_path}")
            raise Exception(f"Conversion failed for {total_errors} out of {file_count} files from {dag_file_path}")
        else:
            print_success(f"Successfully converted {file_count} files from {dag_file_path}")

    def start_file_conversion_process(
        self,
        dag_file_path: Path,
        output_dir: Path,
        user_validate=False,
        debug=False,
        written_outputs: dict[Path, Path] | None = None,
    ) -> list[str]:
        schema_file_path = Path(__file__).absolute().parent / VALIDATOR_SCHEMA_FILE
        validator = ArgumentValidator(schema_file_path)

        try:
            validate_file(dag_file_path, validator)
        except Exception as e:
            print_error("Static validation failed")
            raise e
        dag_object_list = get_dag_object(dag_file_path)

        if written_outputs is None:
            written_outputs = {}
        result_output_yaml = []
        failed_dags = []

        # Supports multiple Dags in a single file. A failing Dag does not stop the others
        for dag_object in dag_object_list:
            try:
                dag_yaml = self._convert_dag(
                    dag_object, dag_file_path, output_dir, validator, user_validate, debug, written_outputs
                )
            except Exception as e:
                if len(dag_object_list) == 1:
                    raise
                print_error(f"Failed to convert Dag {dag_object.dag_id}. Error: {e}")
                failed_dags.append(dag_object.dag_id)
                continue
            result_output_yaml.append(dag_yaml)

        print_info(
            f"{len(dag_object_list)} Dag Object(s) found. Please check the following directory for output: {output_dir}"
        )
        if failed_dags:
            raise Exception(
                f"Failed to convert {len(failed_dags)} out of {len(dag_object_list)} Dags in {dag_file_path.name}: "
                + ", ".join(failed_dags)
            )
        return result_output_yaml

    def _convert_dag(
        self,
        dag_object,
        dag_file_path: Path,
        output_dir: Path,
        validator: ArgumentValidator,
        user_validate: bool,
        debug: bool,
        written_outputs: dict[Path, Path],
    ) -> str:
        """Convert, validate and write one Dag. Returns its YAML"""
        # Output each Dag in a different file
        output_path = output_dir / f"{dag_object.dag_id}{YAML_EXTENSION}"
        if output_path in written_outputs:
            raise Exception(
                f"Dag {dag_object.dag_id} is also defined in {written_outputs[output_path].name}. "
                f"Both would be written to {output_path}"
            )
        written_outputs[output_path] = dag_file_path

        if debug:
            self._write_debug_attributes(dag_object)

        taskflow_parser = TaskFlowAnalyzer(dag_file_path)
        dag_formatted = get_converted_format(taskflow_parser, dag_object, dag_file_path, validator)
        dag_cleaned = get_cleanup_dag(dag_formatted)
        dag_yaml = build_yaml(dag_cleaned)

        # Validation with Dag Factory
        if user_validate:
            print_progress("Validating YAML...")

            # Temporarily rename DAG to avoid conflicts in DagBag
            old_dag_id = next(iter(dag_cleaned.keys()))
            renamed_dict = {TEMPORARY_DAG_ID: dag_cleaned[old_dag_id]}

            is_valid, message = validate_yaml_with_dagbag(dag_object, build_yaml(renamed_dict), dag_file_path)
            if not is_valid:
                print_error(f"YAML validation failed: {message}")
                with open(output_path, "w") as f:
                    f.write(str(dag_yaml))
                print_info(f"YAML written to {output_path} despite validation failure")
                raise Exception(f"Validation failed: {message}")
            print_success("YAML validation successful, no errors found")

        # Output Yaml upon success
        with open(output_path, "w") as f:
            f.write(str(dag_yaml))
        print_success(f"YAML written to {output_path}")

        if self.s3_bucket:
            self._upload_to_s3(output_path.name, output_path)

        print_info("\n\n----------------------------------------------------------------------------------\n\n")
        return dag_yaml

    @staticmethod
    def _write_debug_attributes(dag_object):
        """Output task attributes to intermediate file for debugging DagBag attributes"""
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

    def _upload_to_s3(self, dag_file_name: str, output_path: Path):
        """
        Upload the DAG YAML content to the specified S3 bucket.

        Args:
            output_path (Path): File containing the YAML
            dag_file_name (str): S3 file name
        """
        try:
            print_progress(f"Attempting to upload DAG YAML to s3://{self.s3_bucket}/{dag_file_name}")
            self.s3_client.upload_file(output_path, self.s3_bucket, dag_file_name)

            print_success(f"Successfully uploaded DAG YAML to s3://{self.s3_bucket}/{dag_file_name}")
            return f"s3://{self.s3_bucket}/{dag_file_name}"

        except Exception as e:
            print_error(f"Failed to upload DAG YAML to S3: {e}")
            raise e
