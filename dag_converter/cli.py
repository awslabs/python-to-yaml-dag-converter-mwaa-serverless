from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer()
app.command()


@app.callback()
def callback():
    """
    CLI tool for converting Python airflow Dags to YAML Dags for DagFactory
    """


@app.command()
def convert(
    input_path: Annotated[Path, typer.Argument(help="Path containing Python Dag(s) to convert")],
    output: Annotated[Path, typer.Option(help="Path to output converted YAML Dag(s) to")] = Path("output_yaml/"),
    bucket: Annotated[str, typer.Option(help="S3 bucket to upload converted Dags to. Uses local AWS credentials")] = "",
    validate: Annotated[bool, typer.Option(help="Validate the output YAML using DagFactory")] = True,
    debug: Annotated[
        bool, typer.Option(help="Enable logging DagBag objects to .initial_task_attributes before and after conversion")
    ] = False,  # noqa 501
):
    """
    Loads Python Dags from input and converts them to YAML Dags.
    """
    from dag_converter.conversion_manager import ConversionManager

    conversion_manager = ConversionManager(s3_bucket=bucket)
    conversion_manager.start_conversion_process(
        dag_file_path=input_path, output_dir=output, user_validate=validate, debug=debug
    )


if __name__ == "__main__":
    app()
