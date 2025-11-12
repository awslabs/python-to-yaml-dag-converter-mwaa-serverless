from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer()
app.command()


@app.callback()
def callback():
    """
    CLI tool for converting Python airflow DAGs to YAML DAGs for DagFactory
    """


@app.command()
def convert(
    input_path: Annotated[Path, typer.Argument(help="Path containing Python DAG(s) to convert")],
    output: Annotated[Path, typer.Option(help="Path to output converted YAML DAG(s) to")] = Path("output_yaml/"),
    bucket: Annotated[str, typer.Option(help="S3 bucket to upload converted DAGs to. Uses local AWS credentials")] = "",
    validate: Annotated[bool, typer.Option(help="Validate the output YAML using DagFactory")] = True,
    debug: Annotated[bool, typer.Option(help="Enable logging DagBag objects before and after conversion")] = False,
):
    """
    Loads Python DAGs from input and converts them to YAML DAGs.
    """
    from dag_converter.conversion_manager import ConversionManager

    conversion_manager = ConversionManager(s3_bucket=bucket)
    conversion_manager.start_conversion_process(
        dag_file_path=input_path, output_dir=output, user_validate=validate, debug=debug
    )


if __name__ == "__main__":
    app()
