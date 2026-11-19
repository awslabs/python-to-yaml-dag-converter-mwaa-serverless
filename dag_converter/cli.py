import sys
from pathlib import Path
from typing import Annotated

import typer

from dag_converter.managed_env import SERVICE_OVERRIDES, TARGET_PYTHON

DEFAULT_PACKAGES = ", ".join(f"{name}=={version}" for name, version in SERVICE_OVERRIDES.items())

app = typer.Typer()
app.command()
cache_app = typer.Typer(help="Manage the cached conversion environments")
app.add_typer(cache_app, name="cache")


@app.callback()
def callback():
    """
    CLI tool for converting Python airflow Dags to YAML Dags for DagFactory
    """


def run_in_managed_env(requirements: Path | None) -> None:
    """Re-run the current command inside the conversion environment and exit with its code, unless already in it.
    Inside the environment ``--requirements`` has already been installed and is ignored."""
    from dag_converter import managed_env

    if managed_env.in_managed_env():
        return
    try:
        code = managed_env.run_in_managed_env(sys.argv[1:], requirements)
    except managed_env.ManagedEnvError as e:
        from rich.markup import escape

        from dag_converter.console_config import print_error

        print_error(escape(str(e)))
        raise typer.Exit(1) from None
    raise typer.Exit(code)


@app.command()
def convert(
    input_path: Annotated[Path, typer.Argument(help="Path containing Python Dag(s) to convert")],
    output: Annotated[Path, typer.Option(help="Path to output converted YAML Dag(s) to")] = Path("output_yaml/"),
    bucket: Annotated[str, typer.Option(help="S3 bucket to upload converted Dags to. Uses local AWS credentials")] = "",
    validate: Annotated[bool, typer.Option(help="Validate the output YAML using DagFactory")] = True,
    debug: Annotated[
        bool, typer.Option(help="Enable logging DagBag objects to .initial_task_attributes before and after conversion")
    ] = False,  # noqa 501
    requirements: Annotated[
        Path | None,
        typer.Option(
            help="requirements.txt with extra packages your Dags import. The conversion environment comes with "
            f"Python {TARGET_PYTHON} and {DEFAULT_PACKAGES}",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
    skip_mwaa_serverless_managed_env: Annotated[
        bool,
        typer.Option(
            "--skip-mwaa-serverless-managed-env",
            help="Parse Dags with the packages in your current Python environment instead of the isolated "
            "conversion environment. Needs the airflow extra installed",
        ),
    ] = False,
    include: Annotated[
        list[str] | None,
        typer.Option(help="Glob, relative to the Dag folder, of extra files to add to the code package (repeatable)"),
    ] = None,
):
    """
    Loads Python Dags from input and converts them to YAML Dags.
    """
    if not skip_mwaa_serverless_managed_env:
        run_in_managed_env(requirements)
    else:
        import importlib.util

        from rich.markup import escape

        from dag_converter.console_config import print_error, print_warning
        from dag_converter.managed_env import CONVERTER_DIST, CONVERTER_EXTRA

        if importlib.util.find_spec("airflow") is None:
            print_error(
                f"--skip-mwaa-serverless-managed-env needs Apache Airflow in the current Python environment. "
                f"Install it with: pip install '{escape(f'{CONVERTER_DIST}[{CONVERTER_EXTRA}]')}'"
            )
            raise typer.Exit(1)
        print_warning("Converting with the packages in the current Python environment.")
        if requirements:
            print_warning(
                "--requirements is ignored with --skip-mwaa-serverless-managed-env; install the packages yourself."
            )

    from dag_converter.conversion_manager import ConversionManager

    conversion_manager = ConversionManager(s3_bucket=bucket, includes=include or [])
    conversion_manager.start_conversion_process(
        dag_file_path=input_path, output_dir=output, user_validate=validate, debug=debug
    )


@cache_app.command()
def clean():
    """
    Delete all cached conversion environments and downloads.
    """
    import shutil

    from dag_converter.console_config import print_error, print_info, print_success
    from dag_converter.managed_env import cache_root, cache_size, format_size

    root = cache_root()
    if not root.exists():
        print_info(f"Nothing to clean: {root} does not exist")
        return
    size = cache_size(root)
    try:
        shutil.rmtree(root)
    except OSError as e:
        print_error(f"Could not remove {root}: {e}")
        raise typer.Exit(1) from None
    print_success(f"Removed {root} ({format_size(size)})")


@cache_app.command()
def info():
    """
    Show where conversion environments are cached and how much space they use.
    """
    from dag_converter.console_config import print_info
    from dag_converter.managed_env import cache_root, cache_size, format_size

    root = cache_root()
    print_info(f"Location: {root}")
    print_info(f"Size: {format_size(cache_size(root))}")


if __name__ == "__main__":
    app()
