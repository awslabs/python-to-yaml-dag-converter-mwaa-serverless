"""Tests for the managed conversion environment (uv is never actually run)."""

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from dag_converter import managed_env
from dag_converter.cli import app

runner = CliRunner(env={"NO_COLOR": "1", "TERM": "dumb"})
DAY = 24 * 3600
NOW = 1_000 * DAY


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """Point the cache root at a temp dir."""
    root = tmp_path / "cache" / "dag-converter"
    monkeypatch.setattr(managed_env, "cache_root", lambda: root)
    return root


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


# --- where the converter comes from ---------------------------------------------------------------------------------


def test_converter_args_from_source_checkout():
    # The test suite runs from a source checkout
    args = managed_env.converter_args()
    assert args[0] == "--from" and args[2] == "--with-editable" and args[3] == f"{args[1]}[airflow]"
    assert Path(args[1], "pyproject.toml").is_file()


def test_converter_args_released(monkeypatch):
    monkeypatch.setattr(managed_env, "_source_checkout", lambda: None)
    assert managed_env.converter_args() == [
        "--from",
        f"{managed_env.CONVERTER_DIST}[airflow]=={managed_env.__version__}",
    ]


# --- override warnings ----------------------------------------------------------------------------------------------


def test_warn_overridden_pins():
    lines = [
        "apache-airflow==2.9.0",
        "apache-airflow-providers-amazon>=9.0,<10",  # allows the service version
        "Dag_Factory==0.23.0",
        "boto3",
        "duckdb==1.0.0",
        "-c /abs/constraints.txt",
        "not a requirement !!",
    ]
    with patch.object(managed_env, "print_warning") as warn:
        managed_env.warn_overridden_pins(lines, "requirements.txt")
    messages = [c.args[0] for c in warn.call_args_list]
    assert messages == [
        "requirements.txt pins apache-airflow==2.9.0, but MWAA Serverless always uses 3.0.6; converting with 3.0.6.",
        "requirements.txt pins Dag_Factory==0.23.0, but MWAA Serverless always uses 1.0.0; converting with 1.0.0.",
    ]


def test_overrides_text_matches_service_versions():
    assert managed_env.overrides_text().splitlines() == [
        "apache-airflow==3.0.6",
        "apache-airflow-providers-amazon==9.30.0",
        "dag-factory==1.0.0",
        "boto3==1.43.1",
        "botocore==1.43.1",
    ]


# --- cache location and installer environment -----------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX cache location")
def test_cache_root_posix(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert managed_env.cache_root() == tmp_path / "dag-converter"
    monkeypatch.delenv("XDG_CACHE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    assert managed_env.cache_root() == Path.home() / ".cache" / "dag-converter"


def test_installer_env(monkeypatch, tmp_path):
    monkeypatch.setenv("PIP_INDEX_URL", "https://pip.example/simple")
    monkeypatch.setenv("PIP_EXTRA_INDEX_URL", "https://extra.example/simple")
    monkeypatch.setenv("UV_EXTRA_INDEX_URL", "https://uv-extra.example/simple")
    monkeypatch.delenv("UV_INDEX_URL", raising=False)
    env = managed_env.installer_env(tmp_path)
    assert env["UV_CACHE_DIR"] == str(tmp_path / "uv-cache")
    assert env["UV_PYTHON_INSTALL_DIR"] == str(tmp_path / "python")
    assert env["UV_INDEX_URL"] == "https://pip.example/simple"
    assert env["UV_EXTRA_INDEX_URL"] == "https://uv-extra.example/simple"


# --- running in the environment ------------------------------------------------------------------------------------


def test_run_in_env_uses_uv_tool_run(cache, tmp_path, monkeypatch):
    monkeypatch.setattr(managed_env, "_find_uv", lambda: "UV")
    monkeypatch.setattr(managed_env, "converter_args", lambda: ["--from", "SRC"])
    requirements = write(tmp_path / "requirements.txt", "apache-airflow==2.9.0\nduckdb\n")
    overrides = []

    def fake_run(cmd, env=None, **kwargs):
        if cmd[1:3] == ["tool", "run"]:
            overrides.append(Path(cmd[cmd.index("--overrides") + 1]).read_text())
        return subprocess.CompletedProcess(cmd, 7)

    with (
        patch.object(managed_env.subprocess, "run", side_effect=fake_run) as run,
        patch.object(managed_env, "print_warning") as warn,
    ):
        assert managed_env.run_in_env(["python", "script.py"], requirements) == 7

    cmd = run.call_args_list[0].args[0]
    overrides_index = cmd.index("--overrides")
    assert cmd[:overrides_index] == ["UV", "tool", "run", "--python", "3.12", "--from", "SRC"]
    assert cmd[overrides_index + 2 :] == ["--with-requirements", str(requirements), "python", "script.py"]
    assert overrides == [managed_env.overrides_text()]
    env = run.call_args_list[0].kwargs["env"]
    assert env[managed_env.ENV_FLAG] == "1"
    assert env["UV_CACHE_DIR"] == str(cache / "uv-cache")
    warn.assert_called_once()
    assert (cache / managed_env.PRUNE_MARKER).exists()


def test_run_in_env_without_requirements_ignores_cleanup_errors(cache, monkeypatch):
    monkeypatch.setattr(managed_env, "_find_uv", lambda: "UV")
    def failing_prune(*_):
        raise OSError("cleanup errors are ignored")

    monkeypatch.setattr(managed_env, "_maybe_prune", failing_prune)
    with patch.object(managed_env.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
        assert managed_env.run_in_env(["python"], None) == 0
    assert "--with-requirements" not in run.call_args.args[0]


def test_run_in_managed_env_reruns_same_argv(monkeypatch):
    with patch.object(managed_env, "run_in_env", return_value=3) as run:
        assert managed_env.run_in_managed_env(["convert", "dag.py"], None) == 3
    run.assert_called_once_with(["python", "-m", "dag_converter.cli", "convert", "dag.py"], None)


def test_missing_uv_raises(monkeypatch):
    monkeypatch.setitem(sys.modules, "uv", None)
    with pytest.raises(managed_env.ManagedEnvError, match="installation is incomplete"):
        managed_env._find_uv()


def test_cli_reruns_outside_managed_env(monkeypatch, tmp_path):
    monkeypatch.delenv(managed_env.ENV_FLAG)
    requirements = write(tmp_path / "requirements.txt", "")
    argv = ["convert", "tests/unit/samples/aws_s3.py", "--requirements", str(requirements)]
    monkeypatch.setattr(sys, "argv", ["dag-converter", *argv])
    with (
        patch.object(managed_env, "run_in_managed_env", return_value=3) as rerun,
        patch("dag_converter.conversion_manager.ConversionManager") as manager,
    ):
        result = runner.invoke(app, argv)
    assert result.exit_code == 3
    rerun.assert_called_once_with(argv, requirements)
    manager.assert_not_called()


def test_cli_reports_env_errors(monkeypatch):
    monkeypatch.delenv(managed_env.ENV_FLAG)
    error = managed_env.ManagedEnvError("Failed to prepare the conversion environment:\n[boom]")
    with patch.object(managed_env, "run_in_managed_env", side_effect=error):
        result = runner.invoke(app, ["convert", "tests/unit/samples/aws_s3.py"])
    assert result.exit_code == 1
    assert "Failed to prepare the conversion environment" in result.output
    assert "[boom]" in result.output


def test_cli_ignores_requirements_inside_managed_env(tmp_path):
    requirements = write(tmp_path / "requirements.txt", "")
    with (
        patch.object(managed_env, "run_in_managed_env") as rerun,
        patch("dag_converter.conversion_manager.ConversionManager") as manager,
    ):
        result = runner.invoke(app, ["convert", "tests/unit/samples/aws_s3.py", "--requirements", str(requirements)])
    assert result.exit_code == 0
    rerun.assert_not_called()
    manager.return_value.start_conversion_process.assert_called_once()


def test_cli_skip_mwaa_serverless_managed_env_converts_in_place(monkeypatch, tmp_path):
    monkeypatch.delenv(managed_env.ENV_FLAG)
    requirements = write(tmp_path / "requirements.txt", "")
    argv = [
        "convert",
        "tests/unit/samples/aws_s3.py",
        "--skip-mwaa-serverless-managed-env",
        "--requirements",
        str(requirements),
    ]
    with (
        patch.object(managed_env, "run_in_managed_env") as rerun,
        patch("dag_converter.conversion_manager.ConversionManager") as manager,
    ):
        result = runner.invoke(app, argv)
    output = " ".join(result.output.split())
    assert result.exit_code == 0
    rerun.assert_not_called()
    manager.return_value.start_conversion_process.assert_called_once()
    assert "current Python environment" in output
    assert "--requirements is ignored" in output


def test_cli_skip_mwaa_serverless_managed_env_needs_airflow(monkeypatch):
    monkeypatch.delenv(managed_env.ENV_FLAG)
    with patch("importlib.util.find_spec", return_value=None):
        result = runner.invoke(app, ["convert", "tests/unit/samples/aws_s3.py", "--skip-mwaa-serverless-managed-env"])
    output = " ".join(result.output.split())
    assert result.exit_code == 1
    assert "python-to-yaml-dag-converter-mwaa-serverless[airflow]" in output


# --- cleanup --------------------------------------------------------------------------------------------------------


def test_prune_runs_at_most_once_per_7_days(cache, monkeypatch):
    monkeypatch.setattr(managed_env, "_find_uv", lambda: "UV")
    (cache / "uv-cache").mkdir(parents=True)
    with patch.object(managed_env.subprocess, "run") as run:
        managed_env._maybe_prune(cache, NOW)
        marker = cache / managed_env.PRUNE_MARKER
        os.utime(marker, (NOW, NOW))
        managed_env._maybe_prune(cache, NOW + 6 * DAY)
        assert run.call_count == 1
        managed_env._maybe_prune(cache, NOW + 8 * DAY)
        assert run.call_count == 2
    assert run.call_args.args[0] == ["UV", "cache", "prune", "--quiet"]
    assert run.call_args.kwargs["env"]["UV_CACHE_DIR"] == str(cache / "uv-cache")


# --- cache commands -------------------------------------------------------------------------------------------------


def test_cache_clean_removes_root(cache):
    write(cache / "envs" / "abc" / "file", "x" * 2048)
    result = runner.invoke(app, ["cache", "clean"])
    assert result.exit_code == 0
    assert not cache.exists()
    assert "Removed" in result.output and "2.0 KB" in result.output


def test_cache_clean_nothing_to_clean(cache):
    result = runner.invoke(app, ["cache", "clean"])
    assert result.exit_code == 0
    assert "Nothing to clean" in result.output


def test_cache_info(cache):
    write(cache / "uv-cache" / "file", "x" * 2048)
    result = runner.invoke(app, ["cache", "info"])
    output = " ".join(result.output.split())
    assert result.exit_code == 0
    assert "Size: 2.0 KB" in output


def test_format_size():
    assert managed_env.format_size(512) == "512 B"
    assert managed_env.format_size(3 * 1024**2) == "3.0 MB"
    assert managed_env.format_size(5 * 1024**3) == "5.0 GB"
