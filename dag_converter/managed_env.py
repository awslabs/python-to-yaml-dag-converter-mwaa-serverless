"""Run every conversion inside an isolated environment that matches the MWAA Serverless runtime.

The outer ``dag-converter`` process re-runs the same command through ``uv tool run`` with Python 3.12, the converter,
the packages MWAA Serverless always uses (at the service's versions) and the customer's optional requirements, with
``DAG_CONVERTER_MANAGED_ENV=1`` set. uv caches the environment and reuses it while those inputs are unchanged.
"""

import contextlib
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from dag_converter import __version__
from dag_converter.console_config import print_warning

ENV_FLAG = "DAG_CONVERTER_MANAGED_ENV"
TARGET_PYTHON = "3.12"
CONVERTER_DIST = "python-to-yaml-dag-converter-mwaa-serverless"
# Optional dependencies of the converter with the packages needed to convert Dags
CONVERTER_EXTRA = "airflow"
# Packages MWAA Serverless always uses, at the service's version. Customer pins on these are replaced.
SERVICE_OVERRIDES = {
    "apache-airflow": "3.0.6",
    "apache-airflow-providers-amazon": "9.30.0",
    "dag-factory": "1.0.0",
    "boto3": "1.43.1",
    "botocore": "1.43.1",
}
# uv never deletes cached environments by itself; prune them at most this often
PRUNE_INTERVAL_SECONDS = 7 * 24 * 3600
PRUNE_MARKER = ".last-prune"

logger = logging.getLogger(__name__)


class ManagedEnvError(Exception):
    """The conversion environment could not be prepared."""


def in_managed_env() -> bool:
    """True when already running inside the managed environment."""
    return os.environ.get(ENV_FLAG) == "1"


def cache_root() -> Path:
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local"
    else:
        base = os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    return Path(base) / "dag-converter"


def overrides_text() -> str:
    return "".join(f"{name}=={version}\n" for name, version in SERVICE_OVERRIDES.items())


def warn_overridden_pins(lines: list[str], source: str) -> None:
    """Warn for every requirement that pins a service package to a version MWAA Serverless does not use."""
    for line in lines:
        line = line.split(" #", 1)[0].strip()
        if not line or line.startswith(("#", "-")):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        service_version = SERVICE_OVERRIDES.get(canonicalize_name(req.name))
        if service_version and req.specifier and not req.specifier.contains(service_version, prereleases=True):
            print_warning(
                f"{source} pins {req.name}{req.specifier}, but MWAA Serverless always uses {service_version}; "
                f"converting with {service_version}."
            )


def _source_checkout() -> Path | None:
    """Root of the converter source checkout this module runs from, if any."""
    root = Path(__file__).resolve().parent.parent
    pyproject = root / "pyproject.toml"
    if pyproject.is_file() and re.search(rf'^name\s*=\s*"{CONVERTER_DIST}"', pyproject.read_text(), re.MULTILINE):
        return root
    return None


def converter_args() -> list[str]:
    """Where uv installs the converter from: the released version, or the source checkout we run from (editable,
    because uv would otherwise keep reusing its first build of the checkout)."""
    checkout = _source_checkout()
    if checkout is None:
        return ["--from", f"{CONVERTER_DIST}[{CONVERTER_EXTRA}]=={__version__}"]
    return ["--from", str(checkout), "--with-editable", f"{checkout}[{CONVERTER_EXTRA}]"]


def _find_uv() -> str:
    try:
        from uv import find_uv_bin

        return find_uv_bin()
    except (ImportError, FileNotFoundError) as e:
        raise ManagedEnvError(
            f"Failed to prepare the conversion environment: the {CONVERTER_DIST} installation is incomplete. "
            f"Reinstall it with: pip install --force-reinstall {CONVERTER_DIST}"
        ) from e


def installer_env(root: Path) -> dict[str, str]:
    """Environment for uv: private cache and Python install dirs, pip index settings mapped."""
    env = os.environ.copy()
    env["UV_CACHE_DIR"] = str(root / "uv-cache")
    env["UV_PYTHON_INSTALL_DIR"] = str(root / "python")
    env["UV_NO_CONFIG"] = "1"
    for pip_var, uv_var in (("PIP_INDEX_URL", "UV_INDEX_URL"), ("PIP_EXTRA_INDEX_URL", "UV_EXTRA_INDEX_URL")):
        if env.get(pip_var) and not env.get(uv_var):
            env[uv_var] = env[pip_var]
    return env


def run_in_env(command: list[str], requirements: Path | None) -> int:
    """Run ``command`` (looked up in the environment first, e.g. ``python``) inside the conversion environment;
    returns its exit code."""
    if requirements:
        warn_overridden_pins(requirements.read_text().splitlines(), requirements.name)
    uv = _find_uv()
    root = cache_root()
    with tempfile.TemporaryDirectory() as tmp:
        overrides = Path(tmp, "service-overrides.txt")
        overrides.write_text(overrides_text())
        cmd = [uv, "tool", "run", "--python", TARGET_PYTHON, *converter_args(), "--overrides", str(overrides)]
        if requirements:
            cmd += ["--with-requirements", str(requirements)]
        code = subprocess.run([*cmd, *command], env={**installer_env(root), ENV_FLAG: "1"}).returncode
    try:
        _maybe_prune(root, time.time())
    except Exception:  # cleanup must never fail the conversion
        logger.debug("Cache cleanup failed", exc_info=True)
    return code


def run_in_managed_env(argv: list[str], requirements: Path | None) -> int:
    """Re-run the same command (``argv`` without the program name) inside the managed environment; returns its exit
    code."""
    return run_in_env(["python", "-m", "dag_converter.cli", *argv], requirements)


def _maybe_prune(root: Path, now: float) -> None:
    marker = root / PRUNE_MARKER
    with contextlib.suppress(OSError):
        if now - marker.stat().st_mtime < PRUNE_INTERVAL_SECONDS:
            return
    if (root / "uv-cache").is_dir():
        subprocess.run(
            [_find_uv(), "cache", "prune", "--quiet"],
            env=installer_env(root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    root.mkdir(parents=True, exist_ok=True)
    marker.touch()


def cache_size(root: Path) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(dirpath, name)).st_size
    return total


def format_size(size: int) -> str:
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"
