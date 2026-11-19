"""Build a per-Dag code zip for MWAA Serverless, when the Dag needs one.

The service extracts the zip to /usr/local/airflow/dags, which is on the import path, so local files are stored
relative to the Dag folder root.

Local files are the union of:
* local modules newly imported while parsing the Dag file (sys.modules diff; files are parsed one at a time and
  local modules are evicted afterwards, so attribution is per file)
* other local files read while parsing the Dag file, e.g. a config file Dags are generated from
* a static AST walk of imports, recursively over local modules (finds imports that only run at task time)
* paths and module references found in the generated YAML
* parent ``__init__.py`` files of every included module
Files inside virtualenvs/site-packages under the Dag folder are never treated as local.

Third-party packages: imported top-level modules are mapped to distributions; the standard library, Airflow and
everything pre-installed on the service (plus their dependency closure, computed from local metadata with the
service's environment markers) are excluded. The remaining distributions and their dependencies (again minus what
the service provides) are installed at the versions used for parsing, as Linux x86_64 / Python 3.12 wheels, and
stored at the zip root.
"""

import ast
import contextlib
import fnmatch
import importlib.metadata
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from packaging.markers import default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from dag_converter import managed_env
from dag_converter.console_config import print_info, print_progress, print_warning

# Path the code zip is extracted to (and which is on the import path) on the service
SERVICE_DAGS_DIR = "/usr/local/airflow/dags"
MAX_CODE_SIZE_BYTES = 250 * 1024 * 1024
SKIP_DIR_NAMES = {".git", "__pycache__"}
SKIP_FILE_PATTERNS = ["*.pyc", ".DS_Store"]
SECRET_FILE_PATTERNS = ["profiles.yml", ".env", "*.pem", "*.key"]
MODULE_REFERENCE_KEYS = {"python_callable", "operator", "__type__"}
# Wheel platform of the service (linux x86_64, glibc 2.28+); also accepts older manylinux tags
UV_PLATFORM = "x86_64-manylinux_2_28"
# Distributions pre-installed on the service. They take precedence over same-named packages in the code zip, so
# they are never bundled. Only some are forced to the service's version in the conversion environment.
PREINSTALLED_PACKAGES = {
    *managed_env.SERVICE_OVERRIDES,
    "apache-airflow-providers-duckdb",
    "duckdb",
    "pyyaml",
    "lz4",
}
# Runtime requirements of pre-installed packages that the conversion environment does not install, used when they
# are not installed locally (from their PyPI metadata; duckdb and lz4 have none outside extras)
PREINSTALLED_REQUIREMENTS = {
    "apache-airflow-providers-duckdb": ["apache-airflow", "apache-airflow-providers-common-sql", "duckdb"],
}
# Environment markers of the service, used to evaluate dependency markers
TARGET_MARKERS = {
    "sys_platform": "linux",
    "platform_system": "Linux",
    "platform_machine": "x86_64",
    "python_version": managed_env.TARGET_PYTHON,
    "python_full_version": f"{managed_env.TARGET_PYTHON}.0",
    "implementation_name": "cpython",
    "extra": "",
}


class PackagingError(Exception):
    pass


# ---------------------------------------------------------------------------------------------
# Path helpers. Paths are absolute but not symlink-resolved, so a symlink inside the Dag folder
# keeps its own name in the zip; resolve() is only used to compare file identities.


def find_dag_root(input_path: Path) -> Path:
    """The Dag folder root (``/usr/local/airflow/dags`` on the service): ``./dags`` if it contains the input,
    otherwise the input's folder (a file's parent)."""
    path = Path(input_path).absolute()
    cwd_dags = (Path.cwd() / "dags").resolve()
    for candidate in (path, *path.parents):
        if candidate.resolve() == cwd_dags:  # keep the input's form of the path (e.g. /tmp vs /private/tmp)
            return candidate
    return path if path.is_dir() else path.parent


@contextlib.contextmanager
def dag_root_on_import_path(dag_root: Path) -> Iterator[None]:
    """Mirror the service, where the Dag folder root is on the import path."""
    root = str(dag_root)
    added = root not in sys.path
    if added:
        sys.path.append(root)
    try:
        yield
    finally:
        if added and root in sys.path:
            sys.path.remove(root)


def _roots(dag_root: Path) -> list[str]:
    """Absolute and symlink-resolved forms of the Dag folder root (e.g. /tmp vs /private/tmp)."""
    return list(dict.fromkeys([os.path.abspath(dag_root), str(Path(dag_root).resolve())]))


def relative_to_root(path: str | Path, dag_root: Path) -> str | None:
    """Posix path of ``path`` relative to the Dag folder root: "" for the root itself, None if outside it."""
    path = os.path.abspath(path)
    for root in _roots(dag_root):
        if path == root:
            return ""
        if path.startswith(root + os.sep):
            return Path(path[len(root) + 1 :]).as_posix()
    return None


def _in_venv(path: Path, dag_root: Path) -> bool:
    """Whether path is inside a virtualenv directory (one with pyvenv.cfg) below the Dag folder root."""
    current = Path(os.path.abspath(path)).parent
    while relative_to_root(current, dag_root):
        if (current / "pyvenv.cfg").exists():
            return True
        current = current.parent
    return False


def _is_local_module_file(file: str | Path, dag_root: Path) -> bool:
    """A file under the Dag folder root that is not part of an installed environment inside it."""
    path = os.path.abspath(file)
    if not relative_to_root(path, dag_root) or {"site-packages", "dist-packages"} & set(Path(path).parts):
        return False
    prefixes = {os.path.abspath(p) for p in (sys.prefix, sys.base_prefix)}
    if any(path.startswith(p + os.sep) and relative_to_root(p, dag_root) for p in prefixes):
        return False
    return not _in_venv(Path(path), dag_root)


def _is_secret(rel: str) -> bool:
    return any(fnmatch.fnmatch(rel.rsplit("/", 1)[-1], p) for p in SECRET_FILE_PATTERNS)


def is_excluded(path: Path, dag_root: Path, excluded_files: set[Path], output_dir: Path) -> bool:
    rel = relative_to_root(path, dag_root)
    if not rel:
        return True
    parts = rel.split("/")
    if any(part in SKIP_DIR_NAMES for part in parts):
        return True
    if any(fnmatch.fnmatch(parts[-1], p) for p in SKIP_FILE_PATTERNS):
        return True
    real = path.resolve()
    if real in excluded_files:
        return True
    out = Path(output_dir).resolve()
    if real == out or out in real.parents:
        return True
    return _in_venv(path, dag_root)


def _walk(path: Path) -> Iterator[Path]:
    if path.is_file():
        yield path
        return
    visited: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(path, followlinks=True):
        real = os.path.realpath(dirpath)
        if real in visited:
            dirnames[:] = []
            continue
        visited.add(real)
        dirnames[:] = sorted(
            d for d in dirnames if d not in SKIP_DIR_NAMES and not (Path(dirpath) / d / "pyvenv.cfg").exists()
        )
        for name in sorted(filenames):
            if (Path(dirpath) / name).is_file():
                yield Path(dirpath) / name


# ---------------------------------------------------------------------------------------------
# Local module discovery


def resolve_module(name: str, dag_root: Path) -> Path | None:
    """File of a module importable from the Dag folder root, if it is a local module."""
    base = Path(os.path.abspath(dag_root)).joinpath(*name.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _local_modules(dag_root: Path) -> dict[str, Any]:
    """Loaded local modules (excluding Dag file modules, which DagBag names ``unusual_prefix_*``)."""
    return {
        name: module
        for name, module in list(sys.modules.items())
        if not name.startswith("unusual_prefix_")
        and getattr(module, "__file__", None)
        and _is_local_module_file(module.__file__, dag_root)
    }


def local_modules_loaded_since(snapshot: set[str], dag_root: Path) -> set[Path]:
    """Files of local modules imported since ``snapshot``."""
    return {Path(os.path.abspath(m.__file__)) for name, m in _local_modules(dag_root).items() if name not in snapshot}


def evict_local_modules(dag_root: Path) -> dict[str, Any]:
    """Drop local modules from sys.modules so the next Dag file (or validation) re-imports them."""
    removed = _local_modules(dag_root)
    for name in removed:
        del sys.modules[name]
    return removed


def _imports_in(tree: ast.AST, py_file: Path, dag_root: Path) -> tuple[set[str], list[str]]:
    """Absolute module names imported anywhere in the file, plus dynamic-import warnings."""
    names: set[str] = set()
    warnings = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                package_dir = Path(os.path.abspath(py_file)).parent
                for _ in range(node.level - 1):
                    package_dir = package_dir.parent
                prefix = relative_to_root(package_dir, dag_root)
                if prefix is None:
                    continue
                module = ".".join(p for p in (prefix.replace("/", "."), node.module or "") if p)
            else:
                module = node.module or ""
            if module:
                names.add(module)
            names.update(f"{module}.{alias.name}" if module else alias.name for alias in node.names)
        elif isinstance(node, ast.Call):
            func = node.func
            func_name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if func_name in ("import_module", "__import__"):
                arg = node.args[0] if node.args else None
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    names.add(arg.value)
                else:
                    warnings.append(
                        f"{py_file}:{node.lineno}: dynamic import with a non-literal module name; "
                        "the module cannot be discovered automatically, use --include"
                    )
    return names, warnings


def static_imports(py_files: set[Path], dag_root: Path) -> tuple[set[Path], set[str]]:
    """Recursively follow imports from ``py_files``; return (local files, external module names)."""
    local: set[Path] = set()
    external: set[str] = set()
    pending = list(py_files)
    seen: set[Path] = set()
    while pending:
        py_file = Path(os.path.abspath(pending.pop()))
        if py_file in seen or py_file.suffix != ".py" or not _is_local_module_file(py_file, dag_root):
            continue
        seen.add(py_file)
        try:
            tree = ast.parse(py_file.read_text(), filename=str(py_file))
        except (SyntaxError, UnicodeDecodeError, OSError):
            continue
        names, warnings = _imports_in(tree, py_file, dag_root)
        for warning in warnings:
            print_warning(warning)
        for name in names:
            resolved = resolve_module(name, dag_root)
            if resolved and _is_local_module_file(resolved, dag_root):
                local.add(resolved)
                pending.append(resolved)
            elif "." not in name or not resolve_module(name.rsplit(".", 1)[0], dag_root):
                # "from local_mod import func" yields "local_mod.func": local, not external
                external.add(name)
    return local, external


def with_parent_inits(files: set[Path], dag_root: Path) -> set[Path]:
    """Add the ``__init__.py`` of every package containing an included module."""
    result = set(files)
    for file in files:
        if file.suffix != ".py":
            continue
        parent = Path(os.path.abspath(file)).parent
        while relative_to_root(parent, dag_root):
            if (parent / "__init__.py").is_file():
                result.add(parent / "__init__.py")
            parent = parent.parent
    return result


# ---------------------------------------------------------------------------------------------
# YAML references and path rewriting


def iter_strings(obj: Any, path: str = "") -> Iterator[tuple[str, str, str]]:
    """Yield (yaml key path, last key, string value) for every string in a nested structure."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield from iter_strings(value, f"{path}.{key}" if path else str(key))
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            yield from iter_strings(value, f"{path}[{index}]")
    elif isinstance(obj, str):
        yield path, path.rsplit(".", 1)[-1], obj


def _absolute_refs(value: str, dag_root: Path) -> list[str]:
    """Existing absolute paths under (or equal to) the Dag folder root in a YAML string: the whole value
    (stripped), otherwise each whitespace-separated word, e.g. bash_command "bash /x/dags/run.sh arg"."""
    if len(value) > 1024:
        return []
    words = [value.strip()] + value.split()
    refs = [w for w in dict.fromkeys(words) if os.path.isabs(w) and relative_to_root(w, dag_root) is not None]
    refs = [w for w in refs if os.path.exists(w)]
    return refs[:1] if refs[:1] == words[:1] else refs


def _module_ref(value: str, dag_file: Path, dag_root: Path) -> str:
    """Dotted path from the Dag folder root for a module reference that only resolves next to the Dag file,
    e.g. "mydag.func" for dags/sub/mydag.py becomes "sub.mydag.func"."""
    module, _, attr = value.rpartition(".")
    if not module or resolve_module(module, dag_root):
        return value
    file = resolve_module(module, Path(dag_file).parent)
    rel = relative_to_root(file, dag_root) if file else None
    if not rel:
        return value
    parts = Path(rel).with_suffix("").parts
    return ".".join([*(parts[:-1] if parts[-1] == "__init__" else parts), attr])


def rewrite_module_refs(obj: Any, dag_file: Path, dag_root: Path) -> Any:
    """Apply ``_module_ref`` to every python_callable/operator/__type__ value."""
    if isinstance(obj, dict):
        return {
            key: _module_ref(value, dag_file, dag_root)
            if key in MODULE_REFERENCE_KEYS and isinstance(value, str)
            else rewrite_module_refs(value, dag_file, dag_root)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [rewrite_module_refs(value, dag_file, dag_root) for value in obj]
    return obj


def yaml_references(dag_dict: dict, dag_file: Path, dag_root: Path) -> tuple[set[Path], set[Path], set[str]]:
    """Return (local module files, other local files/dirs, external module names) referenced in YAML."""
    modules: set[Path] = set()
    paths: set[Path] = set()
    external: set[str] = set()
    search_dirs = [Path(dag_root), Path(dag_file).parent]
    for yaml_path, key, value in iter_strings(dag_dict):
        if key in MODULE_REFERENCE_KEYS and "." in value:
            module = value.rsplit(".", 1)[0]
            resolved = resolve_module(module, dag_root) or resolve_module(value, dag_root)
            if resolved:
                modules.add(resolved)
            else:
                external.add(module)
            continue
        refs = _absolute_refs(value, dag_root)
        for ref in refs:
            if relative_to_root(ref, dag_root):
                paths.add(Path(os.path.abspath(ref)))
            else:
                print_warning(
                    f"{yaml_path} is the Dag folder root ({ref}); it becomes {SERVICE_DAGS_DIR} but is not "
                    "packaged as a whole. Files under it that are referenced in the YAML are included; add any "
                    "other files used from it (e.g. templates) with --include"
                )
        stripped = value.strip()
        if refs or os.path.isabs(stripped) or "\n" in value or len(value) > 1024 or not stripped:
            continue
        if "/" in stripped or re.search(r"\.\w+$", stripped):
            for candidate in (d / stripped for d in search_dirs):
                if relative_to_root(candidate, dag_root) and candidate.exists():
                    paths.add(Path(os.path.abspath(candidate)))
                    break
    return modules, paths, external


def _service_path(path: str, dag_root: Path, target: str) -> str:
    rel = relative_to_root(path, dag_root)
    return f"{target.rstrip('/')}/{rel}" if rel else target.rstrip("/")


def rewrite_paths(obj: Any, dag_root: Path, target: str = SERVICE_DAGS_DIR) -> Any:
    """Rewrite the paths found by ``_absolute_refs`` (so only what is packaged) to ``target/<relative>``."""
    if isinstance(obj, dict):
        return {key: rewrite_paths(value, dag_root, target) for key, value in obj.items()}
    if isinstance(obj, list):
        return [rewrite_paths(value, dag_root, target) for value in obj]
    if isinstance(obj, str):
        refs = _absolute_refs(obj, dag_root)
        if refs == [obj.strip()]:
            return obj.replace(refs[0], _service_path(refs[0], dag_root, target), 1)
        return re.sub(r"\S+", lambda m: _service_path(m[0], dag_root, target) if m[0] in refs else m[0], obj)
    return obj


def find_local_paths(obj: Any, dag_root: Path | None = None) -> list[tuple[str, str]]:
    """Return (yaml key path, value) for strings containing paths under the home directory or the Dag folder
    root, which do not exist on the service."""
    prefixes = {str(Path.home()), os.path.realpath(Path.home()), *(_roots(dag_root) if dag_root else [])}
    prefixes = {p for p in prefixes if p and p != "/"}
    return [(path, value) for path, _, value in iter_strings(obj) if any(p in value for p in prefixes)]


def warn_local_paths(obj: Any, dag_root: Path | None = None) -> None:
    found = find_local_paths(obj, dag_root)
    if found:
        lines = "\n".join(f"  {path}: {value}" for path, value in found)
        print_warning(
            "The YAML contains machine-specific local paths that will not exist on MWAA Serverless:\n" + lines
        )


# ---------------------------------------------------------------------------------------------
# Third-party distributions


def _requirements_of(dist_name: str) -> list[str]:
    try:
        requires = importlib.metadata.requires(dist_name) or []
    except importlib.metadata.PackageNotFoundError:
        return PREINSTALLED_REQUIREMENTS.get(canonicalize_name(dist_name), [])
    env = {**default_environment(), **TARGET_MARKERS}
    names = []
    for line in requires:
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        if req.marker is None or req.marker.evaluate(env):
            names.append(canonicalize_name(req.name))
    return names


def dependency_closure(names: set[str]) -> set[str]:
    """Normalized names of ``names`` and their runtime dependencies on the service platform (from local metadata)."""
    closure: set[str] = set()
    pending = [canonicalize_name(n) for n in names]
    while pending:
        name = pending.pop()
        if name in closure:
            continue
        closure.add(name)
        pending.extend(_requirements_of(name))
    return closure


def service_provided() -> set[str]:
    """Distributions available on the service: the pre-installed packages and their dependencies."""
    return dependency_closure(PREINSTALLED_PACKAGES)


def _provider_distribution(module_name: str) -> str | None:
    """Installed provider distribution owning ``airflow.providers.<a>.<b>...``.

    E.g. ``airflow.providers.cncf.kubernetes.operators.pod`` -> ``apache-airflow-providers-cncf-kubernetes``."""
    parts = module_name.split(".")[2:]
    for end in range(len(parts), 0, -1):
        name = "apache-airflow-providers-" + "-".join(parts[:end])
        try:
            importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError:
            continue
        return canonicalize_name(name)
    return None


def third_party_distributions(module_names: set[str]) -> dict[str, str]:
    """Map imported modules to the distributions to bundle: {normalized name: local version}."""
    mapping = importlib.metadata.packages_distributions()
    # Providers live under the "airflow" namespace shared with core, so map them by module path instead
    requested: set[str] = {
        dist
        for name in module_names
        if name.startswith("airflow.providers.") and (dist := _provider_distribution(name))
    }
    for top in sorted({name.split(".")[0] for name in module_names}):
        if top in sys.stdlib_module_names or top in ("airflow", "dag_converter", "__future__"):
            continue
        requested.update(canonicalize_name(dist) for dist in mapping.get(top, []))
    bundled = {}
    for name in sorted(dependency_closure(requested) - service_provided()):
        try:
            bundled[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            print_warning(f"Distribution '{name}' is required but not installed locally; skipping it")
    return bundled


def _install_command(spec: str, target: Path) -> list[str]:
    return [
        *(managed_env._find_uv(), "pip", "install", "--quiet", "--no-deps", "--target", str(target)),
        *("--python", sys.executable, "--python-platform", UV_PLATFORM),
        *("--python-version", managed_env.TARGET_PYTHON, "--only-binary", ":all:", spec),
    ]


def stage_distributions(dists: dict[str, str]) -> dict[str, Path]:
    """Install the locally used version of each distribution for the service platform into a cached staging dir."""
    root = managed_env.cache_root()
    staged = {}
    for name, version in sorted(dists.items()):
        spec = f"{name}=={version}"
        target = root / "wheels" / f"{name}-{version}-{UV_PLATFORM}-py{managed_env.TARGET_PYTHON}"
        if not (target / ".complete").exists():
            shutil.rmtree(target, ignore_errors=True)
            print_progress(f"Installing {spec} for linux x86_64 / Python {managed_env.TARGET_PYTHON}...")
            result = subprocess.run(
                _install_command(spec, target), capture_output=True, text=True, env=managed_env.installer_env(root)
            )
            if result.returncode != 0:
                shutil.rmtree(target, ignore_errors=True)
                raise PackagingError(
                    f"Could not install '{name}' for linux x86_64 / Python {managed_env.TARGET_PYTHON} "
                    f"(a binary wheel is required). Installer output:\n{(result.stderr or result.stdout).strip()}"
                )
            (target / ".complete").write_text(spec)
        staged[name] = target
    return staged


# ---------------------------------------------------------------------------------------------
# Zip


def _human(size: int) -> str:
    return f"{size / (1024 * 1024):.2f} MB" if size >= 1024 * 1024 else f"{size / 1024:.1f} KB"


def build_zip(zip_path: Path, local_files: dict[str, Path], staged: dict[str, Path]) -> None:
    """Write the zip, warn above the size limit and print a manifest."""
    entries: dict[str, Path] = dict(sorted(local_files.items()))
    dist_summary = []
    for name, directory in sorted(staged.items()):
        count, size = 0, 0
        for file in _walk(directory):
            arcname = file.relative_to(directory).as_posix()
            # .lock is uv's lock file for the target folder
            if arcname in (".complete", ".lock") or arcname.startswith("bin/") or arcname.endswith(".pyc"):
                continue
            if arcname not in entries:
                entries[arcname] = file
                count += 1
                size += file.stat().st_size
        dist_summary.append((name, count, size))

    total = sum(file.stat().st_size for file in entries.values())
    if total > MAX_CODE_SIZE_BYTES:
        largest = ", ".join(
            f"{name} ({_human(size)})" for name, _, size in sorted(dist_summary, key=lambda d: -d[2])[:5]
        )
        print_warning(
            f"Code package {zip_path.name} is {_human(total)} uncompressed, which exceeds the MWAA Serverless "
            f"limit of {_human(MAX_CODE_SIZE_BYTES)}; the service will reject it"
            + (f". Largest packages: {largest}" if largest else "")
        )

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for arcname, file in entries.items():
            zf.write(file, arcname)

    print_info(f"Code package {zip_path} ({len(entries)} files, {_human(total)} uncompressed):")
    for arcname, file in sorted(local_files.items()):
        print_info(f"  {arcname} ({_human(file.stat().st_size)})")
    for name, count, size in dist_summary:
        print_info(f"  package {name} ({count} files, {_human(size)})")


@contextlib.contextmanager
def extracted_on_import_path(zip_path: Path, dag_root: Path) -> Iterator[Path]:
    """Extract the zip to a temp dir and hide the Dag folder root (path and loaded modules) while validating."""
    old_sys_path = list(sys.path)
    hidden = set(_roots(dag_root))
    with tempfile.TemporaryDirectory(prefix="dag-converter-code-") as temp_dir:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(temp_dir)
        evicted = evict_local_modules(dag_root)
        sys.path[:] = [p for p in sys.path if os.path.abspath(p or os.getcwd()) not in hidden]
        try:
            yield Path(temp_dir)
        finally:
            sys.path[:] = old_sys_path
            evict_local_modules(Path(temp_dir))
            # Later Dags of the same file still reference these modules
            sys.modules.update(evicted)


# ---------------------------------------------------------------------------------------------
# Orchestration


_opened_files: set[str] | None = None


def _record_open(event: str, args: tuple) -> None:
    if event == "open" and _opened_files is not None and isinstance(args[0], str):
        mode = args[1] if isinstance(args[1], str) else "r"
        if not set(mode) & set("wax+"):
            _opened_files.add(os.path.abspath(args[0]))


@contextlib.contextmanager
def record_opened_files() -> Iterator[set[str]]:
    """Collect the paths of files opened for reading inside the block."""
    global _opened_files
    if not getattr(record_opened_files, "hooked", False):
        # Audit hooks cannot be removed, so one hook is added and only records inside this block
        sys.addaudithook(_record_open)
        record_opened_files.hooked = True
    _opened_files = opened = set()
    try:
        yield opened
    finally:
        _opened_files = None


def discover_file_modules(
    dag_file: Path, dag_root: Path, snapshot: set[str], opened: set[str] = frozenset()
) -> tuple[set[Path], set[str]]:
    """Local files (modules, and other files in ``opened``) and external module names for one parsed Dag file."""
    loaded = local_modules_loaded_since(snapshot, dag_root)
    local, external = static_imports({Path(dag_file), *loaded}, dag_root)
    data = {
        Path(path)
        for path in opened
        if not path.endswith((".py", ".pyc")) and os.path.isfile(path) and _is_local_module_file(path, dag_root)
    }
    return loaded | local | data, external


def package_dag(
    dag_dict: dict,
    dag_id: str,
    dag_file: Path,
    dag_root: Path,
    file_modules: tuple[set[Path], set[str]],
    includes: list[str],
    output_dir: Path,
    dag_files: set[Path],
) -> Path | None:
    """Build ``<output>/<dag_id>.zip`` if the Dag needs local files or third-party packages; returns the zip path
    or None."""
    module_files, external = file_modules
    yaml_modules, yaml_paths, yaml_external = yaml_references(dag_dict, dag_file, dag_root)
    yaml_local, yaml_local_external = static_imports(yaml_modules, dag_root)
    modules = with_parent_inits(module_files | yaml_modules | yaml_local, dag_root)

    excluded_dag_files = {Path(f).resolve() for f in dag_files}
    files: dict[str, Path] = {}
    secrets: set[str] = set()

    def add(path: Path, allow_dag_file: bool, explicit: bool = False) -> None:
        skip = set() if allow_dag_file else excluded_dag_files
        for file in _walk(path):
            if is_excluded(file, dag_root, skip, output_dir):
                continue
            rel = relative_to_root(file, dag_root)
            if _is_secret(rel) and not explicit:
                secrets.add(rel)
            else:
                files[rel] = file

    for module in modules:
        add(module, allow_dag_file=True)
    for path in yaml_paths:
        add(path, allow_dag_file=False)
    for pattern in includes:
        matched = list(Path(dag_root).glob(pattern))
        if not matched:
            print_warning(f"--include pattern '{pattern}' did not match anything under {dag_root}")
        for path in matched:
            add(path, allow_dag_file=False, explicit=True)
    if secrets - files.keys():
        print_warning(
            "Not packaging likely secrets (add them with --include if they are needed): "
            + ", ".join(sorted(secrets - files.keys()))
        )

    dists = third_party_distributions(external | yaml_external | yaml_local_external)
    zip_path = Path(output_dir) / f"{dag_id}.zip"
    if not files and not dists:
        zip_path.unlink(missing_ok=True)  # from an earlier run
        print_info(f"No code package needed for {dag_id}")
        return None
    build_zip(zip_path, files, stage_distributions(dists))
    return zip_path
