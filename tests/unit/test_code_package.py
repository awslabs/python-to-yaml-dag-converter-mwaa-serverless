"""Tests for code_package module."""

import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dag_converter import code_package
from dag_converter.code_package import (
    PackagingError,
    build_zip,
    find_dag_root,
    find_local_paths,
    is_excluded,
    package_dag,
    rewrite_paths,
    static_imports,
    with_parent_inits,
    yaml_references,
)


class PackagingTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = (Path(self.temp_dir.name) / "dags").resolve().parent / "dags"
        self.root.mkdir()
        self.write("my_dag.py", "import os\nfrom helpers.util import run\n\ndef f():\n    import lazy_mod\n")
        self.write("helpers/__init__.py", "")
        self.write("helpers/util.py", "from . import sibling\nimport yaml\n\ndef run():\n    pass\n")
        self.write("helpers/sibling.py", "import importlib\nimportlib.import_module(name)\n")
        self.write("lazy_mod.py", "")
        self.write("callables/__init__.py", "")
        self.write("callables/tasks.py", "def cb():\n    pass\n")
        self.write("scripts/run.sh", "echo hi")
        self.write("scripts/profiles.yml", "secret: true")
        self.write("scripts/.DS_Store", "")
        self.write("scripts/__pycache__/x.pyc", "")
        self.write("venv/pyvenv.cfg", "")
        self.write("venv/lib/site.py", "")
        self.write("out/old.yaml", "")
        self.dag_file = self.root / "my_dag.py"

    def tearDown(self):
        self.temp_dir.cleanup()

    def write(self, rel, content):
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path


class TestDiscovery(PackagingTestCase):
    def test_static_imports_follow_local_modules(self):
        with patch.object(code_package, "print_warning") as warn:
            local, external = static_imports({self.dag_file}, self.root)
        rels = {p.relative_to(self.root).as_posix() for p in local}
        self.assertEqual(rels, {"helpers/__init__.py", "helpers/util.py", "helpers/sibling.py", "lazy_mod.py"})
        self.assertIn("yaml", external)
        self.assertIn("os", external)
        self.assertNotIn("helpers.util.run", external)
        self.assertIn("dynamic import", warn.call_args[0][0])

    def test_parent_inits(self):
        files = with_parent_inits({(self.root / "callables" / "tasks.py").resolve()}, self.root)
        self.assertIn((self.root / "callables" / "__init__.py").resolve(), files)

    def test_yaml_references(self):
        dag = {
            "d": {
                "tasks": {
                    "t": {
                        "operator": "airflow.providers.standard.operators.bash.BashOperator",
                        "python_callable": "callables.tasks.cb",
                        "bash_command": "scripts/run.sh",
                        "cwd": str(self.root / "scripts"),
                        "target_name": "dev",
                    }
                }
            }
        }
        modules, paths, external = yaml_references(dag, self.dag_file, self.root)
        self.assertEqual(modules, {(self.root / "callables" / "tasks.py").resolve()})
        self.assertEqual(paths, {(self.root / "scripts" / "run.sh").resolve(), (self.root / "scripts").resolve()})
        self.assertIn("airflow.providers.standard.operators.bash", external)

    def test_local_modules_loaded_since(self):
        sys.path.insert(0, str(self.root))
        try:
            snapshot = set(sys.modules)
            import lazy_mod  # noqa: F401

            found = code_package.local_modules_loaded_since(snapshot, self.root)
            self.assertEqual(found, {(self.root / "lazy_mod.py").resolve()})
            code_package.evict_local_modules(self.root)
            self.assertNotIn("lazy_mod", sys.modules)
        finally:
            sys.path.remove(str(self.root))

    def test_venv_inside_root_is_not_local(self):
        site = self.root / ".venv" / "lib" / "python3.12" / "site-packages"
        self.write(".venv/pyvenv.cfg", "")
        self.write(".venv/lib/python3.12/site-packages/third_party_mod.py", "import lazy_mod\n")
        sys.path[:0] = [str(self.root), str(site)]
        try:
            snapshot = set(sys.modules)
            import third_party_mod  # noqa: F401

            found = code_package.local_modules_loaded_since(snapshot, self.root)
            self.assertEqual(found, {self.root / "lazy_mod.py"})
            self.assertEqual(static_imports({site / "third_party_mod.py"}, self.root), (set(), set()))
            code_package.evict_local_modules(self.root)
            self.assertIn("third_party_mod", sys.modules)
            self.assertNotIn("lazy_mod", sys.modules)
        finally:
            sys.path[:] = [p for p in sys.path if p not in (str(self.root), str(site))]
            sys.modules.pop("third_party_mod", None)

    def test_files_read_while_parsing(self):
        config = self.write("configs/dags.yaml", "a: 1")
        self.write("venv/data.txt", "x")
        with code_package.record_opened_files() as opened:
            config.read_text()
            (self.root / "venv" / "data.txt").read_text()
            (self.root / "lazy_mod.py").read_text()
            (self.root / "written.log").write_text("x")
        config.read_text()  # outside the block: not recorded
        self.assertIn(str(config), opened)
        self.assertNotIn(str(self.root / "written.log"), opened)
        local, _ = code_package.discover_file_modules(self.dag_file, self.root, set(sys.modules), opened)
        rels = {p.relative_to(self.root).as_posix() for p in local}
        self.assertIn("configs/dags.yaml", rels)
        self.assertNotIn("venv/data.txt", rels)
        self.assertNotIn("written.log", rels)


class TestExclusions(PackagingTestCase):
    def excluded(self, rel):
        return is_excluded(self.root / rel, self.root, {self.dag_file.resolve()}, self.root / "out")

    def test_never_included(self):
        for rel in ["my_dag.py", "scripts/.DS_Store", "scripts/__pycache__/x.pyc", "venv/lib/site.py", "out/old.yaml"]:
            self.assertTrue(self.excluded(rel), rel)
        self.assertFalse(self.excluded("scripts/run.sh"))

    def test_rewrite_paths(self):
        (self.root / "dbt" / "proj").mkdir(parents=True)
        data = {"a": str(self.root / "dbt" / "proj"), "b": ["/elsewhere/x", str(self.root / "missing.sql")], "c": 1}
        result = rewrite_paths(data, self.root)
        self.assertEqual(result["a"], "/usr/local/airflow/dags/dbt/proj")
        # Only existing paths (which are packaged) are rewritten
        self.assertEqual(result["b"], ["/elsewhere/x", str(self.root / "missing.sql")])
        self.assertEqual(rewrite_paths(data, self.root, "/tmp/x")["a"], "/tmp/x/dbt/proj")

    def test_bash_command_paths_are_packaged_and_rewritten(self):
        script = str(self.root / "scripts" / "run.sh")
        dag = {"d": {"tasks": {"a": {"bash_command": f"{script} "}, "b": {"bash_command": f"bash {script} --x 1"}}}}
        _, paths, _ = yaml_references(dag, self.dag_file, self.root)
        self.assertEqual(paths, {self.root / "scripts" / "run.sh"})
        tasks = rewrite_paths(dag, self.root)["d"]["tasks"]
        self.assertEqual(tasks["a"]["bash_command"], "/usr/local/airflow/dags/scripts/run.sh ")
        self.assertEqual(tasks["b"]["bash_command"], "bash /usr/local/airflow/dags/scripts/run.sh --x 1")

    def test_dag_root_itself_is_rewritten_but_not_packaged(self):
        dag = {"d": {"template_searchpath": str(self.root)}}
        with patch.object(code_package, "print_warning") as warn:
            _, paths, _ = yaml_references(dag, self.dag_file, self.root)
        self.assertEqual(paths, set())
        self.assertIn("--include", warn.call_args[0][0])
        self.assertEqual(rewrite_paths(dag, self.root), {"d": {"template_searchpath": "/usr/local/airflow/dags"}})

    def test_subfolder_dag_module_refs(self):
        sub_dag = self.write("sub/mydag.py", "def func():\n    pass\n")
        dag = {"d": {"tasks": {"t": {"python_callable": "mydag.func", "operator": "x.y.Op"}, "u": {"x": "lazy_mod.f"}}}}
        result = code_package.rewrite_module_refs(dag, sub_dag, self.root)
        self.assertEqual(result["d"]["tasks"]["t"], {"python_callable": "sub.mydag.func", "operator": "x.y.Op"})
        self.assertEqual(code_package.rewrite_module_refs(result, self.dag_file, self.root), result)
        modules, _, _ = yaml_references(result, sub_dag, self.root)
        self.assertEqual(modules, {sub_dag})

    def test_find_local_paths(self):
        data = {
            "t": {
                "exe": str(Path.home() / "venv" / "bin" / "dbt"),
                "ok": "/usr/local/airflow/dags/x",
                "tmp": "echo hi > /tmp/out",
                "cfg": str(self.root / "missing.cfg"),
            }
        }
        self.assertEqual([k for k, _ in find_local_paths(data)], ["t.exe"])
        self.assertEqual([k for k, _ in find_local_paths(data, self.root)], ["t.exe", "t.cfg"])


class TestDagRoot(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project = Path(self.temp_dir.name).resolve()
        (self.project / "dags" / "sub").mkdir(parents=True)
        (self.project / "dags" / "sub" / "x.py").write_text("")
        self.cwd = os.getcwd()

    def tearDown(self):
        os.chdir(self.cwd)
        self.temp_dir.cleanup()

    def test_dags_folder_in_cwd_is_the_root(self):
        os.chdir(self.project)
        self.assertEqual(find_dag_root(self.project / "dags" / "sub" / "x.py"), self.project / "dags")
        self.assertEqual(find_dag_root(self.project / "dags" / "sub"), self.project / "dags")

    def test_otherwise_the_input_folder(self):
        os.chdir(self.project / "dags")
        self.assertEqual(find_dag_root(self.project / "dags" / "sub" / "x.py"), self.project / "dags" / "sub")
        self.assertEqual(find_dag_root(self.project / "dags" / "sub"), self.project / "dags" / "sub")

    def test_dag_root_on_import_path(self):
        root = self.project / "dags"
        with code_package.dag_root_on_import_path(root):
            self.assertIn(str(root), sys.path)
        self.assertNotIn(str(root), sys.path)


class TestZip(PackagingTestCase):
    def test_package_dag_contents(self):
        dag = {
            "d": {"tasks": {"t": {"python_callable": "callables.tasks.cb", "searchpath": [str(self.root / "scripts")]}}}
        }
        out = Path(self.temp_dir.name) / "output"
        out.mkdir()
        file_modules = static_imports({self.dag_file}, self.root)
        with (
            patch.object(code_package, "third_party_distributions", return_value={}) as dists,
            patch.object(code_package, "print_warning"),
        ):
            zip_path = package_dag(
                dag, "d", self.dag_file, self.root, file_modules, ["lazy_mod.py"], out, {self.dag_file}
            )
        self.assertIn("yaml", dists.call_args[0][0])
        self.assertEqual(zip_path, out / "d.zip")
        names = set(zipfile.ZipFile(zip_path).namelist())
        expected = {"helpers/__init__.py", "helpers/util.py", "helpers/sibling.py", "lazy_mod.py"}
        expected |= {"callables/__init__.py", "callables/tasks.py", "scripts/run.sh"}
        self.assertEqual(names, expected)

    def package(self, dag, includes=()):
        out = Path(self.temp_dir.name) / "output"
        out.mkdir(exist_ok=True)
        with (
            patch.object(code_package, "third_party_distributions", return_value={}),
            patch.object(code_package, "print_warning") as warn,
            patch.object(code_package, "print_info"),
        ):
            zip_path = package_dag(dag, "d", self.dag_file, self.root, (set(), set()), list(includes), out, set())
        return set(zipfile.ZipFile(zip_path).namelist()), " ".join(c.args[0] for c in warn.call_args_list)

    def test_no_package_when_nothing_to_bundle(self):
        out = Path(self.temp_dir.name) / "output"
        out.mkdir()
        with (
            patch.object(code_package, "third_party_distributions", return_value={}),
            patch.object(code_package, "stage_distributions") as stage,
            patch.object(code_package, "print_info") as info,
        ):
            result = package_dag(
                {"d": {"bash_command": "echo hi"}},
                "d",
                self.dag_file,
                self.root,
                (set(), set()),
                [],
                out,
                {self.dag_file},
            )
        self.assertIsNone(result)
        stage.assert_not_called()
        self.assertFalse((out / "d.zip").exists())
        info.assert_called_once_with("No code package needed for d")

    def test_secrets_are_skipped_with_warning_unless_included(self):
        dag = {"d": {"searchpath": str(self.root / "scripts")}}
        names, warnings = self.package(dag)
        self.assertNotIn("scripts/profiles.yml", names)
        self.assertIn("scripts/profiles.yml", warnings)
        names, warnings = self.package(dag, includes=["scripts/profiles.yml"])
        self.assertIn("scripts/profiles.yml", names)
        self.assertNotIn("secrets", warnings)

    def test_symlinks_keep_their_path_under_the_root(self):
        outside = Path(self.temp_dir.name) / "outside" / "shared"
        outside.mkdir(parents=True)
        (outside / "lib.py").write_text("")
        (self.root / "shared").symlink_to(outside)
        self.write("configs/prod.yaml", "a: 1")
        (self.root / "config.yaml").symlink_to(self.root / "configs" / "prod.yaml")
        (outside / "loop").symlink_to(outside)
        names, _ = self.package({"d": {"a": str(self.root / "config.yaml"), "b": str(self.root / "shared")}})
        self.assertEqual(names, {"config.yaml", "shared/lib.py"})

    def test_size_limit_warns_but_writes_zip(self):
        zip_path = Path(self.temp_dir.name) / "x.zip"
        with (
            patch.object(code_package, "MAX_CODE_SIZE_BYTES", 3),
            patch.object(code_package, "print_warning") as warn,
        ):
            build_zip(zip_path, {"scripts/run.sh": self.root / "scripts" / "run.sh"}, {})
        self.assertTrue(zip_path.exists())
        self.assertIn("exceeds the MWAA Serverless limit", warn.call_args.args[0])

    def test_package_with_only_third_party_packages(self):
        staged = Path(self.temp_dir.name) / "staged"
        (staged / "foo").mkdir(parents=True)
        (staged / "foo" / "__init__.py").write_text("")
        out = Path(self.temp_dir.name) / "output"
        out.mkdir()
        with (
            patch.object(code_package, "third_party_distributions", return_value={"foo": "1.0"}),
            patch.object(code_package, "stage_distributions", return_value={"foo": staged}) as stage,
            patch.object(code_package, "print_info"),
        ):
            zip_path = package_dag({"d": {}}, "d", self.dag_file, self.root, (set(), {"foo"}), [], out, set())
        stage.assert_called_once_with({"foo": "1.0"})
        self.assertEqual(zipfile.ZipFile(zip_path).namelist(), ["foo/__init__.py"])

    def test_install_failure_names_package(self):
        failed = type("Result", (), {"returncode": 1, "stderr": "No matching distribution found", "stdout": ""})
        with (
            patch.object(code_package.managed_env, "cache_root", return_value=Path(self.temp_dir.name) / "cache"),
            patch.object(code_package.subprocess, "run", return_value=failed),
            patch.object(code_package, "print_progress"),
            self.assertRaises(PackagingError) as ctx,
        ):
            code_package.stage_distributions({"sdist-only": "1.0"})
        self.assertIn("sdist-only", str(ctx.exception))
        self.assertIn("No matching distribution", str(ctx.exception))
        self.assertEqual(list((Path(self.temp_dir.name) / "cache").glob("wheels/*")), [])

    def test_stage_installs_local_version_once(self):
        ok = type("Result", (), {"returncode": 0, "stderr": "", "stdout": ""})

        def install(cmd, **kwargs):
            Path(cmd[cmd.index("--target") + 1]).mkdir(parents=True)
            return ok

        cache = Path(self.temp_dir.name) / "cache"
        with (
            patch.object(code_package.managed_env, "cache_root", return_value=cache),
            patch.object(code_package.subprocess, "run", side_effect=install) as run,
            patch.object(code_package, "print_progress"),
        ):
            first = code_package.stage_distributions({"foo": "1.0"})
            second = code_package.stage_distributions({"foo": "1.0"})
        run.assert_called_once()
        self.assertEqual(run.call_args[0][0][-1], "foo==1.0")
        self.assertEqual(run.call_args.kwargs["env"]["UV_CACHE_DIR"], str(cache / "uv-cache"))
        self.assertEqual(first, second)
        self.assertTrue(first["foo"].is_relative_to(cache / "wheels"))

    def test_install_command_flags(self):
        with patch.object(code_package.managed_env, "_find_uv", return_value="/bin/uv"):
            cmd = " ".join(code_package._install_command("foo==1.0", Path("/t")))
        self.assertTrue(cmd.startswith("/bin/uv pip install"))
        for flags in ("--no-deps", "--target /t", "--python-platform x86_64-manylinux_2_28", "--python-version 3.12"):
            self.assertIn(flags, cmd)
        self.assertIn("--only-binary :all: foo==1.0", cmd)

    def test_manifest(self):
        staged = Path(self.temp_dir.name) / "staged"
        (staged / "foo-1.0.dist-info").mkdir(parents=True)
        (staged / "foo-1.0.dist-info" / "METADATA").write_text("x")
        (staged / "foo").mkdir()
        (staged / "foo" / "__init__.py").write_text("")
        (staged / "bin").mkdir()
        (staged / "bin" / "foo").write_text("")
        (staged / ".complete").write_text("")
        (staged / ".lock").write_text("")
        with patch.object(code_package, "print_info") as info:
            build_zip(
                Path(self.temp_dir.name) / "x.zip",
                {"scripts/run.sh": self.root / "scripts" / "run.sh"},
                {"foo": staged},
            )
        manifest = "\n".join(c.args[0] for c in info.call_args_list)
        self.assertIn("(3 files,", manifest)
        self.assertIn("  scripts/run.sh (", manifest)
        self.assertIn("  package foo (2 files,", manifest)

    def test_extracted_on_import_path_hides_dag_root(self):
        zip_path = Path(self.temp_dir.name) / "code.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("lazy_mod.py", "")
        sys.path.insert(0, str(self.root))
        try:
            import lazy_mod

            with code_package.extracted_on_import_path(zip_path, self.root) as code_dir:
                self.assertTrue((code_dir / "lazy_mod.py").exists())
                self.assertNotIn(str(self.root), sys.path)
                self.assertNotIn("lazy_mod", sys.modules)
            self.assertIn(str(self.root), sys.path)
            # Local modules are restored for the next Dag of the same file
            self.assertIs(sys.modules["lazy_mod"], lazy_mod)
        finally:
            sys.path.remove(str(self.root))
            sys.modules.pop("lazy_mod", None)


class TestThirdParty(unittest.TestCase):
    def test_preinstalled_includes_service_overrides(self):
        self.assertLessEqual(set(code_package.managed_env.SERVICE_OVERRIDES), code_package.PREINSTALLED_PACKAGES)
        self.assertIn("duckdb", code_package.PREINSTALLED_PACKAGES)
        self.assertNotIn("duckdb", code_package.managed_env.SERVICE_OVERRIDES)

    def test_dependency_markers_are_evaluated_for_the_service(self):
        requires = {
            "a": ["b; sys_platform == 'linux'", "c; sys_platform == 'darwin'", "d; python_version < '3.12'", "e[x]"],
        }
        with patch.object(code_package.importlib.metadata, "requires", side_effect=lambda n: requires.get(n, [])):
            self.assertEqual(code_package.dependency_closure({"a"}), {"a", "b", "e"})

    def test_requirements_of_preinstalled_package_not_installed_locally(self):
        def requires(name):
            if name == "apache-airflow-providers-duckdb":
                raise code_package.importlib.metadata.PackageNotFoundError(name)
            return []

        with patch.object(code_package.importlib.metadata, "requires", side_effect=requires):
            self.assertIn("apache-airflow-providers-common-sql", code_package.service_provided())

    def test_excludes_stdlib_airflow_and_preinstalled(self):
        dists = code_package.third_party_distributions({"os", "json", "airflow.models", "yaml", "boto3"})
        self.assertEqual(dists, {})

    def test_provider_distribution(self):
        module = "airflow.providers.cncf.kubernetes.operators.pod"
        self.assertEqual(code_package._provider_distribution(module), "apache-airflow-providers-cncf-kubernetes")
        self.assertEqual(
            code_package._provider_distribution("airflow.providers.amazon.aws.hooks.s3"),
            "apache-airflow-providers-amazon",
        )
        self.assertIsNone(code_package._provider_distribution("airflow.providers.not_installed.hooks.x"))

    def test_bundles_provider_that_is_not_preinstalled(self):
        # cncf-kubernetes is installed with the converter's airflow extra but is not part of the service runtime
        dists = code_package.third_party_distributions(
            {"airflow.providers.cncf.kubernetes.operators.pod", "airflow.providers.amazon.aws.hooks.s3"}
        )
        self.assertIn("apache-airflow-providers-cncf-kubernetes", dists)
        self.assertNotIn("apache-airflow-providers-amazon", dists)
        self.assertNotIn("apache-airflow", dists)

    def test_bundles_non_preinstalled_package(self):
        # pytest is a dev dependency that is not part of the service runtime
        dists = code_package.third_party_distributions({"pytest", "_pytest.config"})
        self.assertIn("pytest", dists)
        self.assertNotIn("pyyaml", dists)

    def test_missing_distribution_is_skipped_with_warning(self):
        with (
            patch.object(code_package, "dependency_closure", side_effect=[{"not-installed-dist"}, set()]),
            patch.object(code_package, "print_warning") as warn,
        ):
            self.assertEqual(code_package.third_party_distributions({"x"}), {})
        self.assertIn("not-installed-dist", warn.call_args[0][0])
