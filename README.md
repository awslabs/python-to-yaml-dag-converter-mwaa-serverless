## Python to Yaml Dag Converter for MWAA Serverless

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Python to Yaml Dag Converter for MWAA Serverless is a command-line tool for converting [Apache Airflow®](https://airflow.apache.org) Dags written in Python to [dag-factory](https://github.com/astronomer/dag-factory) compatible Dags written in YAML.

## Prerequisites

- Python 3.11+

## Benefits
- Onboard existing Python Dags to declarative YAML
- Ensures compatibility with DagFactory 1.0
- Convert Python Dags to MWAA Serverless compatible YAML
- Upload converted YAML to S3

## ⚡ Quick Start

### **Step 1**: Install
```
pip install python-to-yaml-dag-converter-mwaa-serverless
```
This installs only the command-line tool: Dags are converted in an isolated environment with Apache Airflow (see
[Conversion environment](#conversion-environment)). To also install Airflow and the other conversion packages in the
current environment, for `--skip-mwaa-serverless-managed-env`, install the `airflow` extra into a fresh virtual
environment (it pins Apache Airflow 3.0.6, replacing any other Airflow version there):
```
pip install 'python-to-yaml-dag-converter-mwaa-serverless[airflow]'
```

### **Step 2**: Use
```
dag-converter convert <python-dag-file>
```
Call with the `--help` flag for more options

If your Dags import packages that the [conversion environment](#conversion-environment) doesn't include, list them in a requirements file:
```
dag-converter convert <python-dag-file> --requirements requirements.txt
```

### **Step 3**: Review
Review the converted Dags in the `output_yaml` folder in the current directory

## Options
* `--help` Show help message and exit
* `--output`: PATH Path to output converted YAML Dag(s) to [default: output_yaml]
* `--bucket`: TEXT S3 bucket to upload converted Dags and their code packages to. Uses local AWS credentials
* `--validate/--no-validate`: Validate the output YAML using DagFactory
* `--debug/--no-debug` Enable logging DagBag objects to .initial_task_attributes before and after conversion
* `--requirements`: FILE requirements.txt with extra packages your Dags import. The conversion environment comes with Python 3.12 and apache-airflow==3.0.6, apache-airflow-providers-amazon==9.30.0, dag-factory==1.0.0, boto3==1.43.1, botocore==1.43.1
* `--skip-mwaa-serverless-managed-env`: Parse Dags with the packages in your current Python environment instead of the isolated conversion environment. Needs the `airflow` extra installed. `--requirements` is ignored
* `--include`: TEXT Glob, relative to the Dag folder, of extra files to add to the code package (repeatable)

## Conversion environment
`dag-converter convert` parses your Dags in an isolated environment that matches MWAA Serverless: Python 3.12,
Apache Airflow 3.0.6 and the service's versions of the Amazon provider, dag-factory, boto3 and botocore, plus any
packages from `--requirements`. If your requirements pin one of those packages to a different version, the service
version is used instead and a warning is shown.

* The first run needs network access: it downloads the packages (and Python 3.12 if it is not installed), which
  may take a minute. Later runs reuse the cached environment.
* Private package indexes are used when set through the `PIP_INDEX_URL` and `PIP_EXTRA_INDEX_URL` environment
  variables (`pip.conf` is not read).
* Environments are created with `uv tool run` and cached in `~/.cache/dag-converter` (`$XDG_CACHE_HOME/dag-converter`
  if set, `%LOCALAPPDATA%\dag-converter` on Windows). A changed requirements file, including files it includes with
  `-r`, gets a new environment. Cleanup is automatic: unused environments are pruned once a week.
* `dag-converter cache info` shows the cache location and size; `dag-converter cache clean` deletes it.

## Code package
MWAA Serverless extracts a workflow's code zip to `/usr/local/airflow/dags`, which is on the import path. When a Dag
needs files beyond its YAML, the converter writes `<dag_id>.zip` next to `<dag_id>.yaml`:

* The Dag folder is `./dags` if it contains the input, otherwise the input's folder (a file's parent). Files are
  stored relative to it, and it is on the import path while converting, as on the service.
* Local modules the Dag imports (while parsing or inside its callables), their parent `__init__.py` files, other
  files read while parsing (e.g. a config file the Dags are generated from), and files and folders the YAML refers
  to are included. Files in virtualenvs and likely secrets (`.env`, `*.pem`,
  `*.key`, `profiles.yml`) are left out; add files the converter cannot find, or secrets you need, with `--include`.
* Third-party packages that are not pre-installed on the service are bundled at the zip root with their
  dependencies, at the versions used for conversion, as Linux x86_64 / Python 3.12 wheels. Packages without such a
  wheel cannot be bundled. Wheels are cached in the conversion cache.
* Paths under the Dag folder in the YAML are rewritten to `/usr/local/airflow/dags/...`; other local paths are
  reported as warnings.
* With `--validate`, the YAML is validated against the extracted zip instead of the Dag folder.
* The service rejects code packages over 250 MB uncompressed; larger packages are written with a warning.

## Limitations
* Behavior that is not supported in `dag-factory` will not be converted
* Dynamic task mapping conversion is not supported

## Examples
* See supported [examples](https://github.com/awslabs/python-to-yaml-dag-converter-mwaa-serverless/blob/main/dev/integ_test/python_test_folder/supported)
* Try one out
```
dag-converter convert airflow_s3.py
```

## Contributing and Bug Reporting

See [CONTRIBUTING](CONTRIBUTING.md#contributing-guidelines) for more information.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
