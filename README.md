## Python to Yaml Dag Converter for MWAA Serverless

Python to Yaml Dag Converter for MWAA Serverless is a command-line tool for converting [Apache Airflow®](https://airflow.apache.org) DAGs written in Python to [dag-factory](https://github.com/astronomer/dag-factory) compatible DAGs written in YAML.

## Benefits
- Onboard existing Python DAGs to declarative YAML
- Convert Python DAGs to MWAA Serverless compatible YAML

## Installation
```
pip install python-to-yaml-dag-converter-mwaa-serverless
```

## Usage
```
dag-converter convert <python-dag-file>
```
Converted DAGs will be outputted to `output_yaml` unless overridden using the `--output` flag.

Call with `--help` for more options.
```
dag-converter convert --help
```

## Limitations
* Python and Bash Operators are not supported for conversion
* Behavior that is not supported in `dag-factory` will not be converted

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
