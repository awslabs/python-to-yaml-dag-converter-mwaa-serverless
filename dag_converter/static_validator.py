import ast
from pathlib import Path

from dag_converter.conversion.exceptions import InvalidOperatorError
from dag_converter.schema_parser import ArgumentValidator


def validate_file(file_path: Path, validator: ArgumentValidator):
    operators_in_file = extract_operator_types(file_path)
    allowed_operators = validator.get_allowed_operators()
    for operator in operators_in_file:
        valid = False
        for allowed_operator in allowed_operators:
            if operator in allowed_operator:
                valid = True
                break
        if not valid:
            raise InvalidOperatorError(f"Operator {operator} is not supported")


def extract_dag_params(file_path: Path) -> list[str]:
    """Extract DAG constructor parameters from the AST"""
    with open(file_path) as f:
        code = f.read()

    tree = ast.parse(code)
    dag_params = []

    # Find DAG instantiation
    for node in ast.walk(tree):
        # Look for DAG constructor calls
        if not isinstance(node, ast.Call):
            continue

        # Check if it's a DAG constructor
        # TODO: does not support decorator instantiation of Dag
        if (isinstance(node.func, ast.Name) and node.func.id == "DAG") or (
            isinstance(node.func, ast.Attribute) and node.func.attr == "DAG"
        ):
            # Extract parameters from DAG constructor
            for keyword in node.keywords:
                dag_params.append(keyword.arg)

    return dag_params


def extract_operator_types(file_path: Path) -> set[str]:
    """
    Extract all operator types used in the DAG file.

    Args:
        file_path: Path to the Python file containing the DAG

    Returns:
        Set of operator class names (e.g., 'PythonOperator', 'S3Operator')
    """
    with open(file_path) as f:
        code = f.read()

    tree = ast.parse(code)
    operator_types = set()

    # Find all operator instantiations
    for node in ast.walk(tree):
        # Look for any Call nodes (function/class calls)
        if isinstance(node, ast.Call):
            # Get the operator class name
            if isinstance(node.func, ast.Name):
                # Direct class name like "PythonOperator(...)"
                class_name = node.func.id
                if "Operator" in class_name or "Sensor" in class_name:
                    operator_types.add(class_name)

        # Look for method calls like "SomeOperator.partial/expand/expand_kwargs(...)"
        elif (
            isinstance(node, ast.Attribute)
            and node.attr in ("partial", "expand", "expand_kwargs")
            and isinstance(node.value, ast.Name)
        ):
            # Direct class name like "PythonOperator.partial"
            class_name = node.value.id
            if "Operator" in class_name or "Sensor" in class_name:
                operator_types.add(class_name)

    return operator_types
