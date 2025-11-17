#!/usr/bin/env python3

import ast
import sys
from pathlib import Path


def extract_init_params(file_path):
    """Extract parameters from __init__ methods of classes in a Python file."""
    with open(file_path) as f:
        content = f.read()

    try:
        tree = ast.parse(content)
    except SyntaxError:
        return {}

    params = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    class_params = []
                    # Regular arguments
                    for arg in item.args.args:
                        if arg.arg not in ["self", "kwargs"]:
                            class_params.append(arg.arg)
                    # Keyword-only arguments (after *)
                    for arg in item.args.kwonlyargs:
                        if arg.arg != "kwargs":
                            class_params.append(arg.arg)
                    if class_params:
                        params[node.name] = class_params

    return params


def main():
    if len(sys.argv) != 2:
        print("Usage: python extract_operator_params.py <folder_path>")
        sys.exit(1)

    folder_path = Path(sys.argv[1])
    if not folder_path.exists():
        print(f"Folder {folder_path} does not exist")
        sys.exit(1)

    all_params = set()

    for py_file in folder_path.glob("*.py"):
        params = extract_init_params(py_file)
        for class_params in params.values():
            all_params.update(class_params)

    # Sort and format output
    for param in sorted(all_params):
        print(f"  {param}: any(required=False)")


if __name__ == "__main__":
    main()
