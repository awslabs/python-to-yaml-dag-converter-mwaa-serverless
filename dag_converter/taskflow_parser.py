import ast
from pathlib import Path


class TaskFlowAnalyzer:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.task_functions = {}  # Map of function name to list of parameter names
        self.analyze_file()

    def analyze_file(self) -> None:
        """
        Analyze a Python file to find all functions with @task decorator
        and extract their parameter names.
        """
        with open(self.file_path) as file:
            source = file.read()

        tree = ast.parse(source)
        self._find_task_functions(tree)

    def _find_task_functions(self, tree: ast.AST) -> None:
        """Find all functions with @task decorator in the AST."""
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and self._has_task_decorator(node):
                # Extract function parameter names
                param_names = [arg.arg for arg in node.args.args]
                self.task_functions[node.name] = param_names

    def _has_task_decorator(self, func_node: ast.FunctionDef) -> bool:
        """Check if a function has the @task decorator."""
        for decorator in func_node.decorator_list:
            if (
                isinstance(decorator, ast.Name)
                and decorator.id == "task"
                or isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "task"
            ):
                return True

        return False
