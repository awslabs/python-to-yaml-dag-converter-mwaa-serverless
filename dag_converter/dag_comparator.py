from typing import Any

from airflow.models.dag import DAG

from .console_config import print_info, print_success, print_warning


def has_angle_bracket_notation(value: Any) -> bool:
    """Check if a value has angle bracket notation (e.g., <function>, <object>)"""
    str_value = str(value)
    return str_value.startswith("<") and str_value.endswith(">")


def compare_values(val_initial: Any, val_final: Any, attr_name: str) -> list[str]:
    """Compare two values, returning list of specific differences"""
    differences = []

    # If both have angle brackets, consider them equal
    # TODO: this was used since callables will always be slightly different due to unique id
    # However, logging the information can be helpful regardless
    if has_angle_bracket_notation(val_initial) and has_angle_bracket_notation(val_final):
        return []

    # Handle dictionaries
    if isinstance(val_initial, dict) and isinstance(val_final, dict):
        # Check for missing keys
        keys_initial = set(val_initial.keys())
        keys_final = set(val_final.keys())

        missing_in_final = keys_initial - keys_final
        missing_in_initial = keys_final - keys_initial

        if missing_in_final:
            differences.append(f"Keys missing in final dict: {missing_in_final}")
        if missing_in_initial:
            differences.append(f"Keys missing in initial dict: {missing_in_initial}")

        # Compare common keys
        common_keys = keys_initial & keys_final
        for key in common_keys:
            key_diffs = compare_values(val_initial[key], val_final[key], f"{attr_name}[{key}]")
            if key_diffs:
                differences.extend([f"Key '{key}': {diff}" for diff in key_diffs])

        return differences

    # Handle lists (order doesn't matter)
    if isinstance(val_initial, list) and isinstance(val_final, list):
        if len(val_initial) != len(val_final):
            return [f"Different lengths: {len(val_initial)} vs {len(val_final)}"]

        # Create sorted lists for comparison, handling angle bracket items
        def sort_key(item):
            if has_angle_bracket_notation(item):
                return "<object>"  # All angle bracket items sort together
            return str(item)

        sorted_initial = sorted(val_initial, key=sort_key)
        sorted_final = sorted(val_final, key=sort_key)

        for i, (item_initial, item_final) in enumerate(zip(sorted_initial, sorted_final, strict=False)):
            item_diffs = compare_values(item_initial, item_final, f"{attr_name}[{i}]")
            if item_diffs:
                differences.extend([f"Index {i}: {diff}" for diff in item_diffs])

        return differences

    # Handle sets (order doesn't matter)
    if isinstance(val_initial, set) and isinstance(val_final, set):
        if len(val_initial) != len(val_final):
            return [f"Different set sizes: {len(val_initial)} vs {len(val_final)}"]

        # Create sorted lists for comparison, handling angle bracket items
        def sort_key(item):
            if has_angle_bracket_notation(item):
                return "<object>"  # All angle bracket items sort together
            return str(item)

        sorted_initial = sorted(val_initial, key=sort_key)
        sorted_final = sorted(val_final, key=sort_key)

        for i, (item_initial, item_final) in enumerate(zip(sorted_initial, sorted_final, strict=False)):
            item_diffs = compare_values(item_initial, item_final, f"{attr_name}[{i}]")
            if item_diffs:
                differences.extend([f"Item {i}: {diff}" for diff in item_diffs])

        return differences

    # Handle tuples (order doesn't matter)
    if isinstance(val_initial, tuple) and isinstance(val_final, tuple):
        if len(val_initial) != len(val_final):
            return [f"Different lengths: {len(val_initial)} vs {len(val_final)}"]

        # Create sorted tuples for comparison, handling angle bracket items
        def sort_key(item):
            if has_angle_bracket_notation(item):
                return "<object>"  # All angle bracket items sort together
            return str(item)

        sorted_initial = sorted(val_initial, key=sort_key)
        sorted_final = sorted(val_final, key=sort_key)

        for i, (item_initial, item_final) in enumerate(zip(sorted_initial, sorted_final, strict=False)):
            item_diffs = compare_values(item_initial, item_final, f"{attr_name}[{i}]")
            if item_diffs:
                differences.extend([f"Index {i}: {diff}" for diff in item_diffs])

        return differences

    # For regular values, they must match exactly
    if val_initial != val_final:
        return [f"{val_initial} != {val_final}"]

    return []


def compare_task_attributes(task_initial: Any, task_final: Any, task_id: str) -> list[str]:
    """Compare attributes of two tasks"""
    differences = []

    # Get all attributes from both tasks
    attrs_initial = set(vars(task_initial).keys())
    attrs_final = set(vars(task_final).keys())

    # Check for missing attributes
    missing_in_final = attrs_initial - attrs_final
    missing_in_initial = attrs_final - attrs_initial

    if missing_in_final:
        differences.append(f"Task {task_id}: Attributes missing in final task: {missing_in_final}")
    if missing_in_initial:
        differences.append(f"Task {task_id}: Attributes missing in initial task: {missing_in_initial}")

    # Compare common attributes
    common_attrs = attrs_initial & attrs_final
    for attr in common_attrs:
        val_initial = getattr(task_initial, attr)
        val_final = getattr(task_final, attr)

        attr_diffs = compare_values(val_initial, val_final, attr)
        if attr_diffs:
            for diff in attr_diffs:
                differences.append(f"Task {task_id}.{attr}: {diff}")

    return differences


def compare_dags(dag_initial: DAG, dag_final: DAG) -> dict[str, list[str]]:
    """Compare two Dag objects and their tasks"""
    # Dictionary to store differences by task
    differences_by_task = {}

    # Get task dictionaries
    tasks_initial = {task.task_id: task for task in dag_initial.tasks}
    tasks_final = {task.task_id: task for task in dag_final.tasks}

    # Check for missing tasks
    task_ids_initial = set(tasks_initial.keys())
    task_ids_final = set(tasks_final.keys())

    missing_in_final = task_ids_initial - task_ids_final
    missing_in_initial = task_ids_final - task_ids_initial

    if missing_in_final:
        differences_by_task["DAG_STRUCTURE"] = [f"Tasks missing in final Dag: {missing_in_final}"]
    if missing_in_initial:
        if "DAG_STRUCTURE" not in differences_by_task:
            differences_by_task["DAG_STRUCTURE"] = []
        differences_by_task["DAG_STRUCTURE"].append(f"Tasks missing in initial Dag: {missing_in_initial}")

    # Compare common tasks
    common_tasks = task_ids_initial & task_ids_final
    # Go through tasks in original order
    for task_id in tasks_initial:
        if task_id not in common_tasks:
            continue
        task_diffs = compare_task_attributes(tasks_initial[task_id], tasks_final[task_id], task_id)

        # Group differences by task_id
        for diff in task_diffs:
            # Extract task_id from the difference string
            parts = diff.split(":", 1)
            task_identifier = parts[0].strip()

            # Extract just the task ID without attribute info
            if "." in task_identifier:
                # For attributes like "Task task_id.attribute"
                task_parts = task_identifier.split(".", 1)
                task_id_only = task_parts[0].replace("Task ", "")
                attribute = task_parts[1]
            else:
                # For general task differences like "Task task_id: message"
                task_id_only = task_identifier.replace("Task ", "")
                attribute = None

            if task_id_only not in differences_by_task:
                differences_by_task[task_id_only] = []

            # Store the difference without the task_id prefix
            if attribute:
                differences_by_task[task_id_only].append(f"{attribute}: {parts[1].strip()}")
            else:
                differences_by_task[task_id_only].append(parts[1].strip())

    # Print results in a grouped format
    total_diffs = sum(len(diffs) for diffs in differences_by_task.values())

    if differences_by_task:
        print_warning(f"\nFound {total_diffs} differences:")
        for task_id, diffs in differences_by_task.items():
            print_info(f"\n  Task: {task_id}")
            for diff in diffs:
                print_info(f"    - {diff}")
        print_info("")
    else:
        print_success("\n✅ DAGs are equivalent (ignoring object references)")

    return differences_by_task
