import copy
import functools
import inspect
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.sdk import BaseOperator
from airflow.sdk.definitions._internal.types import NOTSET


class NoDagFoundError(Exception):
    """The file imports without errors but does not define a Dag"""


# Attribute holding the keyword arguments the Dag file passed to the operator class
USER_KWARGS_ATTRIBUTE = "_dag_converter_user_kwargs"
# Attribute holding the public attributes of the operator right after __init__
INIT_ATTRIBUTES_ATTRIBUTE = "_dag_converter_init_attributes"
# Attribute holding a copy of _BaseOperator__init_kwargs right after __init__. Airflow updates the original
# when attributes are assigned later
INIT_KWARGS_ATTRIBUTE = "_dag_converter_init_kwargs"
# Attribute holding the names of attributes assigned from outside the operator's own methods after __init__
ASSIGNED_NAMES_ATTRIBUTE = "_dag_converter_assigned_names"
# Attribute holding whether the operator was created inside a Dag, so Airflow applied the Dag default_args
CREATED_IN_DAG_ATTRIBUTE = "_dag_converter_created_in_dag"

# Attributes Airflow changes after __init__ when tasks are connected or added to a Dag
AIRFLOW_MANAGED_ATTRIBUTES = {"upstream_task_ids", "downstream_task_ids", "task_group", "dag", "start_date", "end_date"}


def _get_init_parameters(operator_class: type) -> Iterator[inspect.Parameter]:
    """Parameters of every __init__ in the class MRO, base classes first"""
    for cls in reversed(operator_class.__mro__):
        try:
            yield from inspect.signature(cls.__init__).parameters.values()
        except (TypeError, ValueError):
            continue


@functools.cache
def get_constructor_parameter_names(operator_class: type) -> frozenset[str]:
    """Names of the named parameters of every __init__ in the class MRO"""
    names = {p.name for p in _get_init_parameters(operator_class) if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)}
    names.discard("self")
    return frozenset(names)


def get_operator_defaults(operator_class: type) -> dict[str, Any]:
    """Returns the constructor default of every parameter of 'operator_class', following the class MRO"""
    return {p.name: p.default for p in _get_init_parameters(operator_class) if p.default is not p.empty}


def values_differ(a: Any, b: Any) -> bool:
    try:
        return bool(a != b)
    except Exception:
        return a is not b


def get_assigned_attributes(operator: Any) -> dict[str, tuple[Any, Any]]:
    """
    Attributes the Dag file assigned after creating the operator, e.g.
    'delete_bucket.trigger_rule = TriggerRule.ALL_DONE', and constructor arguments changed in place.
    Maps each name to (value after __init__, current value). Attributes __init__ did not create have no value
    after __init__ (ArgNotSet)
    """
    init_attributes = vars(operator).get(INIT_ATTRIBUTES_ATTRIBUTE)
    if init_attributes is None:
        return {}
    names = set(vars(operator).get(ASSIGNED_NAMES_ATTRIBUTE, ()))
    # Containers passed as arguments can also be changed in place, e.g. 'task.env["KEY"] = "value"'
    parameters = get_constructor_parameter_names(type(operator))
    names.update(
        name
        for name, init_value in init_attributes.items()
        if name in parameters and isinstance(init_value, list | dict | set)
    )
    assigned = {}
    for name in sorted(names - AIRFLOW_MANAGED_ATTRIBUTES):
        init_value = init_attributes.get(name, NOTSET)
        value = getattr(operator, name, None)
        if values_differ(value, init_value):
            assigned[name] = (init_value, value)
    return assigned


def get_init_kwargs(operator: Any) -> dict[str, Any]:
    """The arguments __init__ received, including default_args and arguments subclasses passed to base classes"""
    return vars(operator).get(INIT_KWARGS_ATTRIBUTE) or getattr(operator, "_BaseOperator__init_kwargs", {})


def was_created_in_dag(operator: Any) -> bool:
    """Whether Airflow applied the Dag default_args when creating the operator"""
    return vars(operator).get(CREATED_IN_DAG_ATTRIBUTE, True)


def _snapshot(value: Any) -> Any:
    """Shallow copy of containers, so in-place changes after __init__ are detected"""
    if isinstance(value, list | dict | set):
        return copy.copy(value)
    return value


@contextmanager
def record_operator_arguments() -> Iterator[None]:
    """
    Record the keyword arguments of every operator instantiation on the operator.

    BaseOperator._BaseOperator__init_kwargs also contains the arguments subclasses pass to their base classes
    (e.g. a BashOperator subclass that builds bash_command), which must not be written to the YAML: DagFactory
    calls the subclass again, and it would get them twice.
    """
    metaclass = type(BaseOperator)
    original_call = metaclass.__dict__.get("__call__")
    parent_call = metaclass.__call__
    original_setattr = BaseOperator.__setattr__

    def recording_setattr(self, key, value):
        original_setattr(self, key, value)
        if key.startswith("_") or INIT_ATTRIBUTES_ATTRIBUTE not in vars(self):
            return
        # Assignments in the operator's own methods (including __setattr__ overrides) are not the Dag file's
        frame = sys._getframe(1)
        while frame is not None and frame.f_code.co_name == "__setattr__":
            frame = frame.f_back
        if frame is not None and frame.f_locals.get("self") is self:
            return
        vars(self).setdefault(ASSIGNED_NAMES_ATTRIBUTE, set()).add(key)

    def recording_call(cls, *args, **kwargs):
        operator = parent_call(cls, *args, **kwargs)
        object.__setattr__(operator, USER_KWARGS_ATTRIBUTE, dict(kwargs))
        init_attributes = {
            name: _snapshot(value)
            for name, value in vars(operator).items()
            if not name.startswith("_") and name not in AIRFLOW_MANAGED_ATTRIBUTES
        }
        object.__setattr__(operator, INIT_ATTRIBUTES_ATTRIBUTE, init_attributes)
        init_kwargs = getattr(operator, "_BaseOperator__init_kwargs", {})
        object.__setattr__(operator, INIT_KWARGS_ATTRIBUTE, {k: _snapshot(v) for k, v in init_kwargs.items()})
        has_dag = getattr(operator, "has_dag", None)
        object.__setattr__(operator, CREATED_IN_DAG_ATTRIBUTE, bool(has_dag()) if has_dag else True)
        return operator

    metaclass.__call__ = recording_call
    BaseOperator.__setattr__ = recording_setattr
    try:
        yield
    finally:
        BaseOperator.__setattr__ = original_setattr
        if original_call is None:
            del metaclass.__call__
        else:
            metaclass.__call__ = original_call


def get_dag_object(dag_file_path: Path) -> list[DAG]:
    """Load the Dags using DagBag and retrieve the Dag objects"""
    dag_file_path = dag_file_path.resolve()

    # Add the DAG file's directory to sys.path so sibling modules can be imported since
    # for python there can be more than one module
    dag_dir = str(dag_file_path.parent)
    if dag_dir not in sys.path:
        sys.path.insert(0, dag_dir)

    with record_operator_arguments():
        dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    found_dags = list(dagbag.dags.values())

    if found_dags:
        return found_dags

    if dagbag.import_errors:
        details = "\n".join(f"{path}:\n{err}" for path, err in dagbag.import_errors.items())
        raise Exception(
            f"Failed to generate dag object for file {dag_file_path}. DagBag import errors:\n{details}"
        )

    raise NoDagFoundError(f"Failed to generate dag object for file {dag_file_path}. It does not define a Dag")


def get_dag_factory_object(dag_file_path: Path) -> DAG:
    """Retrieve the Dag Object created by DagFactory during the validation step"""
    dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    for dag in dagbag.dags.values():
        # DagBag may contain the original Dag generated from the Python file,
        # only retrieve the DagFactory-generated Dag
        if getattr(dag, "is_dagfactory_auto_generated", None):
            return dag

    if dagbag.import_errors:
        details = "\n".join(f"{path}:\n{err}" for path, err in dagbag.import_errors.items())
        raise Exception(
            f"Failed to generate dag object for file {dag_file_path}. DagBag import errors:\n{details}"
        )

    raise Exception(
        f"Failed to generate dag object for file {dag_file_path}. "
        f"DagBag loaded {len(dagbag.dags)} DAG(s) but none were marked is_dagfactory_auto_generated."
    )
