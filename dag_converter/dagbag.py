from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag


def get_dag_object(dag_file_path: Path) -> list[DAG]:
    """Load the Dags using DagBag and retrieve the Dag objects"""
    dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    found_dags = []
    for dag in dagbag.dags.values():
        found_dags.append(dag)

    if found_dags:
        return found_dags

    raise Exception(f"Failed to generate dag object for file {dag_file_path}")


def get_dag_factory_object(dag_file_path: Path) -> DAG:
    """Retrieve the Dag Object created by DagFactory during the validation step"""
    dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    for dag in dagbag.dags.values():
        # DagBag may contain the original Dag generated from the Python file,
        # only retrieve the DagFactory-generated Dag
        if getattr(dag, "is_dagfactory_auto_generated", None):
            return dag

    raise Exception(f"Failed to generate dag object for file {dag_file_path}")
