from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

dag = DAG("dagfactory_test_dag", start_date=datetime(2023, 1, 1), schedule="@daily")
dag.is_dagfactory_auto_generated = True

task1 = BashOperator(task_id="task1", bash_command='echo "test"', dag=dag)
