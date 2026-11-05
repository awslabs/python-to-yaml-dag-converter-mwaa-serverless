from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


class GreetOperator(BaseOperator):
    template_fields = ("name",)

    def __init__(self, name: str, greeting: str = "Hello", **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.greeting = greeting

    def execute(self, context):
        print(f"{self.greeting}, {self.name}!")


def say_hello(who):
    print(f"hello {who}")


with DAG(dag_id="airflow_python_bash_custom", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False):
    py = PythonOperator(task_id="py", python_callable=say_hello, op_kwargs={"who": "world"})
    bash = BashOperator(task_id="bash", bash_command="echo hi", env={"A": "1"}, append_env=True)
    greet = GreetOperator(task_id="greet", name="team", greeting="Hi", retries=2)
    lam = LambdaInvokeFunctionOperator(task_id="lam", function_name="fn", payload='{"a": 1}')
    py >> bash >> greet >> lam
