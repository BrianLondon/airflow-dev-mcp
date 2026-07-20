"""Trivial DAG seeded into the AF2 test cluster.

`say_hello` prints a known marker line so the get_task_logs integration test can
assert on it across Airflow versions.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

MARKER = "HELLO_MARKER_LINE"


def _say_hello():
    print(MARKER)


def _second():
    print("second task done")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airflow-dev-mcp-test"],
) as dag:
    t1 = PythonOperator(task_id="say_hello", python_callable=_say_hello)
    t2 = PythonOperator(task_id="second", python_callable=_second)
    t1 >> t2
