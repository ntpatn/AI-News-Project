# project-a/dags/proj_a__example.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello(conf=None):
    print("Hello from A!", conf)


with DAG(
    dag_id="proj_a__example",  # << ชื่อ DAG ที่จะไป trigger
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual/trigger เท่านั้น
    catchup=False,
    tags=["proj_a"],
):
    PythonOperator(
        task_id="say_hello",
        python_callable=hello,
        op_kwargs={"conf": "{{ dag_run.conf }}"},
    )
