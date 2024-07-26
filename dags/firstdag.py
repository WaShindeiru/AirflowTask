from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id = "first_dag",
    default_args=default_args,
    description='first, simple dag',
    start_date=datetime(2024, 7, 18),
    schedule_interval="@once"
)

task1 = BashOperator(
    task_id='first_bash',
    bash_command="echo 'hello there'",
    dag=dag
)

dag2 = DAG(
    dag_id = "second_dag",
    default_args=default_args,
    description='second, simple dag',
    start_date=datetime(2024, 7, 18),
    schedule_interval="@once"
)