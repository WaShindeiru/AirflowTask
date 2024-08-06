from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from slack import WebClient
from slack.errors import SlackApiError

from jobs_dag import config

from smart_file_sensor import SmartFileSensor


def send_slack_message():
    connection = BaseHook.get_connection("slack_conn")
    slack_token = Variable.get(key="slack_token")
    print(slack_token)
    client = WebClient(token=slack_token)

    try:
        response = client.chat_postMessage(
        channel="C07E6FXDZ6D",
        text="Hello from your app! :tada:")
        print(response)
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        print(e.response["error"])

default_args = {
    "start_date": datetime(2024, 7, 19)
}

path = Variable.get("path_name", default_var="test.txt")

def get_date_difference(dt, ti):
    another_date = datetime.fromisoformat(ti.xcom_pull(key="trigger_logical_date_iso"))
    return another_date

def show_external_run_id():
    hook = PostgresHook(postgres_conn_id='postgres_test')
    query = hook.get_records(sql=f"select run_id from custom_run_id where timestamp = (select max(timestamp) from custom_run_id);")
    message = query[0][0]
    print(message)

with DAG(
    dag_id="trigger_dag",
    schedule_interval="@once",
    default_args=default_args
) as dag:
    fileSensor = FileSensor(
        fs_conn_id="sensor",
        task_id="sensor",
        filepath=path,
        poke_interval=30,
        timeout=6000
    )

    triggerOperator = TriggerDagRunOperator(
        task_id="Trigger_DAG",
        trigger_dag_id="dag_id_2_" + config["dag_id_2"]["table_name"]
    )

    @task_group(group_id="simple_task_group")
    def tg1():
        fileRemover = BashOperator(
            task_id="Remove_file",
            bash_command="rm /tmp/" + path
        )

        externalSensor = ExternalTaskSensor(
            task_id="jobs_sensor",
            external_dag_id="dag_id_2_" + config["dag_id_2"]["table_name"],
            external_task_id=None,
            execution_date_fn=get_date_difference
        )

        show_result = PythonOperator(
            task_id="result",
            python_callable=show_external_run_id,
            provide_context=True
        )

        timestamp = BashOperator(
            task_id="timestamp",
            bash_command="touch /tmp/finished_" + "{{ ts_nodash }}"
        )

        externalSensor >> show_result >> timestamp >> fileRemover

    slack_task = PythonOperator(
        task_id="alert_to_slack",
        python_callable=send_slack_message
    )
        
    fileSensor >> triggerOperator >> tg1() >> slack_task
