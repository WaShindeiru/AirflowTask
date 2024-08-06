from datetime import datetime

import uuid

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from postgre_count_rows import PostgreSQLCountRows

config = {
    'dag_id_1': 
        {'schedule_interval': "@once", 
        "start_date": datetime(2024, 7, 19),
        "table_name": "funny_table"},
    'dag_id_2':
        {'schedule_interval': "@once", 
        "start_date": datetime(2024, 7, 19),
        "table_name": "timestamp_table"},
    'dag_id_3':
        {'schedule_interval': "@once", 
        "start_date": datetime(2024, 7, 19),
        "table_name": "small_table"}
}

def process_table(dag_id: str, database: str, ti, execution_date):
    print(f"{dag_id} start processing tables in database: {database}")

def send_run_id(ti, **context):
    hook = PostgresHook(postgres_conn_id='postgres_test')
    execution_date_ = context['execution_date']
    run_id_ = context['run_id']
    query = hook.get_records(sql=f"insert into custom_run_id values ('{execution_date_}', '{run_id_} ended')")

    ti.xcom_push(key="run_id", value=f"{run_id_} ended")

def check_table(dag_id):
    hook = PostgresHook(postgres_conn_id="postgres_test")
    query = hook.get_records(sql=f"SELECT * FROM information_schema.tables WHERE table_name = '{config[dag_id]["table_name"]}';")
    print(f"queryResult: {query}")
    print(f"query result type: {type(query)}")
    if query:
        return "insert_row"
    else:
        return "create_table"

for i in config.keys():
    with DAG(
        dag_id = i + "_" + config[i]["table_name"],
        schedule_interval = config[i]['schedule_interval'],
        default_args = {key: value for key, value in config[i].items() if key != "table_name"}
    ) as dag:
        task1 = PythonOperator(
            task_id=config[i]["table_name"],
            python_callable=process_table,
            op_kwargs={"dag_id":i, "database":config[i]["table_name"]}
        )

        insert_row = SQLExecuteQueryOperator(
            task_id="insert_row",
            trigger_rule="none_failed",
            conn_id="postgres_test",
            sql=f"INSERT INTO {config[i]["table_name"]} VALUES(" + str(uuid.uuid4().int % 123456789) + ", '{{ ti.xcom_pull(task_ids='get_current_user', key='return_value') }}', '" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "');"
        )

        query_table = PostgreSQLCountRows(
            task_id="query_table",
            trigger_rule="none_failed",
            conn_id="postgres_test",
            table_name=config[i]["table_name"]
        )

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="postgres_test",
            sql=f"CREATE TABLE {config[i]["table_name"]}(custom_id integer NOT NULL, user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);"
        )

        check_table_exists = BranchPythonOperator(
            task_id = "check_table_exists",
            python_callable = check_table,
            op_kwargs={"dag_id":i}
        )

        get_user = BashOperator(
            task_id="get_current_user",
            bash_command="whoami"
        )

        run_id = PythonOperator(
            task_id="run_id",
            python_callable=send_run_id
        )

        task1 >> get_user >> check_table_exists >> create_table >> insert_row >> query_table >> run_id
