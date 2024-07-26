from airflow.models.baseoperator import BaseOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, conn_id: str, table_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        query = hook.get_records(f"select count(*) from {self.table_name};")
        print(query)