from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template


def create_sql_table(table_name, sql_file):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    tables_dir = Path(__file__).parent.parent.parent / 'tables'
    sql_path = tables_dir / sql_file
    sql = Template(sql_path.read_text()).render(table=table_name)
    hook.run(sql)