from pathlib import Path

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from jinja2 import Template


def create_sql_table(table_name, sql_file):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    tables_dir = Path(__file__).parent.parent.parent / 'tables'
    sql_path = tables_dir / sql_file
    sql = Template(sql_path.read_text()).render(table=table_name)
    hook.run(sql)