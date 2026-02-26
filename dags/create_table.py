from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 2, 18),
    tags=['table', 'sql'],
    params={
        'table_name': '',
        'sql_file': ''
    }
)
def create_table():
    @task
    def create_sql_table(params=None):
        from tasks.create_sql_table import create_sql_table
        create_sql_table(params['table_name'], params['sql_file'])
    
    create_sql_table()

create_table()