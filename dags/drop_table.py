from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 2, 18),
    tags=['table', 'sql'],
    params={
        'table_name': ''
    }
)
def drop_table():
    @task
    def drop_sql_table(params=None):
        from tasks.drop_sql_table import drop_sql_table
        drop_sql_table(params['table_name'])
    
    drop_sql_table()

drop_table()
