from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def drop_sql_table(table_name):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    hook.run(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropped table: {table_name}")
