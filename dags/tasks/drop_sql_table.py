from airflow.providers.postgres.hooks.postgres import PostgresHook


def drop_sql_table(table_name):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropped table: {table_name}")
