from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule='0 */12 * * *',
    start_date=datetime(2026, 3, 10),
    tags=['outcomes']
)
def outcome_collector():
    @task
    def fetch_event_outcomes(**context):
        from tasks.fetch_event_outcomes import fetch_event_outcomes
        batch_key = context['ts_nodash']
        fetch_event_outcomes(batch_key)

    fetch_event_outcomes()

outcome_collector()
