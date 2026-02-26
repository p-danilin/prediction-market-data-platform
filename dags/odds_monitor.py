from airflow.sdk import dag, task
from datetime import datetime

@dag(
    # schedule='*/15 * * * *',
    schedule=None,
    start_date=datetime(2026, 2, 18),
    tags=['polymarket','odds']
)
def odds_monitor():
    @task
    def fetch_sportsbook_odds(**context):
        from tasks.fetch_sportsbook_odds import fetch_sportsbook_odds
        batch_key = context['ts_nodash']
        fetch_sportsbook_odds(batch_key)


    @task
    def flatten_sportsbook_odds(**context):
        from tasks.flatten_sportsbook_odds import flatten_sportsbook_odds
        batch_key = context['ts_nodash']
        flatten_sportsbook_odds(batch_key)

    @task
    def analyze_odds(**context):
        from tasks.analyze_odds import analyze_odds
        batch_key = context['ts_nodash']
        analyze_odds(batch_key)

    fetch_sportsbook_odds() >> flatten_sportsbook_odds() >> analyze_odds()

odds_monitor()