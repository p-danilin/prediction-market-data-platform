from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule='*/2 * * * *',
    # schedule=None,
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

    @task
    def ev_strategy(**context):
        from tasks.ev_strategy import ev_strategy
        batch_key = context['ts_nodash']
        ev_strategy(batch_key)

    # @task
    # def post_ev_tweet(**context):
    #     from tasks.post_ev_tweet import post_ev_tweet
    #     batch_key = context['ts_nodash']
    #     post_ev_tweet(batch_key)
    #
    # @task
    # def post_arb_tweet(**context):
    #     from tasks.post_arb_tweet import post_arb_tweet
    #     batch_key = context['ts_nodash']
    #     post_arb_tweet(batch_key)

    # fetch_sportsbook_odds() >> flatten_sportsbook_odds() >> analyze_odds() >> [execute_trades(), post_ev_tweet(), post_arb_tweet()]
    fetch_sportsbook_odds() >> flatten_sportsbook_odds() >> analyze_odds() >> ev_strategy()


odds_monitor()