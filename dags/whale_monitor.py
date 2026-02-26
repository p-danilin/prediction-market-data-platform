from airflow.sdk import dag, task
from datetime import datetime



@dag(
    # schedule='*/15 * * * *',
    schedule=None,
    start_date=datetime(2026, 2, 18),
    tags=['polymarket']
)
def whale_monitor():
    @task
    def fetch_whales():
        from tasks.fetch_whales import fetch_and_upsert_leaderboard
        fetch_and_upsert_leaderboard()

    @task
    def fetch_whale_trades(**context):
        from tasks.fetch_whale_trades import fetch_whale_trades
        batch_key = context['ts_nodash']
        fetch_whale_trades(batch_key)

    @task
    def post_whale_tweets(**context):
        from tasks.post_whale_tweets import post_whale_tweets
        batch_key = context['ts_nodash']
        post_whale_tweets(batch_key)

    fetch_whales() >> fetch_whale_trades() >> post_whale_tweets()

whale_monitor()