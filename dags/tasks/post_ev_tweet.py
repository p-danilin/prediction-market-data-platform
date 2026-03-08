import os
import tweepy
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def post_ev_tweet(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    opportunities = hook.get_records("""
        SELECT home_team, away_team, outcome_name, 
               avg_bookmaker_prob, exchange_prob_implied, exchange_link,
               ev_edge, exchange_key
        FROM odds_analysis
        WHERE batch_key = ? AND ev_edge > 0.02
        ORDER BY ev_edge DESC
        LIMIT 1
    """, parameters=(batch_key,))
    
    if not opportunities:
        print("No +EV opportunities found")
        return
    
    opp = opportunities[0]
    print(f"Found opportunity: {opp}")
    
    home, away, outcome, book_prob, exch_prob, link, edge, exchange = opp
    
    book_pct = int(book_prob * 100)
    exch_pct = int(exch_prob * 100)
    edge_pct = int(edge * 100)
    ev_per_100 = round(edge * 100 / exch_prob, 1)
    
    tweet = f"""🚨 Positive EV Alert

{home} vs {away}

Sportsbooks avg: {book_pct}%
{exchange.title()}: {outcome} @ {exch_pct}%

Edge: +{edge_pct}%
$100 → ${ev_per_100} EV

{link or ''}"""
    
    client = tweepy.Client(
        consumer_key=os.environ['TWITTER_API_KEY'],
        consumer_secret=os.environ['TWITTER_API_SECRET'],
        access_token=os.environ['TWITTER_ACCESS_TOKEN'],
        access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    )
    
    print(f"Tweet length: {len(tweet)} characters")
    print(f"Tweet content:\n{tweet}")
    
    try:
        client.create_tweet(text=tweet)
        print(f"Posted +EV tweet for {outcome}")
    except Exception as e:
        print(f"Twitter API error: {e}")
        raise
