import os
import tweepy
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def post_arb_tweet(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    opportunities = hook.get_records("""
        SELECT home_team, away_team, outcome_name, exchange_key, exchange_price, exchange_link,
               best_opposite_outcome, best_opposite_source_title, best_opposite_price, 
               best_opposite_link, arb_profit_pct
        FROM odds_analysis
        WHERE batch_key = ? AND arb_profit_pct > 0
        ORDER BY arb_profit_pct DESC
        LIMIT 1
    """, parameters=(batch_key,))
    
    if not opportunities:
        print("No arbitrage opportunities found")
        return
    
    opp = opportunities[0]
    home, away, outcome, exch, exch_price, exch_link, opp_outcome, opp_source, opp_price, opp_link, arb = opp
    
    arb_pct = round(arb * 100, 2)
    
    # Calculate bet sizing for $100 total stake
    total_stake = 100
    stake1 = round(total_stake / (1 + exch_price / opp_price), 2)
    stake2 = round(total_stake - stake1, 2)
    profit = round(arb * total_stake, 2)
    
    tweet = f"""💰 Arbitrage Alert

{home} vs {away}

{exch.title()}: {outcome} @ {exch_price} → Bet ${stake1}
{opp_source}: {opp_outcome} @ {opp_price} → Bet ${stake2}

Guaranteed profit: {arb_pct}%
Total stake: ${total_stake} → ${profit} profit

{exch_link or ''}
{opp_link or ''}"""
    
    client = tweepy.Client(
        consumer_key=os.environ['TWITTER_API_KEY'],
        consumer_secret=os.environ['TWITTER_API_SECRET'],
        access_token=os.environ['TWITTER_ACCESS_TOKEN'],
        access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    )
    
    client.create_tweet(text=tweet)
    print(f"Posted arbitrage tweet: {arb_pct}% profit")
