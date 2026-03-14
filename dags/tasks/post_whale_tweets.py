from airflow.providers.postgres.hooks.postgres import PostgresHook
from millify import millify
import tweepy
import os


def post_tweet(tweet):
    client = tweepy.Client(
        consumer_key=os.environ['TWITTER_API_KEY'],
        consumer_secret=os.environ['TWITTER_API_SECRET'],
        access_token=os.environ['TWITTER_ACCESS_TOKEN'],
        access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    )
    
    try:
        response = client.create_tweet(text=tweet)
        print(f"Posted tweet: {response.data['id']}")
        return response.data['id']
    except Exception as e:
        print(f"Error posting tweet: {e}")
        return None


def post_whale_tweets(batch_key):
    hook = PostgresHook(postgres_conn_id='postgres_default')

    # get trades by batch_key
    trades = hook.get_records("""
        SELECT rt.wallet_address, rt.market_title, 
               rt.outcome, rt.price, rt.size, 
               wp.username, wp.pnl, wp.volume
        FROM raw_trades rt 
        JOIN whale_profiles wp ON rt.wallet_address = wp.wallet_address 
        WHERE rt.batch_key = %s
        ORDER BY rt.size DESC
    """, parameters=(batch_key,))
    
    for i, trade in enumerate(trades):
        wallet, title, outcome, price, size, username, pnl, volume = trade
        
        confidence = int(price * 100)
        profile_url = f"https://polymarket.com/@{username}" if username else f"https://polymarket.com/profile/{wallet}"
        
        tweet = f"""🐋 ${millify(size, precision=0)} bet by {profile_url}

{outcome} @ {confidence}%
{title}

Trader stats: ${millify(pnl, precision=0)} profit, ${millify(volume, precision=0)} total volume"""
        
        hook.run("""
            INSERT INTO tweets (tweet_text, batch_key)
            VALUES (%s, %s)
        """, parameters=(tweet, batch_key))
        
        # Only post the first tweet
        if i == 0:
            post_tweet(tweet)
    
    print(f"Saved {len(trades)} tweets")
