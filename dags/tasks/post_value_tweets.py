from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import tweepy
import os
import json


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


def post_value_tweets(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')

    # Get top value plays by edge from cleaned_odds (only positive edge)
    MIN_EDGE = 0.02  # 2% minimum edge to tweet
    
    plays = hook.get_records("""
        SELECT co.home_team, co.away_team, 
               co.outcome, co.exchange_key, 
               co.bookmaker_avg_prob, co.num_bookmakers,
               co.best_bookmaker, co.best_odds,
               co.exchange_prob_clean, co.exchange_price, co.edge,
               ro.raw_response
        FROM cleaned_odds co
        JOIN raw_odds ro ON co.raw_odds_id = ro.id AND co.batch_key = ro.batch_key
        WHERE co.batch_key = ? AND co.edge > ?
        ORDER BY co.edge DESC
    """, parameters=(batch_key, MIN_EDGE))
    
    if not plays:
        print(f"No value plays found for batch_key={batch_key}")
        return
    
    for i, play in enumerate(plays):
        home_team, away_team, outcome, exchange_key, bookmaker_avg_prob, num_books, best_book, best_odds, exchange_prob, exchange_price, edge, raw_json = play
        
        # Parse raw event to find exchange link
        event = json.loads(raw_json)
        exchange_link = None
        exchange_title = None
        
        for bm in event.get('bookmakers', []):
            if bm['key'] == exchange_key:
                exchange_link = bm.get('link')
                exchange_title = bm.get('title', exchange_key)
                break
        
        edge_pct = int(edge * 100)
        bookmaker_pct = int(bookmaker_avg_prob * 100)
        exchange_pct = int(exchange_prob * 100)
        bet_price_pct = int(exchange_price * 100)
        
        # Calculate EV for $100 bet on exchange
        shares = int(100 / exchange_price)
        actual_cost = shares * exchange_price
        payout_if_win = shares * 1.0
        win_prob = bookmaker_avg_prob  # Use bookmaker consensus as true probability
        
        expected_value = payout_if_win * win_prob
        ev = expected_value - actual_cost
        
        # Format bet description
        bet_description = f"Buy {shares} {outcome} @ {bet_price_pct}¢"
        outcome_description = f"{outcome} wins"
        
        # Data-driven format
        tweet = f"""🚨 Positive EV Alert

{home_team} vs {away_team}

Books avg: {bookmaker_pct}% ({num_books} books)
Best book: {best_book} {best_odds}
{exchange_title or exchange_key}: {outcome} @ {exchange_pct}%

Edge: +{edge_pct}%
${actual_cost:.0f} → ${ev:.2f} EV
{bet_description} → ${payout_if_win:.0f} if {outcome_description} ({int(win_prob*100)}%)"""
        
        if exchange_link:
            tweet += f"\n\n{exchange_link}"
        
        hook.run("""
            INSERT INTO tweets (tweet_text, batch_key)
            VALUES (?, ?)
        """, parameters=(tweet, batch_key))
        
        # Only post the first tweet
        if i == 0:
            post_tweet(tweet)
    
    print(f"Saved {len(plays)} value tweets, posted top 1")