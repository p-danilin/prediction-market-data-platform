import requests
import os
import json
from datetime import datetime

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

def fetch_event_outcomes(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    # Get unique sports from past events
    sports = hook.get_pandas_df("""
        SELECT DISTINCT sport_key 
        FROM odds_snapshots 
        WHERE commence_time < datetime('now')
    """)['sport_key'].tolist()
    
    if not sports:
        print("No completed events found")
        return
    
    api_keys = os.getenv('THE_ODDS_API_KEYS', '').split(',')
    api_keys = [k.strip() for k in api_keys if k.strip()]
    
    if not api_keys:
        raise ValueError("THE_ODDS_API_KEYS not set")
    
    all_results = []
    for sport in sports:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/scores/"
        
        for api_key in api_keys:
            params = {
                "apiKey": api_key,
                "daysFrom": 3
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 401:
                print(f"API key exhausted, trying next key...")
                continue
            
            response.raise_for_status()
            results = response.json()
            all_results.extend(results)
            print(f"Fetched {len(results)} results for {sport}")
            break
        else:
            print(f"All API keys exhausted for {sport}")
    
    # Store completed games
    stored = 0
    for game in all_results:
        if not game.get('completed'):
            continue
        
        scores = game.get('scores', [])
        if len(scores) != 2:
            continue
        
        winner = scores[0]['name'] if scores[0]['score'] > scores[1]['score'] else scores[1]['name']
        home_score = next((s['score'] for s in scores if s['name'] == game['home_team']), None)
        away_score = next((s['score'] for s in scores if s['name'] == game['away_team']), None)
        
        hook.run(
            """
            INSERT OR REPLACE INTO event_outcomes 
            (event_id, sport_key, home_team, away_team, commence_time, winner, home_score, away_score, completed, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            parameters=(
                game['id'],
                game['sport_key'],
                game['home_team'],
                game['away_team'],
                game['commence_time'],
                winner,
                home_score,
                away_score,
                True,
                datetime.now().isoformat()
            )
        )
        stored += 1
    
    print(f"Stored {stored} completed event outcomes with batch_key={batch_key}")
