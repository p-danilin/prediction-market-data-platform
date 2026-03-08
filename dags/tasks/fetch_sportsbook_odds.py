import requests
import os
import json
from datetime import datetime

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

REGIONS = "us,us_ex"
SPORTS = ["icehockey_nhl", "basketball_nba",]

def _get_api_keys():
    api_keys = os.getenv('THE_ODDS_API_KEYS', '').split(',')
    return [k.strip() for k in api_keys if k.strip()]

def get_sportsbook_odds():
    api_keys = _get_api_keys()
    if not api_keys:
        raise ValueError("THE_ODDS_API_KEYS not set")
    
    all_events = []
    for sport in SPORTS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
        
        for api_key in api_keys:
            params = {
                "apiKey": api_key,
                "regions": REGIONS,
                "markets": "h2h",
                "oddsFormat": "decimal",
                "includeLinks": "true"
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 401:
                print(f"API key exhausted, trying next key...")
                continue
            
            response.raise_for_status()
            events = response.json()
            all_events.extend(events)
            break
        else:
            raise Exception(f"All API keys exhausted for {sport}")

    return all_events

def fetch_sportsbook_odds(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    events = get_sportsbook_odds()

    for event in events:
        hook.run(
            """
            INSERT OR REPLACE INTO raw_odds (id, batch_key, raw_response, loaded_at)
            VALUES (?, ?, ?, ?)
            """,
            parameters=(
                event['id'],
                batch_key,
                json.dumps(event),
                datetime.now().isoformat()
            )
        )
    
    print(f"Loaded {len(events)} events into raw_odds with batch_key={batch_key}")
