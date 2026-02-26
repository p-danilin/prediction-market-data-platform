import requests
import os
import json
from datetime import datetime

from airflow.providers.sqlite.hooks.sqlite import SqliteHook

REGIONS = "us,us_ex"
SPORTS = ["basketball_nba"]

def get_sportsbook_odds():
    api_key = os.getenv('THE_ODDS_API_KEY')

    all_events = []
    for sport in SPORTS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
        params = {
            "apiKey": api_key,
            "regions": REGIONS,
            "markets": "h2h",
            "oddsFormat": "decimal",
            "includeLinks": "true"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        events = response.json()
        all_events.extend(events)

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