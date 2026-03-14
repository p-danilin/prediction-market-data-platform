import json
import psycopg2.extras
from airflow.providers.postgres.hooks.postgres import PostgresHook


def flatten_sportsbook_odds(batch_key):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("DELETE FROM odds_snapshots WHERE batch_key = %s", parameters=(batch_key,))
    raw_records = hook.get_records(
        "SELECT id, raw_response FROM raw_odds WHERE batch_key = %s",
        parameters=(batch_key,)
    )
    
    rows = []
    for event_id, raw_json in raw_records:
        event = json.loads(raw_json)
        
        for bookmaker in event['bookmakers']:
            for outcome in bookmaker['markets'][0]['outcomes']:
                rows.append((
                    batch_key,
                    event_id,
                    event['sport_key'],
                    event['home_team'],
                    event['away_team'],
                    event['commence_time'],
                    bookmaker['key'],
                    bookmaker['title'],
                    outcome['name'],
                    outcome['price'],
                    bookmaker.get('link'),
                    outcome.get('link'),
                    bookmaker.get('last_update')
                ))
    
    if rows:
        conn = hook.get_conn()
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO odds_snapshots (
                    batch_key, event_id, sport_key, home_team, away_team,
                    commence_time, bookmaker_key, bookmaker_title, outcome_name,
                    price, bookmaker_link, outcome_link, last_update
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)
        conn.commit()
    
    print(f"Flattened {len(rows)} odds snapshots for batch_key={batch_key}")
