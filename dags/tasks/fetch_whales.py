import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

LEADERBOARD_LIMIT = 50
TOTAL_WHALES = 200

def fetch_leaderboard(limit, offset=0):
    url = "https://data-api.polymarket.com/v1/leaderboard"
    params = {
        "timePeriod": "ALL",
        "limit": limit,
        "offset": offset
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_and_upsert_leaderboard():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    result = hook.get_first("SELECT MAX(updated_at) FROM whale_profiles")

    if result and result[0]:
        from datetime import datetime
        last_update = datetime.fromisoformat(result[0])
        hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
        if hours_since_update < 24:
            print(f"Whale profiles updated {hours_since_update:.1f} hours ago, skipping refresh")
            return

    all_whales = []
    for offset in range(0, TOTAL_WHALES, LEADERBOARD_LIMIT):
        whales = fetch_leaderboard(LEADERBOARD_LIMIT, offset)
        all_whales.extend(whales)
        print(f"Fetched {len(whales)} whales at offset {offset}")

    inserted = 0
    for whale in all_whales:
        wallet = whale.get('proxyWallet')
        if not wallet:
            print(f"Skipping whale missing proxyWallet")
            continue
        hook.run("""                                                                                                                                                                                                                                      
                   INSERT INTO whale_profiles                                                                                                                                                                                                         
                   (wallet_address, username, volume, pnl, updated_at)                                                                                                                                                                                  
                   VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                   ON CONFLICT (wallet_address) DO UPDATE SET
                       username = EXCLUDED.username,
                       volume = EXCLUDED.volume,
                       pnl = EXCLUDED.pnl,
                       updated_at = CURRENT_TIMESTAMP
               """,
            parameters=(
                wallet,
                whale.get('userName'),
                whale.get('vol'),
                whale.get('pnl')
            )
        )
        inserted += 1

    print(f"Upserted {inserted} whale profiles")

