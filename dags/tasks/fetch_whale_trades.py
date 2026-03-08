import requests
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor


def fetch_wallet_addresses() -> list[str]:
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    records = hook.get_records("SELECT wallet_address FROM whale_profiles")
    return [r[0] for r in records]

def get_recent_activity(wallet_address, start_time, limit=500):
    url = "https://data-api.polymarket.com/activity"
    params = {
        "user": wallet_address,
        "limit": limit,
        "type": "TRADE",
        "start": start_time,
    }

    response = requests.get(url, params)
    response.raise_for_status()
    return response.json()

def fetch_whale_trades(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    cutoff_time = int((datetime.now() - timedelta(minutes=15)).timestamp())
    whales = fetch_wallet_addresses()
    
    def fetch_wallet_trades(wallet_address):
        rows = []
        try:
            transactions = get_recent_activity(wallet_address, cutoff_time)
            for transaction in transactions:
                if transaction.get('usdcSize', 0) > 50000:
                    rows.append((
                        transaction['transactionHash'],
                        transaction['proxyWallet'],
                        transaction['title'],
                        transaction['slug'],
                        transaction['side'],
                        transaction['outcome'],
                        transaction['price'],
                        transaction['usdcSize'],
                        transaction['timestamp'],
                        batch_key
                    ))
        except Exception as e:
            print(f"Error fetching trades for {wallet_address}: {e}")
        return rows
    
    all_rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_wallet_trades, whales)
        for rows in results:
            all_rows.extend(rows)
    
    if all_rows:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR IGNORE INTO raw_trades
            (transaction_hash, wallet_address, market_title, market_slug,
             side, outcome, price, size, timestamp, batch_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, all_rows)
        conn.commit()
        print(f"Inserted {len(all_rows)} trades with batch_key {batch_key}")
    else:
        print(f"No trades found for batch_key {batch_key}")
