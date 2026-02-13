import sys
sys.path.insert(0, '/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from tasks.polymarket.fetch import fetch_market_data
from tasks.polymarket.detect import calculate_price_change, should_alert


def fetch_and_store(**context):
    """Fetch all active markets from event and store in SQLite."""
    event_slug = context['params']['event_slug']
    
    # Fetch all markets from the event
    from tasks.polymarket.fetch import fetch_event_markets
    markets = fetch_event_markets(event_slug)
    
    # Filter for active, not closed markets
    active_markets = [m for m in markets if m.get('active') and not m.get('closed')]
    
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    # Create table if not exists
    hook.run("""
        CREATE TABLE IF NOT EXISTS polymarket_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            market_slug TEXT,
            question TEXT,
            outcome_prices TEXT,
            volume REAL
        )
    """)
    
    # Store current data for each active market
    for market in active_markets:
        hook.run(
            "INSERT INTO polymarket_prices (timestamp, market_slug, question, outcome_prices, volume) VALUES (?, ?, ?, ?, ?)",
            parameters=(
                datetime.now().isoformat(),
                market.get('slug'),
                market.get('question', ''),
                str(market.get('outcomePrices', [])),
                market.get('volume', 0)
            )
        )
    
    print(f"Stored data for {len(active_markets)} active markets")
    return active_markets


def check_movement(**context):
    """Check for price movements across all markets and log alerts."""
    threshold = context['params'].get('threshold', 1.0)
    
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    # Create alerts table if not exists
    hook.run("""
        CREATE TABLE IF NOT EXISTS polymarket_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            market_slug TEXT,
            question TEXT,
            price_change_pct REAL,
            previous_prices TEXT,
            current_prices TEXT
        )
    """)
    
    # Get all unique markets
    markets = hook.get_records("SELECT DISTINCT market_slug FROM polymarket_prices")
    
    for (market_slug,) in markets:
        # Get last two records for this market
        records = hook.get_records(
            "SELECT outcome_prices, question FROM polymarket_prices WHERE market_slug = ? ORDER BY timestamp DESC LIMIT 2",
            parameters=(market_slug,)
        )
        
        if len(records) < 2:
            continue
        
        current_prices = eval(records[0][0])
        previous_prices = eval(records[1][0])
        question = records[0][1]
        
        change = calculate_price_change(current_prices, previous_prices)
        
        if should_alert(change, threshold):
            # Log to console
            print(f"ALERT: {question} moved {change:.2f}%")
            print(f"Previous: {previous_prices}")
            print(f"Current: {current_prices}")
            
            # Store in alerts table
            hook.run(
                "INSERT INTO polymarket_alerts (timestamp, market_slug, question, price_change_pct, previous_prices, current_prices) VALUES (?, ?, ?, ?, ?, ?)",
                parameters=(
                    datetime.now().isoformat(),
                    market_slug,
                    question,
                    change,
                    str(previous_prices),
                    str(current_prices)
                )
            )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'polymarket_monitor',
    default_args=default_args,
    description='Monitor Polymarket event for price movements',
    schedule=timedelta(minutes=5),
    catchup=False,
    params={
        'event_slug': 'us-strikes-iran-by',
        'threshold': 1.0
    }
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_and_store',
        python_callable=fetch_and_store,
    )
    
    detect_task = PythonOperator(
        task_id='check_movement',
        python_callable=check_movement,
    )
    
    fetch_task >> detect_task
