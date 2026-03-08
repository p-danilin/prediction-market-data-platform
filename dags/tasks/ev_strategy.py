from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from .utils.trade_executor import TradeExecutor
from datetime import datetime, timezone

MIN_EV_EDGE = 0.02
MAX_BETS_PER_OUTCOME = 1
MIN_BET_SIZE = 5.0
MIN_LIMIT_PRICE = 0.2
MAX_LIMIT_PRICE = 0.80
MIN_BOOKMAKERS = 3
EXCHANGE_KEY = 'polymarket'


def ev_strategy(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    opportunities = _fetch_opportunities(hook, batch_key)
    if not opportunities:
        print(f"No opportunities found for batch_key={batch_key}")
        return

    filtered_opps = _filter_by_bet_limits(hook, opportunities)
    if not filtered_opps:
        print(f"All outcomes at max bet limit ({MAX_BETS_PER_OUTCOME}) for batch_key={batch_key}")
        return
    
    print(f"Found {len(filtered_opps)} opportunities to trade for batch_key={batch_key}")
    _execute_trades(hook, batch_key, filtered_opps)


def _fetch_opportunities(hook, batch_key):
    return hook.get_records("""
        SELECT event_id, outcome_name, exchange_key, exchange_price, ev_edge, 
               sport_key, home_team, away_team, commence_time, avg_bookmaker_prob, exchange_link
        FROM odds_analysis
        WHERE batch_key = ? AND exchange_key = ? AND ev_edge > ? AND num_bookmakers >= ?
        ORDER BY ev_edge DESC
    """, parameters=(batch_key, EXCHANGE_KEY, MIN_EV_EDGE, MIN_BOOKMAKERS))


def _filter_by_bet_limits(hook, opportunities):
    # limit bets on one outcome to reduce risk concentration
    filtered = []
    now = datetime.now(timezone.utc)
    
    for opp in opportunities:
        event_id, outcome_name, _, _, _, _, _, _, commence_time = opp[:9]
        
        # Skip if event has already started
        event_start = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
        if now >= event_start:
            print(f"✗ Skipping {outcome_name} - event already started")
            continue
        
        outcome_bet_count = hook.get_first("""
            SELECT COUNT(*) FROM trade_executions
            WHERE event_id = ? AND outcome_name = ? AND status = 'success'
        """, parameters=(event_id, outcome_name))[0]
        
        if outcome_bet_count < MAX_BETS_PER_OUTCOME:
            filtered.append(opp)
        else:
            print(f"✗ Skipping {outcome_name} - already bet {outcome_bet_count} time(s)")
    
    return filtered


def _execute_trades(hook, batch_key, opportunities):
    executor = TradeExecutor()
    conn = hook.get_conn()
    
    for opp in opportunities:
        (event_id, outcome_name, exchange_key, exchange_price, ev_edge, 
         sport_key, home_team, away_team, commence_time, avg_bookmaker_prob, exchange_link) = opp
        
        limit_price = max(0.01, avg_bookmaker_prob - MIN_EV_EDGE)
        
        if not _is_valid_price(limit_price, outcome_name):
            continue
        
        opportunity_with_price = opp + (limit_price,)
        executor.execute_opportunity(conn, batch_key, opportunity_with_price, MIN_BET_SIZE)
    
    conn.commit()


def _is_valid_price(price, outcome_name):
    if MIN_LIMIT_PRICE <= price <= MAX_LIMIT_PRICE:
        return True
    print(f"✗ Skipping {outcome_name} - price {price:.2f} outside {MIN_LIMIT_PRICE}-{MAX_LIMIT_PRICE} range")
    return False
