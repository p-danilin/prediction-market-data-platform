from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from typing import Dict, List
from collections import defaultdict
import json

# Only analyze liquid exchanges and bookmakers for accuracy
TARGET_EXCHANGES = ['polymarket', 'kalshi']
INCLUDE_BOOKMAKERS = ['betmgm', 'betonlineag', 'betrivers', 'draftkings', 'fanduel']


def analyze_odds(batch_key):
    hook = SqliteHook(sqlite_conn_id='sqlite_default')
    
    # delete existing in case of rerun with updated logic
    hook.run("DELETE FROM odds_analysis WHERE batch_key = ?", parameters=(batch_key,))
    
    bookmaker_placeholders = ','.join(['?'] * (len(TARGET_EXCHANGES) + len(INCLUDE_BOOKMAKERS)))
    allowed_bookmakers = TARGET_EXCHANGES + INCLUDE_BOOKMAKERS
    
    snapshots = hook.get_records(f"""
        SELECT event_id, sport_key, home_team, away_team, commence_time,
               bookmaker_key, outcome_name, price, bookmaker_title,
               bookmaker_link, outcome_link
        FROM odds_snapshots
        WHERE batch_key = ?
          AND bookmaker_key IN ({bookmaker_placeholders})
    """, parameters=(batch_key, *allowed_bookmakers))
    
    events = group_by_event(snapshots)
    
    # Analyze each event for each exchange (e.g., Polymarket vs books, Kalshi vs books)
    analysis_rows = []
    for event_id, event_data in events.items():
        for exchange in TARGET_EXCHANGES:
            rows = analyze_event(batch_key, event_id, event_data, exchange)
            analysis_rows.extend(rows)
    
    if analysis_rows:
        conn = hook.get_conn()
        conn.executemany("""
            INSERT INTO odds_analysis (
                batch_key, event_id, outcome_name, sport_key, home_team, away_team,
                commence_time, exchange_key, exchange_price, exchange_prob_implied,
                exchange_prob_clean, exchange_link, num_bookmakers, bookmaker_probs,
                avg_bookmaker_prob, std_bookmaker_prob, ev_edge, best_opposite_outcome,
                best_opposite_source_key, best_opposite_source_title,
                best_opposite_price, best_opposite_prob_clean, best_opposite_link,
                arb_profit_pct, arb_total_implied_prob
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, analysis_rows)
        conn.commit()
    
    print(f"Analyzed {len(analysis_rows)} outcomes for batch_key={batch_key}")


def group_by_event(snapshots) -> Dict:
    events = defaultdict(lambda: {'meta': None, 'odds': []})
    
    for row in snapshots:
        event_id = row[0]
        if events[event_id]['meta'] is None:
            events[event_id]['meta'] = {
                'sport_key': row[1],
                'home_team': row[2],
                'away_team': row[3],
                'commence_time': row[4]
            }
        
        events[event_id]['odds'].append({
            'bookmaker_key': row[5],
            'outcome_name': row[6],
            'price': row[7],
            'bookmaker_title': row[8],
            'bookmaker_link': row[9],
            'outcome_link': row[10]
        })
    
    return events


def analyze_event(batch_key, event_id, event_data, target_exchange) -> List[tuple]:
    """
    Analyze one event for EV and arb opportunities.
    
    For each exchange outcome (e.g., betting on Team A at Polymarket):
    - EV: Compare exchange price vs bookmaker consensus (is the exchange mispriced?)
    - Arb: Find best opposite price to lock in guaranteed profit
    """
    meta = event_data['meta']
    odds = event_data['odds']
    
    exchange_odds = [o for o in odds if o['bookmaker_key'] == target_exchange]
    bookmaker_odds = [o for o in odds if o['bookmaker_key'] != target_exchange and o['bookmaker_key'] in INCLUDE_BOOKMAKERS]
    
    if not exchange_odds or not bookmaker_odds:
        return []
    
    exchange_by_outcome = {o['outcome_name']: o for o in exchange_odds}
    bookmakers_by_outcome = defaultdict(list)
    for o in bookmaker_odds:
        bookmakers_by_outcome[o['outcome_name']].append(o)
    
    # Remove vig from exchange for reference (still use raw implied for EV calculation)
    exchange_probs_clean = remove_vig([o['price'] for o in exchange_odds])
    
    rows = []
    for i, (outcome_name, exchange_odd) in enumerate(exchange_by_outcome.items()):
        bookmakers = bookmakers_by_outcome.get(outcome_name, [])
        if not bookmakers:
            continue
        
        # remove vig from each bookmaker to get "true" probabilities
        bookmaker_probs_clean = []
        for bookmaker in bookmakers:
            bookmaker_all_outcomes = [o for o in bookmaker_odds if o['bookmaker_key'] == bookmaker['bookmaker_key']]
            probs_clean = remove_vig([o['price'] for o in bookmaker_all_outcomes])
            outcome_idx = next(i for i, o in enumerate(bookmaker_all_outcomes) if o['outcome_name'] == outcome_name)
            bookmaker_probs_clean.append(probs_clean[outcome_idx])
        
        bookmaker_data = []
        for bookmaker, prob_clean in zip(bookmakers, bookmaker_probs_clean):
            bookmaker_data.append({
                'key': bookmaker['bookmaker_key'],
                'title': bookmaker['bookmaker_title'],
                'price': bookmaker['price'],
                'prob_clean': prob_clean
            })
        
        avg_bookmaker_prob = sum(bookmaker_probs_clean) / len(bookmaker_probs_clean)
        variance = sum((p - avg_bookmaker_prob) ** 2 for p in bookmaker_probs_clean) / len(bookmaker_probs_clean)
        std_bookmaker_prob = variance ** 0.5
        
        # EV calculation: Use raw implied probability (1/price) because that's what you actually pay
        # Edge = true_prob - price_you_pay
        exchange_prob_implied = 1 / exchange_odd['price']
        ev_edge = avg_bookmaker_prob - exchange_prob_implied
        
        # Arb calculation: Find the highest price on the opposite outcome
        # Higher price = better for bettor = more likely to create an arb
        opposite_outcomes = [name for name in exchange_by_outcome.keys() if name != outcome_name]
        best_opposite = None
        best_opposite_prob_clean = None
        best_opposite_price = 0.0
        arb_profit_pct = 0.0
        arb_total_implied = 0.0
        
        if opposite_outcomes:
            for opp_outcome in opposite_outcomes:
                opp_bookmakers = bookmakers_by_outcome.get(opp_outcome, [])
                for bookmaker in opp_bookmakers:
                    if best_opposite is None or bookmaker['price'] > best_opposite_price:
                        bookmaker_all_outcomes = [o for o in bookmaker_odds if o['bookmaker_key'] == bookmaker['bookmaker_key']]
                        probs_clean = remove_vig([o['price'] for o in bookmaker_all_outcomes])
                        outcome_idx = next(i for i, o in enumerate(bookmaker_all_outcomes) if o['outcome_name'] == opp_outcome)
                        
                        best_opposite = bookmaker
                        best_opposite['outcome_name'] = opp_outcome
                        best_opposite_prob_clean = probs_clean[outcome_idx]
                        best_opposite_price = bookmaker['price']
                
                if opp_outcome in exchange_by_outcome:
                    opp_exchange = exchange_by_outcome[opp_outcome]
                    if opp_exchange['bookmaker_key'] != target_exchange:
                        if best_opposite is None or opp_exchange['price'] > best_opposite_price:
                            opp_idx = list(exchange_by_outcome.keys()).index(opp_outcome)
                            best_opposite = opp_exchange
                            best_opposite['outcome_name'] = opp_outcome
                            best_opposite_prob_clean = exchange_probs_clean[opp_idx]
                            best_opposite_price = opp_exchange['price']
            
            # If total implied prob < 1.0, there's an arb opportunity
            if best_opposite:
                arb_total_implied = (1 / exchange_odd['price']) + (1 / best_opposite_price)
                arb_profit_pct = (1 - arb_total_implied) if arb_total_implied < 1 else 0
        
        if best_opposite is None:
            continue
        
        row = (
            batch_key,
            event_id,
            outcome_name,
            meta['sport_key'],
            meta['home_team'],
            meta['away_team'],
            meta['commence_time'],
            target_exchange,
            exchange_odd['price'],
            exchange_prob_implied,
            exchange_probs_clean[i],
            exchange_odd.get('bookmaker_link'),
            len(bookmakers),
            json.dumps(bookmaker_data),
            avg_bookmaker_prob,
            std_bookmaker_prob,
            ev_edge,
            best_opposite['outcome_name'],
            best_opposite['bookmaker_key'],
            best_opposite.get('bookmaker_title', best_opposite.get('title')),
            best_opposite['price'],
            best_opposite_prob_clean,
            best_opposite.get('bookmaker_link'),
            arb_profit_pct,
            arb_total_implied
        )
        rows.append(row)
    
    return rows


def remove_vig(prices: List[float]) -> List[float]:
    implied_probs = [1 / price for price in prices]
    total_implied = sum(implied_probs)
    return [prob / total_implied for prob in implied_probs]
