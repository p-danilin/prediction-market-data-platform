import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
from unittest.mock import Mock, MagicMock

# Mock airflow before importing
sys.modules['airflow.providers.sqlite.hooks.sqlite'] = MagicMock()

from dags.tasks.analyze_odds import analyze_odds, remove_vig, group_by_event, analyze_event


def test_remove_vig():
    """Test vig removal returns probabilities that sum to 1"""
    prices = [1.5, 2.5, 4.0]
    
    probs = remove_vig(prices)
    
    assert len(probs) == 3
    assert abs(sum(probs) - 1.0) < 0.0001
    assert probs[0] > probs[1] > probs[2]  # Lower price = higher prob


def test_remove_vig_two_outcomes():
    """Test vig removal with typical two-outcome market"""
    prices = [1.91, 1.91]  # Typical -110 odds
    
    probs = remove_vig(prices)
    
    assert abs(sum(probs) - 1.0) < 0.0001
    assert abs(probs[0] - 0.5) < 0.01
    assert abs(probs[1] - 0.5) < 0.01


def test_group_by_event():
    """Test grouping snapshots by event_id"""
    snapshots = [
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'fanduel', 'Pistons', 1.5, 'FanDuel', 'http://link1', 'http://link2'),
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'fanduel', 'Thunder', 2.5, 'FanDuel', 'http://link1', 'http://link3'),
        ('event2', 'basketball_nba', 'Lakers', 'Celtics', '2026-02-26T01:00:00Z',
         'draftkings', 'Lakers', 1.8, 'DraftKings', 'http://link4', 'http://link5'),
    ]
    
    events = group_by_event(snapshots)
    
    assert len(events) == 2
    assert 'event1' in events
    assert 'event2' in events
    assert events['event1']['meta']['home_team'] == 'Pistons'
    assert len(events['event1']['odds']) == 2
    assert len(events['event2']['odds']) == 1


def test_analyze_event_with_ev_and_arb():
    """Test analyzing an event with both EV and arb opportunities"""
    event_data = {
        'meta': {
            'sport_key': 'basketball_nba',
            'home_team': 'Pistons',
            'away_team': 'Thunder',
            'commence_time': '2026-02-26T00:00:00Z'
        },
        'odds': [
            # Exchange (Polymarket)
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Pistons', 'price': 1.5,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly1'},
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Thunder', 'price': 2.8,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly2'},
            # Bookmakers
            {'bookmaker_key': 'fanduel', 'outcome_name': 'Pistons', 'price': 1.6,
             'bookmaker_title': 'FanDuel', 'bookmaker_link': 'http://fd', 'outcome_link': 'http://fd1'},
            {'bookmaker_key': 'fanduel', 'outcome_name': 'Thunder', 'price': 2.5,
             'bookmaker_title': 'FanDuel', 'bookmaker_link': 'http://fd', 'outcome_link': 'http://fd2'},
            {'bookmaker_key': 'draftkings', 'outcome_name': 'Pistons', 'price': 1.55,
             'bookmaker_title': 'DraftKings', 'bookmaker_link': 'http://dk', 'outcome_link': 'http://dk1'},
            {'bookmaker_key': 'draftkings', 'outcome_name': 'Thunder', 'price': 2.6,
             'bookmaker_title': 'DraftKings', 'bookmaker_link': 'http://dk', 'outcome_link': 'http://dk2'},
        ]
    }
    
    rows = analyze_event('batch1', 'event1', event_data, 'polymarket')
    
    assert len(rows) == 2  # One row per exchange outcome
    
    # Check first row structure
    row = rows[0]
    assert row[0] == 'batch1'  # batch_key
    assert row[1] == 'event1'  # event_id
    assert row[2] in ['Pistons', 'Thunder']  # outcome_name
    assert row[3] == 'basketball_nba'  # sport_key
    assert row[7] == 'polymarket'  # exchange_key
    assert isinstance(row[8], float)  # exchange_price
    assert isinstance(row[9], float)  # exchange_prob_implied
    assert isinstance(row[10], float)  # exchange_prob_clean
    assert isinstance(row[12], int)  # num_bookmakers
    assert row[12] == 2  # Should have 2 bookmakers
    
    # Check bookmaker_probs is valid JSON
    import json
    bookmaker_probs = json.loads(row[13])
    assert isinstance(bookmaker_probs, list)
    assert len(bookmaker_probs) == 2
    assert 'key' in bookmaker_probs[0]
    assert 'prob_clean' in bookmaker_probs[0]
    
    assert isinstance(row[14], float)  # avg_bookmaker_prob
    assert isinstance(row[16], (int, float))  # ev_edge
    assert isinstance(row[21], float)  # best_opposite_prob_clean
    assert isinstance(row[23], (int, float))  # arb_profit_pct
    assert isinstance(row[24], float)  # arb_total_implied_prob


def test_analyze_event_no_bookmakers():
    """Test that events without bookmakers are skipped"""
    event_data = {
        'meta': {
            'sport_key': 'basketball_nba',
            'home_team': 'Pistons',
            'away_team': 'Thunder',
            'commence_time': '2026-02-26T00:00:00Z'
        },
        'odds': [
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Pistons', 'price': 1.5,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly1'},
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Thunder', 'price': 2.8,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly2'},
        ]
    }
    
    rows = analyze_event('batch1', 'event1', event_data, 'polymarket')
    
    assert len(rows) == 0  # No bookmakers, so no analysis


def test_analyze_event_no_exchange():
    """Test that events without exchange odds are skipped"""
    event_data = {
        'meta': {
            'sport_key': 'basketball_nba',
            'home_team': 'Pistons',
            'away_team': 'Thunder',
            'commence_time': '2026-02-26T00:00:00Z'
        },
        'odds': [
            {'bookmaker_key': 'fanduel', 'outcome_name': 'Pistons', 'price': 1.6,
             'bookmaker_title': 'FanDuel', 'bookmaker_link': 'http://fd', 'outcome_link': 'http://fd1'},
            {'bookmaker_key': 'fanduel', 'outcome_name': 'Thunder', 'price': 2.5,
             'bookmaker_title': 'FanDuel', 'bookmaker_link': 'http://fd', 'outcome_link': 'http://fd2'},
        ]
    }
    
    rows = analyze_event('batch1', 'event1', event_data, 'polymarket')
    
    assert len(rows) == 0  # No exchange, so no analysis


def test_analyze_event_selects_best_opposite_price():
    """Test that the bookmaker with the highest price is selected for opposite side"""
    event_data = {
        'meta': {
            'sport_key': 'basketball_nba',
            'home_team': 'Clippers',
            'away_team': 'Timberwolves',
            'commence_time': '2026-02-26T00:00:00Z'
        },
        'odds': [
            # Exchange
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Clippers', 'price': 2.5,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly1'},
            {'bookmaker_key': 'polymarket', 'outcome_name': 'Timberwolves', 'price': 1.6,
             'bookmaker_title': 'Polymarket', 'bookmaker_link': None, 'outcome_link': 'http://poly2'},
            # Bookmakers with different prices
            {'bookmaker_key': 'betmgm', 'outcome_name': 'Clippers', 'price': 2.7,
             'bookmaker_title': 'BetMGM', 'bookmaker_link': 'http://mgm', 'outcome_link': 'http://mgm1'},
            {'bookmaker_key': 'betmgm', 'outcome_name': 'Timberwolves', 'price': 1.48,
             'bookmaker_title': 'BetMGM', 'bookmaker_link': 'http://mgm', 'outcome_link': 'http://mgm2'},
            {'bookmaker_key': 'draftkings', 'outcome_name': 'Clippers', 'price': 2.8,
             'bookmaker_title': 'DraftKings', 'bookmaker_link': 'http://dk', 'outcome_link': 'http://dk1'},
            {'bookmaker_key': 'draftkings', 'outcome_name': 'Timberwolves', 'price': 1.46,
             'bookmaker_title': 'DraftKings', 'bookmaker_link': 'http://dk', 'outcome_link': 'http://dk2'},
            {'bookmaker_key': 'betonlineag', 'outcome_name': 'Clippers', 'price': 2.74,
             'bookmaker_title': 'BetOnline', 'bookmaker_link': 'http://bol', 'outcome_link': 'http://bol1'},
            {'bookmaker_key': 'betonlineag', 'outcome_name': 'Timberwolves', 'price': 1.5,
             'bookmaker_title': 'BetOnline', 'bookmaker_link': 'http://bol', 'outcome_link': 'http://bol2'},
        ]
    }
    
    rows = analyze_event('batch1', 'event1', event_data, 'polymarket')
    
    assert len(rows) == 2
    
    # Find the Clippers row (betting on Clippers at Polymarket)
    clippers_row = next(r for r in rows if r[2] == 'Clippers')
    
    # best_opposite_source_key should be betonlineag (1.5 is highest for Timberwolves)
    assert clippers_row[18] == 'betonlineag', f"Expected betonlineag but got {clippers_row[18]}"
    assert clippers_row[20] == 1.5, f"Expected price 1.5 but got {clippers_row[20]}"
    
    # Find the Timberwolves row (betting on Timberwolves at Polymarket)
    wolves_row = next(r for r in rows if r[2] == 'Timberwolves')
    
    # best_opposite_source_key should be draftkings (2.8 is highest for Clippers)
    assert wolves_row[18] == 'draftkings', f"Expected draftkings but got {wolves_row[18]}"
    assert wolves_row[20] == 2.8, f"Expected price 2.8 but got {wolves_row[20]}"


def test_analyze_odds_integration(monkeypatch):
    """Test full analyze_odds function with mocked database"""
    mock_hook = Mock()
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    
    # Mock database responses
    mock_hook_instance.get_records.return_value = [
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'polymarket', 'Pistons', 1.5, 'Polymarket', None, 'http://poly1'),
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'polymarket', 'Thunder', 2.8, 'Polymarket', None, 'http://poly2'),
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'fanduel', 'Pistons', 1.6, 'FanDuel', 'http://fd', 'http://fd1'),
        ('event1', 'basketball_nba', 'Pistons', 'Thunder', '2026-02-26T00:00:00Z',
         'fanduel', 'Thunder', 2.5, 'FanDuel', 'http://fd', 'http://fd2'),
    ]
    
    mock_conn = Mock()
    mock_hook_instance.get_conn.return_value = mock_conn
    
    # Patch SqliteHook in the module
    import dags.tasks.analyze_odds
    monkeypatch.setattr(dags.tasks.analyze_odds, 'SqliteHook', mock_hook)
    
    batch_key = '20260226T120000'
    analyze_odds(batch_key)
    
    # Verify DELETE was called
    mock_hook_instance.run.assert_called_once_with(
        "DELETE FROM odds_analysis WHERE batch_key = ?",
        parameters=(batch_key,)
    )
    
    # Verify get_records was called with filtered query
    call_args = mock_hook_instance.get_records.call_args
    assert 'bookmaker_key IN' in call_args[0][0]
    
    # Verify executemany was called
    assert mock_conn.executemany.called
    call_args = mock_conn.executemany.call_args
    assert 'INSERT INTO odds_analysis' in call_args[0][0]
    
    # Should have 2 rows (one per exchange outcome)
    rows = call_args[0][1]
    assert len(rows) == 2
    
    # Verify bookmaker_probs is JSON (now at index 13)
    import json
    bookmaker_probs = json.loads(rows[0][13])
    assert isinstance(bookmaker_probs, list)
    assert len(bookmaker_probs) == 1  # Only fanduel in test data
    
    # Verify commit
    assert mock_conn.commit.called
