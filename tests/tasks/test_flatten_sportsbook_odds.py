import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import pytest
from unittest.mock import Mock, patch, MagicMock

# Mock airflow before importing
sys.modules['airflow.providers.sqlite.hooks.sqlite'] = MagicMock()

from dags.tasks.flatten_sportsbook_odds import flatten_sportsbook_odds


@patch('dags.tasks.flatten_sportsbook_odds.SqliteHook')
def test_flatten_sportsbook_odds(mock_hook):
    """Test that flatten_sportsbook_odds correctly flattens nested odds data"""
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    
    # Mock raw_odds data
    raw_event = {
        'id': 'event1',
        'sport_key': 'basketball_nba',
        'home_team': 'Pistons',
        'away_team': 'Thunder',
        'commence_time': '2026-02-26T00:00:00Z',
        'bookmakers': [
            {
                'key': 'fanduel',
                'title': 'FanDuel',
                'link': 'https://fanduel.com',
                'last_update': '2026-02-26T00:00:00Z',
                'markets': [
                    {
                        'key': 'h2h',
                        'outcomes': [
                            {'name': 'Pistons', 'price': 1.5, 'link': 'https://fanduel.com/pistons'},
                            {'name': 'Thunder', 'price': 2.5, 'link': 'https://fanduel.com/thunder'}
                        ]
                    }
                ]
            },
            {
                'key': 'draftkings',
                'title': 'DraftKings',
                'markets': [
                    {
                        'key': 'h2h',
                        'outcomes': [
                            {'name': 'Pistons', 'price': 1.6},
                            {'name': 'Thunder', 'price': 2.4}
                        ]
                    }
                ]
            }
        ]
    }
    
    mock_hook_instance.get_records.return_value = [
        ('event1', json.dumps(raw_event))
    ]
    
    mock_conn = Mock()
    mock_hook_instance.get_conn.return_value = mock_conn
    
    batch_key = '20260226T120000'
    flatten_sportsbook_odds(batch_key)
    
    # Verify DELETE was called for idempotency
    mock_hook_instance.run.assert_called_once_with(
        "DELETE FROM odds_snapshots WHERE batch_key = ?",
        parameters=(batch_key,)
    )
    
    # Verify executemany was called with flattened data
    assert mock_conn.executemany.called
    call_args = mock_conn.executemany.call_args
    
    # Check SQL statement
    assert 'INSERT INTO odds_snapshots' in call_args[0][0]
    
    # Check flattened rows (should be 4: 2 bookmakers × 2 outcomes)
    rows = call_args[0][1]
    assert len(rows) == 4
    
    # Verify first row structure
    first_row = rows[0]
    assert first_row[0] == batch_key  # batch_key
    assert first_row[1] == 'event1'  # event_id
    assert first_row[2] == 'basketball_nba'  # sport_key
    assert first_row[3] == 'Pistons'  # home_team
    assert first_row[4] == 'Thunder'  # away_team
    assert first_row[6] in ['fanduel', 'draftkings']  # bookmaker_key
    assert first_row[8] in ['Pistons', 'Thunder']  # outcome_name
    assert isinstance(first_row[9], float)  # price
    
    # Verify commit was called
    assert mock_conn.commit.called


@patch('dags.tasks.flatten_sportsbook_odds.SqliteHook')
def test_flatten_sportsbook_odds_handles_missing_links(mock_hook):
    """Test that flatten handles missing optional fields like links"""
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    
    raw_event = {
        'id': 'event1',
        'sport_key': 'basketball_nba',
        'home_team': 'Pistons',
        'away_team': 'Thunder',
        'commence_time': '2026-02-26T00:00:00Z',
        'bookmakers': [
            {
                'key': 'draftkings',
                'title': 'DraftKings',
                # No 'link' field
                'markets': [
                    {
                        'key': 'h2h',
                        'outcomes': [
                            {'name': 'Pistons', 'price': 1.5}  # No 'link' field
                        ]
                    }
                ]
            }
        ]
    }
    
    mock_hook_instance.get_records.return_value = [
        ('event1', json.dumps(raw_event))
    ]
    
    mock_conn = Mock()
    mock_hook_instance.get_conn.return_value = mock_conn
    
    batch_key = '20260226T120000'
    flatten_sportsbook_odds(batch_key)
    
    # Should not raise an error
    assert mock_conn.executemany.called
    rows = mock_conn.executemany.call_args[0][1]
    assert len(rows) == 1
    
    # Links should be None
    row = rows[0]
    assert row[10] is None  # bookmaker_link
    assert row[11] is None  # outcome_link


@patch('dags.tasks.flatten_sportsbook_odds.SqliteHook')
def test_flatten_sportsbook_odds_empty_batch(mock_hook):
    """Test that flatten handles empty batch gracefully"""
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    
    mock_hook_instance.get_records.return_value = []
    
    batch_key = '20260226T120000'
    flatten_sportsbook_odds(batch_key)
    
    # Should still call DELETE for idempotency
    mock_hook_instance.run.assert_called_once()
    
    # Should not call executemany if no rows
    mock_hook_instance.get_conn.assert_not_called()
