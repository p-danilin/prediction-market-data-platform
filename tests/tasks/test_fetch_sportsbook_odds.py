import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import pytest
from unittest.mock import Mock, patch, MagicMock

# Mock airflow before importing
sys.modules['airflow.providers.sqlite.hooks.sqlite'] = MagicMock()

from dags.tasks.fetch_sportsbook_odds import fetch_sportsbook_odds, get_sportsbook_odds


@patch('dags.tasks.fetch_sportsbook_odds.requests.get')
@patch('dags.tasks.fetch_sportsbook_odds.os.getenv')
def test_get_sportsbook_odds(mock_getenv, mock_requests_get):
    """Test that get_sportsbook_odds calls API correctly and returns events"""
    mock_getenv.return_value = 'test_api_key'
    
    mock_response = Mock()
    mock_response.json.return_value = [
        {
            'id': 'event1',
            'sport_key': 'basketball_nba',
            'home_team': 'Team A',
            'away_team': 'Team B'
        }
    ]
    mock_requests_get.return_value = mock_response
    
    events = get_sportsbook_odds()
    
    assert len(events) == 1
    assert events[0]['id'] == 'event1'
    mock_requests_get.assert_called_once()
    call_args = mock_requests_get.call_args
    assert 'basketball_nba' in call_args[0][0]
    assert call_args[1]['params']['apiKey'] == 'test_api_key'


@patch('dags.tasks.fetch_sportsbook_odds.SqliteHook')
@patch('dags.tasks.fetch_sportsbook_odds.get_sportsbook_odds')
def test_fetch_sportsbook_odds(mock_get_odds, mock_hook):
    """Test that fetch_sportsbook_odds inserts events into database"""
    mock_get_odds.return_value = [
        {
            'id': 'event1',
            'sport_key': 'basketball_nba',
            'home_team': 'Pistons',
            'away_team': 'Thunder'
        },
        {
            'id': 'event2',
            'sport_key': 'basketball_nba',
            'home_team': 'Lakers',
            'away_team': 'Celtics'
        }
    ]
    
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    
    batch_key = '20260226T120000'
    fetch_sportsbook_odds(batch_key)
    
    assert mock_hook_instance.run.call_count == 2
    
    first_call = mock_hook_instance.run.call_args_list[0]
    assert 'INSERT OR REPLACE INTO raw_odds' in first_call[0][0]
    assert first_call[1]['parameters'][0] == 'event1'
    assert first_call[1]['parameters'][1] == batch_key
    assert 'Pistons' in first_call[1]['parameters'][2]
