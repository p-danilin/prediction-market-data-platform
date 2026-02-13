import requests
from typing import Dict, Any, List


def fetch_event_markets(event_slug: str) -> List[Dict[str, Any]]:
    """Fetch all markets for an event from Gamma API."""
    url = f"https://gamma-api.polymarket.com/events/slug/{event_slug}"
    
    response = requests.get(url)
    response.raise_for_status()
    
    event = response.json()
    return event.get('markets', [])


def fetch_market_data(market_slug: str) -> Dict[str, Any]:
    """Fetch specific market data from Gamma API by slug."""
    url = f"https://gamma-api.polymarket.com/markets"
    params = {"slug": market_slug}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    markets = response.json()
    if not markets:
        raise ValueError(f"No market found for slug: {market_slug}")
    
    return markets[0]
