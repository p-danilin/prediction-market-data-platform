import os
import json
import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, PartialCreateOrderOptions, OrderType
from py_clob_client.order_builder.constants import BUY

POLYMARKET_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DEFAULT_SIGNATURE_TYPE = 2
DEFAULT_MIN_ORDER_SIZE = 1.0
TICK_SIZE = "0.01"
REQUEST_TIMEOUT = 10


class PolymarketClient:
    def __init__(self):
        api_creds = ApiCreds(
            api_key=os.getenv("POLYMARKET_API_KEY"),
            api_secret=os.getenv("POLYMARKET_API_SECRET"),
            api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE")
        )
        
        self.client = ClobClient(
            host=POLYMARKET_HOST,
            chain_id=POLYMARKET_CHAIN_ID,
            key=os.getenv("POLYMARKET_PRIVATE_KEY"),
            creds=api_creds,
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", str(DEFAULT_SIGNATURE_TYPE))),
            funder=os.getenv("POLYMARKET_FUNDER_ADDRESS")
        )
    
    def get_token_id_and_min_size(self, slug, outcome_name):
        """Fetch token_id from Polymarket API, then get min_order_size from CLOB"""
        try:
            event = self._fetch_event(slug)
            if not event:
                return None, None
            
            token_id = self._find_token_id(event, outcome_name)
            if not token_id:
                return None, None
            
            min_size = self._fetch_min_order_size(token_id)
            return token_id, min_size
            
        except Exception as e:
            print(f"Error fetching token_id for {slug}: {e}")
            return None, None
    
    def place_order(self, token_id, price, size, expiration):
        """Place a limit order on Polymarket"""
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
            expiration=expiration
        )
        
        options = PartialCreateOrderOptions(tick_size=TICK_SIZE, neg_risk=False)
        signed_order = self.client.create_order(order_args, options)
        return self.client.post_order(signed_order, orderType=OrderType.GTD)
    
    def _fetch_event(self, slug):
        resp = requests.get(f"{GAMMA_API_BASE}/events/slug/{slug}", timeout=REQUEST_TIMEOUT)
        return resp.json() if resp.status_code == 200 else None
    
    def _find_token_id(self, event, outcome_name):
        markets = event.get('markets', [])
        
        for market in markets:
            if market.get('sportsMarketType') != 'moneyline':
                continue
            
            outcomes = json.loads(market.get('outcomes', '[]'))
            token_ids = json.loads(market.get('clobTokenIds', '[]'))
            
            for i, outcome in enumerate(outcomes):
                if self._outcome_matches(outcome, outcome_name) and i < len(token_ids):
                    return token_ids[i]
        
        return None
    
    def _outcome_matches(self, outcome, outcome_name):
        return outcome_name.lower() in outcome.lower() or outcome.lower() in outcome_name.lower()
    
    def _fetch_min_order_size(self, token_id):
        resp = requests.get(
            f"{POLYMARKET_HOST}/book?token_id={token_id}",
            timeout=REQUEST_TIMEOUT
        )
        
        if resp.status_code == 200:
            book_data = resp.json()
            return float(book_data.get('min_order_size', DEFAULT_MIN_ORDER_SIZE))
        
        return DEFAULT_MIN_ORDER_SIZE
