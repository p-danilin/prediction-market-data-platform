import time
from .polymarket_client import PolymarketClient

ORDER_EXPIRATION_SECONDS = 120


class TradeExecutor:
    def __init__(self):
        self.client = PolymarketClient()
    
    def execute_opportunity(self, conn, batch_key, opportunity, min_bet_size=5.0):
        """Execute a single trade opportunity
        
        Args:
            conn: Database connection
            batch_key: Batch identifier
            opportunity: Tuple of (event_id, outcome_name, exchange_key, exchange_price, 
                                   ev_edge, sport_key, home_team, away_team, commence_time, 
                                   avg_bookmaker_prob, exchange_link, limit_price)
            min_bet_size: Minimum bet size in dollars
        """
        (event_id, outcome_name, exchange_key, exchange_price, ev_edge, 
         sport_key, home_team, away_team, commence_time, avg_bookmaker_prob, 
         exchange_link, limit_price) = opportunity
        
        slug = self._extract_slug(exchange_link)
        if not slug:
            self._log_trade(conn, batch_key, event_id, outcome_name, exchange_key, None, 
                           exchange_price, min_bet_size, ev_edge, None, 'failed', 'No exchange_link')
            print(f"✗ No exchange_link for {outcome_name}")
            return
        
        token_id, min_order_size = self.client.get_token_id_and_min_size(slug, outcome_name)
        if not token_id:
            self._log_trade(conn, batch_key, event_id, outcome_name, exchange_key, None, 
                           exchange_price, min_bet_size, ev_edge, None, 'failed', 'Could not find token_id')
            print(f"✗ Could not find token_id for {outcome_name} (slug: {slug})")
            return
        
        order_size = max(min_bet_size, min_order_size or min_bet_size)
        self._place_order(conn, batch_key, event_id, outcome_name, exchange_key, token_id, 
                         limit_price, order_size, ev_edge)
    
    def _extract_slug(self, exchange_link):
        return exchange_link.split('/event/')[-1] if exchange_link else None
    
    def _place_order(self, conn, batch_key, event_id, outcome_name, exchange_key, 
                     token_id, limit_price, order_size, ev_edge):
        try:
            expiration = int(time.time()) + ORDER_EXPIRATION_SECONDS
            order = self.client.place_order(token_id, limit_price, order_size, expiration)
            
            if order.get('success'):
                self._log_trade(conn, batch_key, event_id, outcome_name, exchange_key, token_id,
                               limit_price, order_size, ev_edge, order.get('orderID'), 'success', None)
                print(f"✓ Placed order {order.get('orderID')} for {outcome_name} @ {limit_price:.2f} (size: {order_size})")
            else:
                self._log_trade(conn, batch_key, event_id, outcome_name, exchange_key, token_id,
                               limit_price, order_size, ev_edge, None, 'failed', order.get('errorMsg'))
                print(f"✗ Failed to place order for {outcome_name}: {order.get('errorMsg')}")
        
        except Exception as e:
            self._log_trade(conn, batch_key, event_id, outcome_name, exchange_key, token_id,
                           limit_price, order_size, ev_edge, None, 'error', str(e))
            print(f"✗ Error placing order for {outcome_name}: {e}")
    
    def _log_trade(self, conn, batch_key, event_id, outcome_name, exchange_key, token_id, 
                   target_price, size, ev_edge, order_id, status, error_msg):
        conn.execute("""
            INSERT INTO trade_executions (
                batch_key, event_id, outcome_name, exchange_key, token_id,
                target_price, size, ev_edge, order_id, status, error_msg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (batch_key, event_id, outcome_name, exchange_key, token_id,
              target_price, size, ev_edge, order_id, status, error_msg))
