CREATE TABLE IF NOT EXISTS trade_executions (
    id SERIAL PRIMARY KEY,
    batch_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    outcome_name TEXT NOT NULL,
    exchange_key TEXT NOT NULL,
    token_id TEXT,
    target_price REAL NOT NULL,
    size REAL NOT NULL,
    ev_edge REAL NOT NULL,
    order_id TEXT,
    status TEXT NOT NULL,
    error_msg TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
