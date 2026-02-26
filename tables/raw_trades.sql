CREATE TABLE IF NOT EXISTS raw_trades (
    transaction_hash TEXT PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    market_title TEXT,
    market_slug TEXT,
    side TEXT,
    outcome TEXT,
    price REAL,
    size REAL,
    timestamp INTEGER,
    batch_key TEXT,
    loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
)