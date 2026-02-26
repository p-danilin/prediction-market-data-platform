CREATE TABLE IF NOT EXISTS whale_profiles (
    wallet_address TEXT PRIMARY KEY,
    username TEXT,
    volume REAL,
    pnl REAL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)