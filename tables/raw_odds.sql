CREATE TABLE IF NOT EXISTS raw_odds (
    id TEXT,
    batch_key TEXT,
    raw_response TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, batch_key)
)