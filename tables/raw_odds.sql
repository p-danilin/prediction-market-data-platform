CREATE TABLE IF NOT EXISTS {{ table }} (
    id TEXT,
    batch_key TEXT,
    raw_response TEXT,
    loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, batch_key)
)