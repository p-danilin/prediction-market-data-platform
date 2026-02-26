CREATE TABLE odds_snapshots (
    event_id TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    sport_key TEXT NOT NULL,
    commence_time DATETIME NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    bookmaker_key TEXT NOT NULL,
    bookmaker_title TEXT,
    last_update TEXT,
    bookmaker_link TEXT,
    market_link TEXT,
    outcome_name TEXT NOT NULL,
    price REAL NOT NULL,
    outcome_link TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_key, event_id, bookmaker_key, outcome_name)
)