CREATE TABLE IF NOT EXISTS {{ table }} (
    event_id TEXT PRIMARY KEY,
    sport_key TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    commence_time DATETIME NOT NULL,
    winner TEXT NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    completed BOOLEAN NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
