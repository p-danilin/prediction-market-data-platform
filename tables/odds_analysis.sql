CREATE TABLE IF NOT EXISTS {{ table }} (
    batch_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    outcome_name TEXT NOT NULL,
    
    -- Event context
    sport_key TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    commence_time DATETIME NOT NULL,
    
    -- Exchange side (where you bet)
    exchange_key TEXT NOT NULL,
    exchange_price REAL NOT NULL,
    exchange_prob_implied REAL NOT NULL,  -- Raw implied prob (1/price) - what you actually pay
    exchange_prob_clean REAL NOT NULL,    -- Vig-removed prob (for reference)
    exchange_link TEXT,
    
    -- Bookmaker consensus (the "true" probability)
    num_bookmakers INTEGER NOT NULL,
    bookmaker_probs TEXT NOT NULL,  -- JSON array of {key, title, price, prob_clean}
    avg_bookmaker_prob REAL NOT NULL,
    std_bookmaker_prob REAL,
    
    -- EV calculation
    ev_edge REAL NOT NULL,
    
    -- Arb calculation (exchange vs best opposite side from any source)
    best_opposite_outcome TEXT NOT NULL,
    best_opposite_source_key TEXT NOT NULL,  -- Could be bookmaker or exchange
    best_opposite_source_title TEXT NOT NULL,
    best_opposite_price REAL NOT NULL,
    best_opposite_prob_clean REAL NOT NULL,
    best_opposite_link TEXT,
    arb_profit_pct REAL NOT NULL,
    arb_total_implied_prob REAL NOT NULL,  -- Sum of both sides' implied probs
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_key, event_id, exchange_key, outcome_name)
);