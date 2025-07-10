-- Stock Analysis Database Schema for PostgreSQL

-- Main stocks table
CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(255),
    sector VARCHAR(100),
    market_cap_category VARCHAR(50), -- e.g., 'Mid Cap', 'Large Cap', etc.
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Stock analysis snapshots table
CREATE TABLE stock_analyses (
    id SERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    analysis_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Price metrics
    current_price DECIMAL(10,2) NOT NULL,
    mean_price DECIMAL(10,2) NOT NULL,
    price_deviation_percent DECIMAL(5,2), -- +8.7%
    
    -- Technical indicators
    z_score DECIMAL(6,3), -- 3.15
    rsi DECIMAL(5,2), -- 71.4
    bollinger_position DECIMAL(5,3), -- 1.03
    volatility_percent DECIMAL(5,2), -- 31.8%
    volume_ratio DECIMAL(5,2), -- 0.9x
    five_day_change_percent DECIMAL(5,2), -- +7.4%
    hurst_exponent DECIMAL(4,3), -- 0.62
    
    -- Analysis results
    strength_rating VARCHAR(20), -- 'Strong', 'Weak', 'Moderate'
    market_condition VARCHAR(20), -- 'OVERBOUGHT', 'OVERSOLD', 'NEUTRAL'
    
    UNIQUE(stock_id, analysis_date)
);

-- Trading recommendations table
CREATE TABLE trading_recommendations (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER NOT NULL REFERENCES stock_analyses(id) ON DELETE CASCADE,
    
    -- Recommendation details
    action VARCHAR(20) NOT NULL, -- 'BUY_PUTS', 'BUY_CALLS', 'HOLD', 'SELL'
    confidence_level VARCHAR(10) NOT NULL, -- 'HIGH', 'MEDIUM', 'LOW'
    time_horizon VARCHAR(10) NOT NULL, -- 'SHORT', 'MEDIUM', 'LONG'
    
    -- Entry criteria (stored as JSON for flexibility)
    entry_criteria JSONB,
    
    -- Exit criteria (stored as JSON for flexibility)
    exit_criteria JSONB,
    
    -- Risk factors (stored as JSON array)
    risk_factors JSONB,
    
    -- Educational notes
    educational_notes TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_stocks_symbol ON stocks(symbol);
CREATE INDEX idx_stock_analyses_stock_id ON stock_analyses(stock_id);
CREATE INDEX idx_stock_analyses_date ON stock_analyses(analysis_date);
CREATE INDEX idx_trading_recommendations_analysis_id ON trading_recommendations(analysis_id);

-- Example data insertion for the MT stock
INSERT INTO stocks (symbol, name, sector, market_cap_category) 
VALUES ('MT', 'ArcelorMittal', 'Basic Materials', 'Mid Cap');

-- Example analysis data
INSERT INTO stock_analyses (
    stock_id, 
    current_price, 
    mean_price, 
    price_deviation_percent,
    z_score,
    rsi,
    bollinger_position,
    volatility_percent,
    volume_ratio,
    five_day_change_percent,
    hurst_exponent,
    strength_rating,
    market_condition
) VALUES (
    1, -- assuming MT gets id 1
    33.53,
    30.84,
    8.7,
    3.15,
    71.4,
    1.03,
    31.8,
    0.9,
    7.4,
    0.62,
    'Strong',
    'OVERBOUGHT'
);

-- Example trading recommendation
INSERT INTO trading_recommendations (
    analysis_id,
    action,
    confidence_level,
    time_horizon,
    entry_criteria,
    exit_criteria,
    risk_factors,
    educational_notes
) VALUES (
    1, -- assuming the analysis gets id 1
    'BUY_PUTS',
    'HIGH',
    'SHORT',
    '["Price significantly above mean (z-score > 2)", "RSI indicates overbought (above 70)", "Bollinger position near upper band (> 0.9)"]'::jsonb,
    '["Price returns near or below mean", "RSI falls below 50"]'::jsonb,
    '["Stock may continue upward momentum longer than expected", "Market news or events can invalidate mean reversion"]'::jsonb,
    'Mean reversion strategy assumes price will revert to average'
);

