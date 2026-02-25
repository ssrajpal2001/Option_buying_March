-- Multi-Tenant Trading Bot Schema (Commercial Phase 1)

-- 1. Users Table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255), -- For future Web Dashboard
    subscription_tier VARCHAR(50) DEFAULT 'FREE', -- FREE, PRO, ELITE
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. User Broker Credentials
-- Stores API keys for Zerodha, Upstox, etc.
CREATE TABLE IF NOT EXISTS user_brokers (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    broker_name VARCHAR(50) NOT NULL, -- ZERODHA, ANGELONE, UPSTOX
    instance_name VARCHAR(100) NOT NULL, -- e.g., 'Primary Account'
    api_key VARCHAR(255) NOT NULL,
    api_secret_encrypted TEXT NOT NULL, -- Encrypted at rest
    access_token TEXT, -- Optional: cached current token
    token_updated_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    broker_settings JSONB DEFAULT '{}', -- User-specific qty, etc.
    UNIQUE(user_id, instance_name)
);

-- 3. User Strategy Config
-- Allows different users to run the same logic with different parameters
CREATE TABLE IF NOT EXISTS user_strategies (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    instrument_name VARCHAR(50) NOT NULL, -- NIFTY, BANKNIFTY
    logic_version VARCHAR(10) DEFAULT 'v3',
    entry_tf_minutes INTEGER DEFAULT 1,
    exit_tf_minutes INTEGER DEFAULT 5,
    exit_logic_type VARCHAR(50) DEFAULT 'S1_LOW',
    itm_distance INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, instrument_name)
);

-- 4. Trades Ledger (Multi-Tenant)
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    broker_id INTEGER REFERENCES user_brokers(id),
    strategy_id INTEGER REFERENCES user_strategies(id),
    instrument_symbol VARCHAR(100) NOT NULL,
    side VARCHAR(10) NOT NULL, -- CALL, PUT
    entry_price NUMERIC(10, 2),
    exit_price NUMERIC(10, 2),
    entry_timestamp TIMESTAMP WITH TIME ZONE,
    exit_timestamp TIMESTAMP WITH TIME ZONE,
    pnl NUMERIC(15, 2),
    status VARCHAR(20), -- OPEN, CLOSED, CANCELLED
    exit_reason TEXT,
    strategy_log TEXT, -- Narrative of the trade
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trades_user_id ON trades(user_id);
CREATE INDEX IF NOT EXISTS idx_user_brokers_user_id ON user_brokers(user_id);
CREATE INDEX IF NOT EXISTS idx_user_strategies_user_id ON user_strategies(user_id);
