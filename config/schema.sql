-- AlgoSoft Multi-Tenant Schema
-- EC2 setup: psql -U postgres -d algosoft -f config/schema.sql

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'client',
    is_active BOOLEAN DEFAULT FALSE,
    subscription_tier VARCHAR(20) DEFAULT 'FREE',
    max_brokers INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    activated_at TIMESTAMP WITH TIME ZONE,
    activated_by INTEGER
);

CREATE TABLE IF NOT EXISTS data_providers (
    id SERIAL PRIMARY KEY,
    provider VARCHAR(30) NOT NULL UNIQUE,
    api_key_encrypted TEXT,
    access_token_encrypted TEXT,
    status VARCHAR(20) DEFAULT 'not_configured',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS client_broker_instances (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    broker VARCHAR(30) NOT NULL,
    instance_label VARCHAR(100),
    api_key_encrypted TEXT,
    access_token_encrypted TEXT,
    trading_mode VARCHAR(10) DEFAULT 'paper',
    instrument VARCHAR(20) DEFAULT 'NIFTY',
    quantity INTEGER DEFAULT 25,
    strategy_version VARCHAR(5) DEFAULT 'V3',
    status VARCHAR(20) DEFAULT 'idle',
    bot_pid INTEGER,
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(client_id, broker)
);

CREATE TABLE IF NOT EXISTS order_state (
    id SERIAL PRIMARY KEY,
    instance_id INTEGER NOT NULL REFERENCES client_broker_instances(id) ON DELETE CASCADE,
    trade_id VARCHAR(50),
    ce_order_id VARCHAR(50),
    pe_order_id VARCHAR(50),
    ce_status VARCHAR(20),
    pe_status VARCHAR(20),
    ce_fill_price NUMERIC(10,2),
    pe_fill_price NUMERIC(10,2),
    total_premium NUMERIC(10,2),
    state VARCHAR(30) DEFAULT 'PENDING',
    retry_count INTEGER DEFAULT 0,
    failure_reason TEXT,
    opened_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    closed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS trade_history (
    id SERIAL PRIMARY KEY,
    instance_id INTEGER NOT NULL REFERENCES client_broker_instances(id) ON DELETE CASCADE,
    client_id INTEGER NOT NULL REFERENCES users(id),
    trade_type VARCHAR(10),
    direction VARCHAR(10),
    strike INTEGER,
    entry_price NUMERIC(10,2),
    exit_price NUMERIC(10,2),
    pnl_pts NUMERIC(10,2),
    pnl_rs NUMERIC(12,2),
    quantity INTEGER,
    broker VARCHAR(30),
    exit_reason TEXT,
    instrument VARCHAR(20),
    trading_mode VARCHAR(10),
    opened_at TIMESTAMP WITH TIME ZONE,
    closed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_failures (
    id SERIAL PRIMARY KEY,
    instance_id INTEGER NOT NULL REFERENCES client_broker_instances(id) ON DELETE CASCADE,
    client_id INTEGER NOT NULL REFERENCES users(id),
    order_side VARCHAR(5),
    broker_error TEXT,
    failure_reason VARCHAR(50),
    retry_attempt INTEGER DEFAULT 0,
    paired_leg_closed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    actor_id INTEGER REFERENCES users(id),
    actor_role VARCHAR(20),
    action VARCHAR(100),
    target_type VARCHAR(30),
    target_id INTEGER,
    details JSONB,
    ip_address VARCHAR(45),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cbi_client_id ON client_broker_instances(client_id);
CREATE INDEX IF NOT EXISTS idx_order_state_instance ON order_state(instance_id);
CREATE INDEX IF NOT EXISTS idx_trade_history_client ON trade_history(client_id);
CREATE INDEX IF NOT EXISTS idx_trade_history_instance ON trade_history(instance_id);
CREATE INDEX IF NOT EXISTS idx_order_failures_instance ON order_failures(instance_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_actor ON audit_log(actor_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);

INSERT INTO data_providers (provider, status)
VALUES ('upstox', 'not_configured')
ON CONFLICT (provider) DO NOTHING;
