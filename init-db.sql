-- Initialize databases on first start
CREATE DATABASE "conduktor-console";
GRANT ALL PRIVILEGES ON DATABASE "conduktor-console" TO postgres;

-- Wallet tables
CREATE TABLE IF NOT EXISTS wallets (
    user_id VARCHAR(64) PRIMARY KEY,
    balance DECIMAL(18,2) DEFAULT 0,
    version BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    type VARCHAR(20) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    balance_after DECIMAL(18,2),
    reference_id VARCHAR(128),
    status VARCHAR(20) DEFAULT 'SUCCESS',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at);
