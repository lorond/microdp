CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER nessie WITH PASSWORD 'nessie';
CREATE DATABASE nessie OWNER nessie;

-- Subsequent statements run on $POSTGRES_DB (typically `wallet`), provided by the
-- postgres entrypoint. Do NOT \connect to a hard-coded name — that breaks when
-- POSTGRES_DB is overridden via .env.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    currency TEXT NOT NULL DEFAULT 'USD',
    opening_balance NUMERIC(18, 2) NOT NULL DEFAULT 0,
    current_balance NUMERIC(18, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, currency)
);

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id),
    account_id UUID NOT NULL REFERENCES accounts(id),
    type TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('credit', 'debit')),
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL DEFAULT 'USD',
    description TEXT,
    merchant TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    balance_after NUMERIC(18, 2) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_user_created ON transactions(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_account_created ON transactions(account_id, created_at DESC);

INSERT INTO users (id, full_name, email)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'Demo Customer', 'demo@example.com'),
    ('00000000-0000-0000-0000-000000000002', 'Travel Customer', 'travel@example.com'),
    ('00000000-0000-0000-0000-000000000003', 'Retail Customer', 'retail@example.com')
ON CONFLICT (id) DO NOTHING;

INSERT INTO accounts (id, user_id, currency, opening_balance, current_balance)
VALUES
    ('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 'USD', 1250.00, 1250.00),
    ('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000002', 'USD', 850.00, 850.00),
    ('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000003', 'USD', 1500.00, 1500.00)
ON CONFLICT (id) DO NOTHING;

