-- Create mart schema
CREATE SCHEMA IF NOT EXISTS mart;

-- Daily spend aggregated by category
CREATE TABLE IF NOT EXISTS mart.daily_spend_by_category (
    id              SERIAL PRIMARY KEY,
    spend_date      DATE,
    category_code   VARCHAR(50),
    category_name   VARCHAR(100),
    total_txns      INT,
    total_amount_usd NUMERIC(15, 2),
    avg_txn_amount  NUMERIC(15, 2),
    max_txn_amount  NUMERIC(15, 2),
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (spend_date, category_code)
);
