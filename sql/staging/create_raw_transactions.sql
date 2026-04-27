-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Raw transactions table (exact dump, no transformations)
CREATE TABLE IF NOT EXISTS staging.raw_transactions (
    id              SERIAL PRIMARY KEY,
    txn_id          VARCHAR(50),
    account_id      VARCHAR(50),
    customer_id     VARCHAR(50),
    txn_date        VARCHAR(30),
    amount          VARCHAR(20),
    currency        VARCHAR(10),
    txn_type        VARCHAR(20),
    merchant_name   VARCHAR(100),
    merchant_category VARCHAR(50),
    status          VARCHAR(20),
    channel         VARCHAR(20),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
