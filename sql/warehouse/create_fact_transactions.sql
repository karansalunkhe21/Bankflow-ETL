-- Fact transactions table (cleaned & enriched)
CREATE TABLE IF NOT EXISTS warehouse.fact_transactions (
    txn_id              VARCHAR(50) PRIMARY KEY,
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    txn_date            DATE,
    txn_year            INT,
    txn_month           INT,
    txn_day             INT,
    txn_weekday         VARCHAR(15),
    amount_original     NUMERIC(15, 2),
    currency            VARCHAR(10),
    amount_usd          NUMERIC(15, 2),
    txn_type            VARCHAR(20),
    merchant_name       VARCHAR(100),
    category_code       VARCHAR(50),
    status              VARCHAR(20),
    channel             VARCHAR(20),
    is_large_txn        BOOLEAN DEFAULT FALSE,
    is_duplicate        BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
