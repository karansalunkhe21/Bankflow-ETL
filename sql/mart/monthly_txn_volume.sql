-- Monthly transaction volume summary
CREATE TABLE IF NOT EXISTS mart.monthly_txn_volume (
    id              SERIAL PRIMARY KEY,
    txn_year        INT,
    txn_month       INT,
    total_txns      INT,
    total_amount_usd NUMERIC(15, 2),
    avg_daily_txns  NUMERIC(10, 2),
    total_customers INT,
    credit_txns     INT,
    debit_txns      INT,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (txn_year, txn_month)
);
