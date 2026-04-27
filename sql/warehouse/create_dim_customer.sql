-- Create warehouse schema
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Customer dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_id     VARCHAR(50) PRIMARY KEY,
    full_name       VARCHAR(100),
    email           VARCHAR(100),
    phone           VARCHAR(20),
    city            VARCHAR(50),
    country         VARCHAR(50),
    account_type    VARCHAR(30),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
