from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import csv
import os
import logging

# ── Default Args ──────────────────────────────────────
default_args = {
    "owner":            "bank_etl",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DB Connection ─────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host=os.environ.get("APP_DB_HOST", "bank_postgres"),
        port=os.environ.get("APP_DB_PORT", 5432),
        dbname=os.environ.get("APP_DB_NAME", "bank_db"),
        user=os.environ.get("APP_DB_USER", "bank_user"),
        password=os.environ.get("APP_DB_PASSWORD", "bank_pass123")
    )

# ── Task 1: Validate files exist ──────────────────────
def validate_files(**kwargs):
    required_files = [
        "/opt/airflow/data/raw/transactions.csv",
        "/opt/airflow/data/raw/customers.csv",
        "/opt/airflow/data/raw/fx_rates.csv"
    ]
    missing = []
    for f in required_files:
        if not os.path.exists(f):
            missing.append(f)

    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")

    logging.info(f"✅ All required files found: {required_files}")

# ── Task 2: Load transactions → staging ───────────────
def load_transactions_to_staging(**kwargs):
    filepath = "/opt/airflow/data/raw/transactions.csv"
    conn     = get_conn()
    cursor   = conn.cursor()

    # Truncate staging before reload
    cursor.execute("TRUNCATE TABLE staging.raw_transactions RESTART IDENTITY;")
    logging.info("🗑️  Truncated staging.raw_transactions")

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    insert_sql = """
        INSERT INTO staging.raw_transactions (
            txn_id, account_id, customer_id, txn_date,
            amount, currency, txn_type, merchant_name,
            merchant_category, status, channel
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    batch = [
        (
            row["txn_id"], row["account_id"], row["customer_id"],
            row["txn_date"], row["amount"], row["currency"],
            row["txn_type"], row["merchant_name"],
            row["merchant_category"], row["status"], row["channel"]
        )
        for row in rows
    ]

    cursor.executemany(insert_sql, batch)
    conn.commit()
    logging.info(f"✅ Loaded {len(batch)} transactions into staging.raw_transactions")

    cursor.close()
    conn.close()

    # Push row count to XCom for next task
    kwargs["ti"].xcom_push(key="txn_count", value=len(batch))

# ── Task 3: Load customers → warehouse ────────────────
def load_customers_to_warehouse(**kwargs):
    filepath = "/opt/airflow/data/raw/customers.csv"
    conn     = get_conn()
    cursor   = conn.cursor()

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    upsert_sql = """
        INSERT INTO warehouse.dim_customer (
            customer_id, full_name, email, phone,
            city, country, account_type
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (customer_id)
        DO UPDATE SET
            full_name    = EXCLUDED.full_name,
            email        = EXCLUDED.email,
            phone        = EXCLUDED.phone,
            city         = EXCLUDED.city,
            country      = EXCLUDED.country,
            account_type = EXCLUDED.account_type,
            updated_at   = CURRENT_TIMESTAMP;
    """

    batch = [
        (
            row["customer_id"], row["full_name"], row["email"],
            row["phone"], row["city"], row["country"], row["account_type"]
        )
        for row in rows
    ]

    cursor.executemany(upsert_sql, batch)
    conn.commit()
    logging.info(f"✅ Upserted {len(batch)} customers into warehouse.dim_customer")

    cursor.close()
    conn.close()

# ── Task 4: Validate row counts ───────────────────────
def validate_row_counts(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM staging.raw_transactions;")
    txn_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM warehouse.dim_customer;")
    cust_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    logging.info(f"📊 staging.raw_transactions : {txn_count} rows")
    logging.info(f"📊 warehouse.dim_customer   : {cust_count} rows")

    if txn_count == 0:
        raise ValueError("❌ No transactions loaded — pipeline failed!")
    if cust_count == 0:
        raise ValueError("❌ No customers loaded — pipeline failed!")

    logging.info("✅ Row count validation passed!")

# ── DAG Definition ────────────────────────────────────
with DAG(
    dag_id="ingest_dag",
    description="Load raw transactions and customers into staging and warehouse",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",   # every day at 6 AM
    catchup=False,
    tags=["bank_etl", "ingestion"],
) as dag:

    t1_validate_files = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files,
    )

    t2_load_transactions = PythonOperator(
        task_id="load_transactions_to_staging",
        python_callable=load_transactions_to_staging,
    )

    t3_load_customers = PythonOperator(
        task_id="load_customers_to_warehouse",
        python_callable=load_customers_to_warehouse,
    )

    t4_validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
    )

    # ── Task Dependencies ─────────────────────────────
    t1_validate_files >> t2_load_transactions >> t3_load_customers >> t4_validate_counts
