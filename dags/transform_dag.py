from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import psycopg2
import os

# ─────────────────────────────────────────
# STEP 1: DAG settings
# ─────────────────────────────────────────
default_args = {
    "owner"      : "bank_etl",
    "retries"    : 2,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────
# STEP 2: Database connection
# ─────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host    = "bank_postgres",
        port    = 5432,
        dbname  = "bank_db",
        user    = "bank_user",
        password= "bank_pass123"
    )

# ─────────────────────────────────────────
# TASK 1: Read FX rates from CSV
# Returns a dict like:
# {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}
# ─────────────────────────────────────────
def load_fx_rates(**kwargs):
    import csv

    fx_rates = {}
    with open("/opt/airflow/data/raw/fx_rates.csv") as f:
        for row in csv.DictReader(f):
            fx_rates[row["currency"]] = float(row["rate_to_usd"])

    print(f"✅ Loaded FX rates: {fx_rates}")

    # Save fx_rates so next task can use it
    kwargs["ti"].xcom_push(key="fx_rates", value=fx_rates)

# ─────────────────────────────────────────
# TASK 2: Clean and transform raw data
# Reads from staging → writes to warehouse
# ─────────────────────────────────────────
def transform_transactions(**kwargs):
    # Get fx_rates from previous task
    fx_rates = kwargs["ti"].xcom_pull(key="fx_rates")

    conn   = get_conn()
    cursor = conn.cursor()

    # Read all raw transactions from staging
    cursor.execute("""
        SELECT
            txn_id, account_id, customer_id,
            txn_date, amount, currency,
            txn_type, merchant_name,
            merchant_category, status, channel
        FROM staging.raw_transactions;
    """)
    rows = cursor.fetchall()
    print(f"📥 Read {len(rows)} rows from staging")

    # Clear old transformed data
    cursor.execute("TRUNCATE TABLE warehouse.fact_transactions RESTART IDENTITY;")
    print("🗑️  Cleared old fact_transactions")

    # Track seen txn_ids to detect duplicates
    seen_ids   = set()
    loaded     = 0
    duplicates = 0
    errors     = 0

    for row in rows:
        (
            txn_id, account_id, customer_id,
            txn_date, amount, currency,
            txn_type, merchant_name,
            merchant_category, status, channel
        ) = row

        # ── Check for duplicates ───────────────
        is_duplicate = txn_id in seen_ids
        seen_ids.add(txn_id)
        if is_duplicate:
            duplicates += 1

        # ── Convert amount to float ────────────
        try:
            amount = float(amount)
        except:
            errors += 1
            continue

        # ── Convert currency to USD ────────────
        rate       = fx_rates.get(currency, 1.0)
        amount_usd = round(amount * rate, 2)

        # ── Parse date and extract parts ───────
        try:
            date_obj    = datetime.strptime(str(txn_date), "%Y-%m-%d")
            txn_year    = date_obj.year
            txn_month   = date_obj.month
            txn_day     = date_obj.day
            txn_weekday = date_obj.strftime("%A")  # Monday, Tuesday etc
        except:
            errors += 1
            continue

        # ── Flag large transactions ────────────
        # Any transaction over $1000 USD is flagged
        is_large = amount_usd > 1000

        # ── Insert clean row into warehouse ────
        cursor.execute("""
            INSERT INTO warehouse.fact_transactions (
                txn_id, account_id, customer_id,
                txn_date, txn_year, txn_month,
                txn_day, txn_weekday,
                amount_original, currency,
                amount_usd, txn_type,
                merchant_name, category_code,
                status, channel,
                is_large_txn, is_duplicate
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (txn_id) DO UPDATE SET
                amount_usd   = EXCLUDED.amount_usd,
                is_large_txn = EXCLUDED.is_large_txn,
                is_duplicate = EXCLUDED.is_duplicate;
        """, (
            txn_id,       account_id,    customer_id,
            txn_date,     txn_year,      txn_month,
            txn_day,      txn_weekday,
            amount,       currency,
            amount_usd,   txn_type,
            merchant_name, merchant_category,
            status,        channel,
            is_large,      is_duplicate
        ))
        loaded += 1

    conn.commit()
    conn.close()

    print(f"✅ Transformed and loaded : {loaded} rows")
    print(f"⚠️  Duplicates flagged    : {duplicates}")
    print(f"❌ Errors skipped         : {errors}")

# ─────────────────────────────────────────
# TASK 3: Verify transformed data
# ─────────────────────────────────────────
def validate_transform(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    # Total rows
    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_transactions;")
    total = cursor.fetchone()[0]

    # Large transactions
    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_transactions WHERE is_large_txn = TRUE;")
    large = cursor.fetchone()[0]

    # Duplicates
    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_transactions WHERE is_duplicate = TRUE;")
    dupes = cursor.fetchone()[0]

    # Currency breakdown
    cursor.execute("""
        SELECT currency, COUNT(*) 
        FROM warehouse.fact_transactions 
        GROUP BY currency 
        ORDER BY COUNT(*) DESC;
    """)
    currencies = cursor.fetchall()

    conn.close()

    print(f"📊 Total rows loaded      : {total}")
    print(f"📊 Large transactions     : {large}")
    print(f"📊 Duplicates flagged     : {dupes}")
    print(f"📊 Currency breakdown     : {currencies}")

    if total == 0:
        raise ValueError("❌ fact_transactions is empty!")

    print("✅ Transform validation passed!")

# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────
with DAG(
    dag_id            = "transform_dag",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 7 * * *",  # every day at 7AM (after ingest)
    catchup           = False,
    tags              = ["bank_etl"],
) as dag:

    task1 = PythonOperator(
        task_id         = "load_fx_rates",
        python_callable = load_fx_rates,
    )

    task2 = PythonOperator(
        task_id         = "transform_transactions",
        python_callable = transform_transactions,
    )

    task3 = PythonOperator(
        task_id         = "validate_transform",
        python_callable = validate_transform,
    )

    # ── Task order ────────────────────────
    # load fx rates → transform → validate
    # ─────────────────────────────────────
    task1 >> task2 >> task3
