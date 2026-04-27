from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2

# ─────────────────────────────────────────
# STEP 1: DAG settings
# ─────────────────────────────────────────
default_args = {
    "owner"      : "bank_etl",
    "retries"    : 1,
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
# TASK 1: Check for large transactions
# Flag any transaction over $1000 USD
# ─────────────────────────────────────────
def check_large_transactions(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            txn_id,
            customer_id,
            amount_usd,
            category_code,
            txn_date
        FROM warehouse.fact_transactions
        WHERE is_large_txn = TRUE
        AND   status       = 'success'
        ORDER BY amount_usd DESC
        LIMIT 10;
    """)
    large_txns = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(*)
        FROM warehouse.fact_transactions
        WHERE is_large_txn = TRUE;
    """)
    total_large = cursor.fetchone()[0]

    conn.close()

    print(f"⚠️  Total large transactions : {total_large}")
    print(f"⚠️  Top 10 large transactions:")
    for txn in large_txns:
        print(f"    → {txn[0]} | customer: {txn[1]} | ${txn[2]} | {txn[3]} | {txn[4]}")

    # Push to xcom so next task can use it
    kwargs["ti"].xcom_push(key="total_large", value=total_large)
    kwargs["ti"].xcom_push(key="large_txns",  value=str(large_txns))

    # Alert if more than 50 large transactions
    if total_large > 50:
        print(f"🚨 ALERT: {total_large} large transactions found!")
    else:
        print(f"✅ Large transaction check passed!")

# ─────────────────────────────────────────
# TASK 2: Check transaction failure rate
# Alert if failure rate is above 25%
# ─────────────────────────────────────────
def check_failure_rate(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    # Get counts by status
    cursor.execute("""
        SELECT status, COUNT(*) AS count
        FROM warehouse.fact_transactions
        GROUP BY status;
    """)
    status_counts = cursor.fetchall()
    conn.close()

    # Calculate failure rate
    total   = sum(row[1] for row in status_counts)
    failed  = next((row[1] for row in status_counts if row[0] == 'failed'), 0)
    pending = next((row[1] for row in status_counts if row[0] == 'pending'), 0)

    failure_rate  = round((failed  / total) * 100, 2)
    pending_rate  = round((pending / total) * 100, 2)

    print(f"📊 Total transactions  : {total}")
    print(f"📊 Failed transactions : {failed}  ({failure_rate}%)")
    print(f"📊 Pending transactions: {pending} ({pending_rate}%)")

    # Push to xcom
    kwargs["ti"].xcom_push(key="failure_rate", value=failure_rate)

    # Alert if failure rate above 25%
    if failure_rate > 25:
        print(f"🚨 ALERT: High failure rate detected! {failure_rate}%")
    else:
        print(f"✅ Failure rate check passed! {failure_rate}%")

# ─────────────────────────────────────────
# TASK 3: Check for duplicate transactions
# Alert if duplicates found
# ─────────────────────────────────────────
def check_duplicates(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM warehouse.fact_transactions
        WHERE is_duplicate = TRUE;
    """)
    dupe_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT txn_id, customer_id, amount_usd, txn_date
        FROM warehouse.fact_transactions
        WHERE is_duplicate = TRUE
        LIMIT 5;
    """)
    dupes = cursor.fetchall()
    conn.close()

    print(f"📊 Duplicate transactions: {dupe_count}")
    if dupes:
        print("⚠️  Sample duplicates:")
        for d in dupes:
            print(f"    → {d[0]} | customer: {d[1]} | ${d[2]} | {d[3]}")

    kwargs["ti"].xcom_push(key="dupe_count", value=dupe_count)

    if dupe_count > 0:
        print(f"🚨 ALERT: {dupe_count} duplicate transactions found!")
    else:
        print("✅ No duplicates found!")

# ─────────────────────────────────────────
# TASK 4: Generate alert summary report
# Prints full summary of all checks
# ─────────────────────────────────────────
def generate_alert_summary(**kwargs):
    ti = kwargs["ti"]

    # Pull results from previous tasks
    total_large  = ti.xcom_pull(key="total_large",  task_ids="check_large_transactions")
    failure_rate = ti.xcom_pull(key="failure_rate", task_ids="check_failure_rate")
    dupe_count   = ti.xcom_pull(key="dupe_count",   task_ids="check_duplicates")

    # Build summary
    print("=" * 50)
    print("       BANK ETL ALERT SUMMARY REPORT")
    print("=" * 50)
    print(f"  Large transactions (>$1000) : {total_large}")
    print(f"  Transaction failure rate    : {failure_rate}%")
    print(f"  Duplicate transactions      : {dupe_count}")
    print("=" * 50)

    # Overall status
    alerts = []
    if total_large > 50:
        alerts.append(f"⚠️  High number of large transactions: {total_large}")
    if failure_rate > 25:
        alerts.append(f"⚠️  High failure rate: {failure_rate}%")
    if dupe_count > 0:
        alerts.append(f"⚠️  Duplicates found: {dupe_count}")

    if alerts:
        print("🚨 ALERTS DETECTED:")
        for alert in alerts:
            print(f"   {alert}")
    else:
        print("✅ ALL CHECKS PASSED — Pipeline healthy!")

    print("=" * 50)

# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────
with DAG(
    dag_id            = "alert_dag",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 9 * * *",  # every day at 9AM (after analytics)
    catchup           = False,
    tags              = ["bank_etl"],
) as dag:

    task1 = PythonOperator(
        task_id         = "check_large_transactions",
        python_callable = check_large_transactions,
    )

    task2 = PythonOperator(
        task_id         = "check_failure_rate",
        python_callable = check_failure_rate,
    )

    task3 = PythonOperator(
        task_id         = "check_duplicates",
        python_callable = check_duplicates,
    )

    task4 = PythonOperator(
        task_id         = "generate_alert_summary",
        python_callable = generate_alert_summary,
    )

    # ── Task order ────────────────────────
    # all checks run first → then summary
    # ─────────────────────────────────────
    [task1, task2, task3] >> task4
