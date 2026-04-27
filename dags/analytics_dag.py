from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import csv
import os

default_args = {
    "owner"      : "bank_etl",
    "retries"    : 2,
    "retry_delay": timedelta(minutes=5),
}

def get_conn():
    return psycopg2.connect(
        host    = "bank_postgres",
        port    = 5432,
        dbname  = "bank_db",
        user    = "bank_user",
        password= "bank_pass123"
    )

def daily_spend_by_category(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE mart.daily_spend_by_category RESTART IDENTITY;")
    print("Cleared daily_spend_by_category")

    cursor.execute("""
        INSERT INTO mart.daily_spend_by_category (
            spend_date, category_code, category_name,
            total_txns, total_amount_usd,
            avg_txn_amount, max_txn_amount
        )
        SELECT
            f.txn_date,
            f.category_code,
            c.category_name,
            COUNT(*),
            ROUND(SUM(f.amount_usd), 2),
            ROUND(AVG(f.amount_usd), 2),
            ROUND(MAX(f.amount_usd), 2)
        FROM warehouse.fact_transactions f
        LEFT JOIN warehouse.dim_category c
            ON f.category_code = c.category_code
        WHERE f.status = 'success'
        GROUP BY f.txn_date, f.category_code, c.category_name
        ORDER BY f.txn_date
        ON CONFLICT (spend_date, category_code)
        DO UPDATE SET
            total_txns       = EXCLUDED.total_txns,
            total_amount_usd = EXCLUDED.total_amount_usd,
            avg_txn_amount   = EXCLUDED.avg_txn_amount,
            max_txn_amount   = EXCLUDED.max_txn_amount,
            updated_at       = CURRENT_TIMESTAMP;
    """)

    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM mart.daily_spend_by_category;")
    count = cursor.fetchone()[0]
    print(f"✅ Daily spend rows: {count}")
    conn.close()

def monthly_txn_volume(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE mart.monthly_txn_volume RESTART IDENTITY;")
    print("Cleared monthly_txn_volume")

    cursor.execute("""
        INSERT INTO mart.monthly_txn_volume (
            txn_year, txn_month,
            total_txns, total_amount_usd,
            avg_daily_txns, total_customers,
            credit_txns, debit_txns
        )
        SELECT
            txn_year,
            txn_month,
            COUNT(*),
            ROUND(SUM(amount_usd), 2),
            ROUND(COUNT(*) / COUNT(DISTINCT txn_date), 2),
            COUNT(DISTINCT customer_id),
            SUM(CASE WHEN txn_type = 'credit' THEN 1 ELSE 0 END),
            SUM(CASE WHEN txn_type = 'debit'  THEN 1 ELSE 0 END)
        FROM warehouse.fact_transactions
        WHERE status = 'success'
        GROUP BY txn_year, txn_month
        ORDER BY txn_year, txn_month
        ON CONFLICT (txn_year, txn_month)
        DO UPDATE SET
            total_txns       = EXCLUDED.total_txns,
            total_amount_usd = EXCLUDED.total_amount_usd,
            avg_daily_txns   = EXCLUDED.avg_daily_txns,
            total_customers  = EXCLUDED.total_customers,
            credit_txns      = EXCLUDED.credit_txns,
            debit_txns       = EXCLUDED.debit_txns,
            updated_at       = CURRENT_TIMESTAMP;
    """)

    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM mart.monthly_txn_volume;")
    count = cursor.fetchone()[0]
    print(f"✅ Monthly volume rows: {count}")
    conn.close()

def export_summary_report(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    os.makedirs("/opt/airflow/data/exports", exist_ok=True)

    # Export monthly report
    cursor.execute("""
        SELECT txn_year, txn_month, total_txns,
               total_amount_usd, avg_daily_txns,
               total_customers, credit_txns, debit_txns
        FROM mart.monthly_txn_volume
        ORDER BY txn_year, txn_month;
    """)
    rows = cursor.fetchall()
    with open("/opt/airflow/data/exports/monthly_volume_report.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["txn_year","txn_month","total_txns",
                         "total_amount_usd","avg_daily_txns",
                         "total_customers","credit_txns","debit_txns"])
        writer.writerows(rows)
    print("✅ Exported monthly_volume_report.csv")

    # Export top categories report
    cursor.execute("""
        SELECT category_code, category_name,
               SUM(total_txns), SUM(total_amount_usd),
               ROUND(AVG(avg_txn_amount), 2)
        FROM mart.daily_spend_by_category
        GROUP BY category_code, category_name
        ORDER BY SUM(total_amount_usd) DESC;
    """)
    rows = cursor.fetchall()
    with open("/opt/airflow/data/exports/top_categories_report.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["category_code","category_name",
                         "total_txns","total_spend_usd","avg_txn"])
        writer.writerows(rows)
    print("✅ Exported top_categories_report.csv")

    conn.close()

def validate_analytics(**kwargs):
    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM mart.daily_spend_by_category;")
    daily_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM mart.monthly_txn_volume;")
    monthly_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT category_code, SUM(total_amount_usd) AS total
        FROM mart.daily_spend_by_category
        GROUP BY category_code
        ORDER BY total DESC
        LIMIT 3;
    """)
    top_categories = cursor.fetchall()
    conn.close()

    print(f"📊 Daily spend rows   : {daily_count}")
    print(f"📊 Monthly volume rows: {monthly_count}")
    print(f"📊 Top 3 categories   : {top_categories}")

    if daily_count == 0:
        raise ValueError("❌ daily_spend_by_category is empty!")
    if monthly_count == 0:
        raise ValueError("❌ monthly_txn_volume is empty!")

    print("✅ Analytics validation passed!")

with DAG(
    dag_id            = "analytics_dag",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 8 * * *",
    catchup           = False,
    tags              = ["bank_etl"],
) as dag:

    task1 = PythonOperator(
        task_id         = "daily_spend_by_category",
        python_callable = daily_spend_by_category,
    )

    task2 = PythonOperator(
        task_id         = "monthly_txn_volume",
        python_callable = monthly_txn_volume,
    )

    task3 = PythonOperator(
        task_id         = "export_summary_report",
        python_callable = export_summary_report,
    )

    task4 = PythonOperator(
        task_id         = "validate_analytics",
        python_callable = validate_analytics,
    )

    task1 >> task2 >> task3 >> task4
