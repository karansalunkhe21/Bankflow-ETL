import psycopg2
import os

# Connection to bank_postgres
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="bank_db",
    user="bank_user",
    password="bank_pass123"
)
conn.autocommit = True
cursor = conn.cursor()

# All SQL files in order
sql_files = [
    "sql/staging/create_raw_transactions.sql",
    "sql/warehouse/create_dim_customer.sql",
    "sql/warehouse/create_dim_category.sql",
    "sql/warehouse/create_fact_transactions.sql",
    "sql/mart/daily_spend_by_category.sql",
    "sql/mart/monthly_txn_volume.sql",
]

print("Setting up banking database schemas...")
print("-" * 40)

for sql_file in sql_files:
    with open(sql_file, "r") as f:
        sql = f.read()
    cursor.execute(sql)
    print(f"✅ Executed: {sql_file}")

cursor.close()
conn.close()
print("-" * 40)
print("✅ All schemas created successfully!")
