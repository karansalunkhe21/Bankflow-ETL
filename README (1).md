# 🏦 BankFlow ETL

> An end-to-end automated banking transaction data pipeline built with **Apache Airflow**, **PostgreSQL**, and **Python** — fully containerized with Docker.

---

## 📌 Project Overview

BankFlow ETL simulates a real-world banking data pipeline. It ingests raw transaction data daily, cleans and enriches it, calculates business KPIs, and detects anomalies — all orchestrated automatically by Apache Airflow.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow 2.8.1 | Pipeline orchestration |
| PostgreSQL 15 | Data storage |
| Python 3.8 | ETL logic |
| Docker + Docker Compose | Containerization |
| psycopg2 | PostgreSQL connector |
| Faker | Synthetic data generation |

---

## 📁 Project Structure

```
bankflow-etl/
├── dags/
│   ├── ingest_dag.py          # Load raw CSV → staging DB
│   ├── transform_dag.py       # Clean & enrich → warehouse DB
│   ├── analytics_dag.py       # KPI aggregations → mart DB
│   └── alert_dag.py           # Anomaly detection & alerts
├── sql/
│   ├── staging/
│   │   └── create_raw_transactions.sql
│   ├── warehouse/
│   │   ├── create_dim_customer.sql
│   │   ├── create_dim_category.sql
│   │   └── create_fact_transactions.sql
│   └── mart/
│       ├── daily_spend_by_category.sql
│       └── monthly_txn_volume.sql
├── scripts/
│   ├── generate_fake_data.py  # Generate 1000 fake transactions
│   └── setup_db.py            # Initialize all DB schemas
├── data/
│   ├── raw/                   # Input CSV files (gitignored)
│   └── exports/               # Output reports (gitignored)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## ⚙️ DAGs Overview

### 1. `ingest_dag` — runs at 6:00 AM daily
- Validates all input CSV files exist
- Loads 1000+ transactions into `staging.raw_transactions`
- Upserts customer data into `warehouse.dim_customer`
- Validates row counts after loading

### 2. `transform_dag` — runs at 7:00 AM daily
- Reads FX rates and converts all amounts to USD
- Parses dates → extracts year, month, day, weekday
- Flags large transactions (> $1000 USD)
- Detects and marks duplicate transactions
- Writes clean data to `warehouse.fact_transactions`

### 3. `analytics_dag` — runs at 8:00 AM daily
- Calculates daily spend aggregated by merchant category
- Calculates monthly transaction volumes and KPIs
- Exports summary reports to CSV files
- Validates all mart tables are populated

### 4. `alert_dag` — runs at 9:00 AM daily
- Checks for unusually large transactions
- Monitors transaction failure rates (alerts if > 25%)
- Detects duplicate transactions
- Generates a full alert summary report

---

## 🗄️ Database Schema

```
staging
└── raw_transactions          → raw dump from CSV files

warehouse
├── fact_transactions         → cleaned & enriched transactions
├── dim_customer              → customer dimension table
└── dim_category              → merchant category dimension table

mart
├── daily_spend_by_category   → daily KPI aggregates
└── monthly_txn_volume        → monthly volume summaries
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Python 3.8+

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/bankflow-etl.git
cd bankflow-etl
```

### 2. Generate fake data
```bash
pip install faker psycopg2-binary
python3 scripts/generate_fake_data.py
```

### 3. Start the stack
```bash
docker compose up airflow-init
docker compose up -d
```

### 4. Setup database schemas
```bash
python3 scripts/setup_db.py
```

### 5. Open Airflow UI
```
URL      : http://localhost:8080
Username : admin
Password : admin123
```

### 6. Trigger DAGs in order
```
1. ingest_dag
2. transform_dag
3. analytics_dag
4. alert_dag
```

---

## 📊 Sample Alert Output

```
==================================================
       BANK ETL ALERT SUMMARY REPORT
==================================================
  Large transactions (>$1000) : 126
  Transaction failure rate    : 20.0%
  Duplicate transactions      : 0
==================================================
🚨 ALERTS DETECTED:
   ⚠️  High number of large transactions: 126
==================================================
```

---

## 🧠 Key Concepts Demonstrated

- ✅ ETL pipeline design (Extract → Transform → Load)
- ✅ Data modelling (Staging → Warehouse → Mart)
- ✅ Airflow DAG authoring with PythonOperator
- ✅ XCom for passing data between Airflow tasks
- ✅ Upsert patterns with PostgreSQL
- ✅ Currency normalization with FX rates
- ✅ Anomaly detection on financial data
- ✅ Docker containerization
- ✅ Parallel task execution in Airflow

---

## 👨‍💻 Author

**Karan Salunkhe**
- GitHub: [@YOUR_USERNAME](https://github.com/YOUR_USERNAME)
- LinkedIn: [your-linkedin](https://linkedin.com/in/your-linkedin)

---

## 📄 License

MIT License — feel free to use this project as a reference!
