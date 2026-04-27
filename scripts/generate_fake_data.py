import csv
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()
random.seed(42)

# ── Config ────────────────────────────────────────────
NUM_CUSTOMERS    = 50
NUM_ACCOUNTS     = 60
NUM_TRANSACTIONS = 1000
START_DATE       = datetime(2024, 1, 1)
END_DATE         = datetime(2024, 12, 31)
OUTPUT_DIR       = "data/raw"
# ──────────────────────────────────────────────────────

CATEGORIES = [
    "GROCERIES", "DINING", "TRANSPORT", "FUEL",
    "UTILITIES", "RENT", "HEALTHCARE", "ENTERTAINMENT",
    "SHOPPING", "TRANSFER", "ATM", "SALARY", "OTHER"
]

CURRENCIES = ["USD", "EUR", "GBP", "INR", "AED"]

FX_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "INR": 0.012,
    "AED": 0.27
}

CHANNELS  = ["mobile", "web", "atm", "branch", "pos"]
TXN_TYPES = ["debit", "credit"]
STATUSES  = ["success", "success", "success", "failed", "pending"]

MERCHANTS = {
    "GROCERIES":     ["Whole Foods", "Walmart", "Trader Joes", "Kroger", "Aldi"],
    "DINING":        ["McDonalds", "Starbucks", "Subway", "Dominos", "Pizza Hut"],
    "TRANSPORT":     ["Uber", "Lyft", "Delta Airlines", "Amtrak", "Ola"],
    "FUEL":          ["Shell", "BP", "Chevron", "ExxonMobil", "Total"],
    "UTILITIES":     ["AT&T", "Verizon", "PG&E", "Con Edison", "Duke Energy"],
    "RENT":          ["Zillow Rentals", "Apartmentlist", "Direct Landlord"],
    "HEALTHCARE":    ["CVS Pharmacy", "Walgreens", "Kaiser", "Cigna", "United Health"],
    "ENTERTAINMENT": ["Netflix", "Spotify", "AMC Theatres", "Steam", "Disney+"],
    "SHOPPING":      ["Amazon", "Target", "Best Buy", "IKEA", "Zara"],
    "TRANSFER":      ["Bank Transfer", "Wire Transfer", "Zelle", "Venmo"],
    "ATM":           ["Chase ATM", "Bank of America ATM", "Wells Fargo ATM"],
    "SALARY":        ["Employer Payroll", "Direct Deposit", "Freelance Payment"],
    "OTHER":         ["Miscellaneous", "Unknown Merchant", "Online Purchase"]
}

AMOUNT_RANGE = {
    "GROCERIES":     (10, 200),
    "DINING":        (5, 100),
    "TRANSPORT":     (5, 500),
    "FUEL":          (20, 120),
    "UTILITIES":     (50, 300),
    "RENT":          (500, 3000),
    "HEALTHCARE":    (10, 500),
    "ENTERTAINMENT": (5, 50),
    "SHOPPING":      (10, 800),
    "TRANSFER":      (100, 5000),
    "ATM":           (20, 500),
    "SALARY":        (1000, 8000),
    "OTHER":         (5, 300)
}

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def generate_customers(n):
    customers = []
    for _ in range(n):
        customers.append({
            "customer_id": f"CUST{str(uuid.uuid4())[:8].upper()}",
            "full_name":   fake.name(),
            "email":       fake.email(),
            "phone":       fake.phone_number()[:15],
            "city":        fake.city(),
            "country":     fake.country(),
            "account_type": random.choice(["savings", "current", "premium"])
        })
    return customers

def generate_accounts(customers, n):
    accounts = []
    for _ in range(n):
        cust = random.choice(customers)
        accounts.append({
            "account_id":  f"ACC{str(uuid.uuid4())[:8].upper()}",
            "customer_id": cust["customer_id"]
        })
    return accounts

def generate_transactions(accounts, n):
    transactions = []
    for _ in range(n):
        account   = random.choice(accounts)
        category  = random.choice(CATEGORIES)
        merchant  = random.choice(MERCHANTS[category])
        currency  = random.choice(CURRENCIES)
        amt_range = AMOUNT_RANGE[category]
        amount    = round(random.uniform(*amt_range), 2)
        txn_date  = random_date(START_DATE, END_DATE)

        transactions.append({
            "txn_id":            f"TXN{str(uuid.uuid4())[:12].upper()}",
            "account_id":        account["account_id"],
            "customer_id":       account["customer_id"],
            "txn_date":          txn_date.strftime("%Y-%m-%d"),
            "amount":            amount,
            "currency":          currency,
            "txn_type":          "credit" if category == "SALARY" else random.choice(TXN_TYPES),
            "merchant_name":     merchant,
            "merchant_category": category,
            "status":            random.choice(STATUSES),
            "channel":           random.choice(CHANNELS)
        })
    return transactions

def save_customers_csv(customers, accounts):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "customers.csv")
    # Merge account type into customer rows
    account_map = {a["customer_id"]: a["account_id"] for a in accounts}
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "customer_id","full_name","email","phone",
            "city","country","account_type"
        ])
        writer.writeheader()
        writer.writerows(customers)
    print(f"✅ Saved {len(customers)} customers → {path}")

def save_transactions_csv(transactions):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, "transactions.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "txn_id","account_id","customer_id","txn_date",
            "amount","currency","txn_type","merchant_name",
            "merchant_category","status","channel"
        ])
        writer.writeheader()
        writer.writerows(transactions)
    print(f"✅ Saved {len(transactions)} transactions → {path}")

def save_fx_rates_csv():
    path = os.path.join(OUTPUT_DIR, "fx_rates.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["currency", "rate_to_usd"])
        writer.writeheader()
        for currency, rate in FX_TO_USD.items():
            writer.writerow({"currency": currency, "rate_to_usd": rate})
    print(f"✅ Saved FX rates → {path}")

if __name__ == "__main__":
    print("Generating fake banking data...")
    print("-" * 40)

    customers    = generate_customers(NUM_CUSTOMERS)
    accounts     = generate_accounts(customers, NUM_ACCOUNTS)
    transactions = generate_transactions(accounts, NUM_TRANSACTIONS)

    save_customers_csv(customers, accounts)
    save_transactions_csv(transactions)
    save_fx_rates_csv()

    print("-" * 40)
    print(f"✅ Done! Generated:")
    print(f"   → {len(customers)} customers")
    print(f"   → {len(accounts)} accounts")
    print(f"   → {len(transactions)} transactions")
    print(f"   → FX rates for {len(FX_TO_USD)} currencies")
    print(f"\n📁 All files saved in: {OUTPUT_DIR}/")
