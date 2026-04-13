"""
generate_and_load.py — Synthetic AML data generator + Snowflake loader.

Generates realistic synthetic customer, transaction, and AML alert data
and loads it directly into Snowflake raw tables using the Snowflake Python
connector. This replaces dbt seeds for transactional data.

In a real production system, this script would be replaced by:
- Core banking system extracts via Snowpipe or COPY INTO
- ADF/Airflow pipelines landing data into ADLS → Snowflake external stage
- Fivetran/Airbyte connectors for CRM and transaction systems

Usage:
    python generate_and_load.py
    python generate_and_load.py --rows-transactions 10000 --truncate
"""

import os
import argparse
import random
from datetime import date, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# ── Snowflake connection ───────────────────────────────────────────────────────

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="SVC_DBT",
        private_key_file=os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        database="AML_DB",
        schema="RAW",
        warehouse="AML_WH",
        role="TRANSFORMER",
    )


# ── Reference data (kept small — matches what seeds cover) ───────────────────

HIGH_RISK_COUNTRIES = [
    "Iran", "North Korea", "Myanmar", "Somalia", "Yemen", "Libya",
    "Syria", "Sudan", "Russia", "Belarus", "Cuba", "Venezuela",
    "Cayman Islands", "BVI", "Panama", "Cyprus", "Malta",
    "Vanuatu", "Marshall Islands", "Nigeria", "Kenya", "Mali",
]

LOW_RISK_COUNTRIES = [
    "Australia", "New Zealand", "United Kingdom", "Canada", "Germany",
    "France", "Japan", "South Korea", "Singapore", "United States",
    "Ireland", "Netherlands", "Sweden", "Norway", "Denmark",
]

CUSTOMER_SEGMENTS = ["RETAIL", "SME", "CORPORATE"]
RISK_RATINGS = ["LOW", "LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH"]  # weighted
KYC_STATUSES = ["APPROVED", "APPROVED", "APPROVED", "PENDING", "UNDER_REVIEW"]
CHANNELS = ["MOBILE", "ONLINE", "BRANCH"]
TX_TYPES = ["DEBIT", "DEBIT", "DEBIT", "CREDIT"]
MERCHANT_CATEGORIES = ["RETAIL", "GROCERY", "BUSINESS", "WIRE_TRANSFER", "WIRE_TRANSFER"]
INCOME_BANDS = ["25K-50K", "50K-100K", "100K-250K", "250K+"]
ANALYSTS = ["analyst_smith", "analyst_jones", "analyst_chen"]
ALERT_TYPES = ["LARGE_CASH", "STRUCTURING", "HIGH_RISK_COUNTRY", "SANCTIONED_COUNTERPARTY"]
ALERT_RULES = {
    "LARGE_CASH": "RULE_LCT_001",
    "STRUCTURING": "RULE_STR_002",
    "HIGH_RISK_COUNTRY": "RULE_HRC_003",
    "SANCTIONED_COUNTERPARTY": "RULE_SAN_001",
}


# ── Generators ────────────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_customers(n: int = 500) -> pd.DataFrame:
    first_names = [
        "James", "Sarah", "Mohammed", "Priya", "David", "Emma", "Yusuf",
        "Angela", "Robert", "Lin", "Fatima", "Michael", "Aisha", "Nguyen",
        "Sophie", "Omar", "Jessica", "Carlos", "Hana", "Boris", "Wei",
        "Amara", "Lucas", "Mei", "Ivan", "Zara", "Ahmed", "Chloe", "Ali", "Lily",
    ]
    last_names = [
        "Nguyen", "Mitchell", "Al-Farsi", "Sharma", "Chen", "Thompson",
        "Ibrahim", "Kowalski", "Blackwood", "Zhang", "Hassan", "O'Brien",
        "Patel", "Van Thanh", "Laurent", "Diallo", "Kim", "Mendez",
        "Yamamoto", "Petrov", "Wang", "Okonkwo", "Silva", "Li", "Ivanov",
    ]
    birth_countries = LOW_RISK_COUNTRIES + HIGH_RISK_COUNTRIES[:8]

    rows = []
    for i in range(1, n + 1):
        risk = random.choice(RISK_RATINGS)
        rows.append({
            "CUSTOMER_ID": f"C{i:04d}",
            "FIRST_NAME": random.choice(first_names),
            "LAST_NAME": random.choice(last_names),
            "DATE_OF_BIRTH": random_date(date(1955, 1, 1), date(2000, 12, 31)).isoformat(),
            "COUNTRY_OF_BIRTH": random.choice(birth_countries),
            "COUNTRY_OF_RESIDENCE": "Australia",
            "RISK_RATING": risk,
            "KYC_STATUS": "APPROVED" if risk == "LOW" else random.choice(KYC_STATUSES),
            "KYC_REVIEW_DATE": random_date(date(2023, 1, 1), date(2024, 12, 31)).isoformat(),
            "ONBOARDING_DATE": random_date(date(2015, 1, 1), date(2023, 12, 31)).isoformat(),
            "CUSTOMER_SEGMENT": random.choice(CUSTOMER_SEGMENTS),
            "ANNUAL_INCOME_BAND": random.choice(INCOME_BANDS),
            "RELATIONSHIP_MANAGER": f"RM_{random.randint(1, 5):02d}",
            "IS_PEP": str(random.random() < 0.05).lower(),
            "IS_SANCTIONED": str(random.random() < 0.02).lower(),
        })
    return pd.DataFrame(rows)


def generate_transactions(customers: pd.DataFrame, n: int = 5000) -> pd.DataFrame:
    customer_ids = customers["CUSTOMER_ID"].tolist()
    high_risk_customer_ids = customers[
        customers["RISK_RATING"] == "HIGH"
    ]["CUSTOMER_ID"].tolist()

    rows = []
    for i in range(1, n + 1):
        # High-risk customers generate more suspicious transactions
        if random.random() < 0.3 and high_risk_customer_ids:
            cust_id = random.choice(high_risk_customer_ids)
        else:
            cust_id = random.choice(customer_ids)

        # Structuring: near-threshold amount
        if random.random() < 0.1:
            amount = round(random.uniform(9000, 9999), 2)
        # Large transaction
        elif random.random() < 0.15:
            amount = round(random.uniform(10000, 200000), 2)
        else:
            amount = round(random.uniform(10, 5000), 2)

        # High-risk country counterparty (more likely for high-risk customers)
        cust_risk = customers.loc[
            customers["CUSTOMER_ID"] == cust_id, "RISK_RATING"
        ].values[0]
        if cust_risk == "HIGH" and random.random() < 0.5:
            counterparty_country = random.choice(HIGH_RISK_COUNTRIES)
        else:
            counterparty_country = random.choice(LOW_RISK_COUNTRIES)

        rows.append({
            "TRANSACTION_ID": f"T{i:05d}",
            "CUSTOMER_ID": cust_id,
            "AMOUNT": amount,
            "CURRENCY": "AUD",
            "TRANSACTION_DATE": random_date(date(2024, 1, 1), date(2024, 10, 31)).isoformat(),
            "TRANSACTION_TYPE": random.choice(TX_TYPES),
            "COUNTERPARTY_COUNTRY": counterparty_country,
            "CHANNEL": random.choice(CHANNELS),
            "MERCHANT_CATEGORY": random.choice(MERCHANT_CATEGORIES),
            "STATUS": random.choices(
                ["COMPLETED", "PENDING", "FAILED"],
                weights=[85, 12, 3]
            )[0],
            "REFERENCE_NUMBER": f"REF-{i:05d}",
        })
    return pd.DataFrame(rows)


def generate_alerts(transactions: pd.DataFrame) -> pd.DataFrame:
    """Generate alerts for transactions that meet AML rule criteria."""
    rows = []
    alert_num = 1

    large_cash_threshold = 10000
    structuring_lower = 9000

    for _, tx in transactions.iterrows():
        alert_type = None

        if tx["AMOUNT"] >= large_cash_threshold:
            alert_type = "LARGE_CASH"
        elif tx["AMOUNT"] >= structuring_lower:
            alert_type = "STRUCTURING"
        elif tx["COUNTERPARTY_COUNTRY"] in HIGH_RISK_COUNTRIES:
            alert_type = "HIGH_RISK_COUNTRY"

        if alert_type is None:
            continue

        # Not every flagged transaction generates an alert (TMS filters some)
        if random.random() > 0.7:
            continue

        risk_score = round(random.uniform(50, 98), 1)
        alert_date = (
            date.fromisoformat(tx["TRANSACTION_DATE"]) + timedelta(days=random.randint(0, 2))
        )
        status = random.choices(
            ["OPEN", "CLOSED", "ESCALATED"],
            weights=[40, 45, 15]
        )[0]
        escalated = status == "ESCALATED" or (risk_score >= 90 and random.random() < 0.5)
        resolution_date = None
        resolution_notes = None

        if status in ("CLOSED", "ESCALATED"):
            resolution_date = (alert_date + timedelta(days=random.randint(1, 10))).isoformat()
            resolution_notes = random.choice([
                "Customer provided supporting documentation.",
                "Verified legitimate business transaction.",
                "Pattern consistent with structuring. SAR filed.",
                "Offshore transfer — FIU notified.",
                "Remittance to family confirmed.",
                "Real estate investment. Title deeds provided.",
            ])

        rows.append({
            "ALERT_ID": f"AL{alert_num:04d}",
            "TRANSACTION_ID": tx["TRANSACTION_ID"],
            "CUSTOMER_ID": tx["CUSTOMER_ID"],
            "ALERT_TYPE": alert_type,
            "RULE_TRIGGERED": ALERT_RULES[alert_type],
            "ALERT_DATE": alert_date.isoformat(),
            "STATUS": status,
            "ASSIGNED_ANALYST": random.choice(ANALYSTS),
            "RISK_SCORE": risk_score,
            "RESOLUTION_DATE": resolution_date,
            "RESOLUTION_NOTES": resolution_notes,
            "ESCALATED_TO_FIU": str(escalated).lower(),
        })
        alert_num += 1

    return pd.DataFrame(rows)


# ── DDL helpers ───────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS RAW.RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(10)     NOT NULL,
    FIRST_NAME          VARCHAR(100),
    LAST_NAME           VARCHAR(100),
    DATE_OF_BIRTH       VARCHAR(10),
    COUNTRY_OF_BIRTH    VARCHAR(100),
    COUNTRY_OF_RESIDENCE VARCHAR(100),
    RISK_RATING         VARCHAR(10),
    KYC_STATUS          VARCHAR(20),
    KYC_REVIEW_DATE     VARCHAR(10),
    ONBOARDING_DATE     VARCHAR(10),
    CUSTOMER_SEGMENT    VARCHAR(20),
    ANNUAL_INCOME_BAND  VARCHAR(20),
    RELATIONSHIP_MANAGER VARCHAR(10),
    IS_PEP              VARCHAR(5),
    IS_SANCTIONED       VARCHAR(5),
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.RAW_TRANSACTIONS (
    TRANSACTION_ID      VARCHAR(10)     NOT NULL,
    CUSTOMER_ID         VARCHAR(10)     NOT NULL,
    AMOUNT              FLOAT           NOT NULL,
    CURRENCY            VARCHAR(5),
    TRANSACTION_DATE    VARCHAR(10),
    TRANSACTION_TYPE    VARCHAR(20),
    COUNTERPARTY_COUNTRY VARCHAR(100),
    CHANNEL             VARCHAR(20),
    MERCHANT_CATEGORY   VARCHAR(50),
    STATUS              VARCHAR(20),
    REFERENCE_NUMBER    VARCHAR(20),
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.RAW_AML_ALERTS (
    ALERT_ID            VARCHAR(10)     NOT NULL,
    TRANSACTION_ID      VARCHAR(10)     NOT NULL,
    CUSTOMER_ID         VARCHAR(10)     NOT NULL,
    ALERT_TYPE          VARCHAR(40),
    RULE_TRIGGERED      VARCHAR(20),
    ALERT_DATE          VARCHAR(10),
    STATUS              VARCHAR(20),
    ASSIGNED_ANALYST    VARCHAR(50),
    RISK_SCORE          FLOAT,
    RESOLUTION_DATE     VARCHAR(10),
    RESOLUTION_NOTES    VARCHAR(500),
    ESCALATED_TO_FIU    VARCHAR(5),
    _LOADED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
""";


def create_raw_schema(conn):
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS AML_DB.RAW")
    for statement in CREATE_TABLES_SQL.strip().split(";"):
        stmt = statement.strip()
        if stmt:
            cursor.execute(stmt)
    cursor.close()
    print("Raw schema and tables created (or already exist).")


def truncate_tables(conn):
    cursor = conn.cursor()
    for table in ["RAW.RAW_AML_ALERTS", "RAW.RAW_TRANSACTIONS", "RAW.RAW_CUSTOMERS"]:
        cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.close()
    print("Tables truncated.")


def load_dataframe(conn, df: pd.DataFrame, table_name: str):
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name=table_name, schema="RAW", database="AML_DB",
        overwrite=False, auto_create_table=False
    )
    if success:
        print(f"  Loaded {nrows:,} rows into RAW.{table_name}")
    else:
        raise RuntimeError(f"Failed to load {table_name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate and load synthetic AML data to Snowflake")
    parser.add_argument("--rows-customers", type=int, default=500)
    parser.add_argument("--rows-transactions", type=int, default=5000)
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before loading")
    args = parser.parse_args()

    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()

    print("Ensuring raw schema and tables exist...")
    create_raw_schema(conn)

    if args.truncate:
        truncate_tables(conn)

    print(f"Generating {args.rows_customers:,} customers...")
    customers = generate_customers(args.rows_customers)

    print(f"Generating {args.rows_transactions:,} transactions...")
    transactions = generate_transactions(customers, args.rows_transactions)

    print("Generating AML alerts from flagged transactions...")
    alerts = generate_alerts(transactions)
    print(f"  Generated {len(alerts):,} alerts from {len(transactions):,} transactions")

    print("Loading to Snowflake...")
    load_dataframe(conn, customers, "RAW_CUSTOMERS")
    load_dataframe(conn, transactions, "RAW_TRANSACTIONS")
    load_dataframe(conn, alerts, "RAW_AML_ALERTS")

    conn.close()
    print("\nDone. Run: dbt seed && dbt run")


if __name__ == "__main__":
    main()
