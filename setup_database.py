import argparse
import csv
import os
import mysql.connector

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = "retail_streaming"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_connection(database=None):
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database
    )


def create_schema(reset=False):
    conn = get_connection()
    cur = conn.cursor()

    if reset:
        cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")

    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur.close()
    conn.close()

    conn = get_connection(DB_NAME)
    cur = conn.cursor()

    statements = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(20) PRIMARY KEY,
            customer_segment VARCHAR(30),
            city VARCHAR(50),
            region VARCHAR(30),
            age_group VARCHAR(20),
            acquisition_channel VARCHAR(30)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(20) PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(40),
            unit_price DECIMAL(12,2)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stores (
            store_id VARCHAR(20) PRIMARY KEY,
            store_name VARCHAR(100),
            city VARCHAR(50),
            region VARCHAR(30),
            store_type VARCHAR(30)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sales_events (
            event_id VARCHAR(30) PRIMARY KEY,
            order_id VARCHAR(30),
            timestamp DATETIME,
            streamed_at DATETIME,
            customer_id VARCHAR(20),
            product_id VARCHAR(20),
            store_id VARCHAR(20),
            region VARCHAR(30),
            sales_channel VARCHAR(30),
            quantity INT,
            unit_price DECIMAL(12,2),
            discount_pct DECIMAL(8,2),
            payment_method VARCHAR(30),
            order_value DECIMAL(14,2),
            order_status VARCHAR(30)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory_events (
            inventory_event_id VARCHAR(30) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            store_id VARCHAR(20),
            product_id VARCHAR(20),
            stock_before INT,
            quantity_change INT,
            stock_after INT,
            reorder_level INT,
            inventory_status VARCHAR(30)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_activity (
            activity_id VARCHAR(30) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            customer_id VARCHAR(20),
            session_id VARCHAR(30),
            product_id VARCHAR(20),
            store_id VARCHAR(20),
            region VARCHAR(30),
            activity_type VARCHAR(30),
            device_type VARCHAR(30),
            duration_seconds INT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS returns_events (
            return_event_id VARCHAR(30) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            order_id VARCHAR(30),
            customer_id VARCHAR(20),
            product_id VARCHAR(20),
            store_id VARCHAR(20),
            region VARCHAR(30),
            quantity_returned INT,
            refund_amount DECIMAL(14,2),
            return_reason VARCHAR(50)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS retail_alerts (
            alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            alert_time DATETIME,
            alert_type VARCHAR(40),
            source_event_id VARCHAR(40),
            severity VARCHAR(20),
            region VARCHAR(30),
            store_id VARCHAR(20),
            product_id VARCHAR(20),
            metric_value DECIMAL(14,2),
            message VARCHAR(255),
            UNIQUE KEY unique_alert (alert_type, source_event_id)
        )
        """
    ]

    for statement in statements:
        cur.execute(statement)

    conn.commit()
    cur.close()
    conn.close()


def load_csv(table, filename, columns, placeholders):
    path = os.path.join(BASE_DIR, "data", filename)
    conn = get_connection(DB_NAME)
    cur = conn.cursor()

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    values = []
    for row in rows:
        values.append(tuple(row[c] for c in columns))

    sql = f"""
        INSERT IGNORE INTO {table}
        ({", ".join(columns)})
        VALUES ({placeholders})
    """
    cur.executemany(sql, values)
    conn.commit()

    print(f"{table}: loaded {len(values)} rows")
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate the retail_streaming database"
    )
    args = parser.parse_args()

    create_schema(reset=args.reset)

    load_csv(
        "customers",
        "customers.csv",
        ["customer_id", "customer_segment", "city", "region", "age_group", "acquisition_channel"],
        "%s, %s, %s, %s, %s, %s"
    )

    load_csv(
        "products",
        "products.csv",
        ["product_id", "product_name", "category", "unit_price"],
        "%s, %s, %s, %s"
    )

    load_csv(
        "stores",
        "stores.csv",
        ["store_id", "store_name", "city", "region", "store_type"],
        "%s, %s, %s, %s, %s"
    )

    print("\nDatabase setup complete: retail_streaming")
