import json
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"

DB_NAME = "retail_streaming"

TOPICS = [
    "sales-events",
    "inventory-events",
    "customer-events",
    "return-events"
]

HIGH_VALUE_ORDER = 10000
HIGH_REFUND = 5000


# -----------------------------
# CREATE DATABASE
# -----------------------------

def create_database():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    cursor = conn.cursor()

    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Database '{DB_NAME}' is ready.")


# -----------------------------
# CONNECT TO DATABASE
# -----------------------------

def get_db():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME
    )


# -----------------------------
# CREATE TABLES
# -----------------------------

def create_tables(conn):

    cur = conn.cursor()

    # SALES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_events (
            event_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            timestamp DATETIME,
            streamed_at DATETIME,
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            store_id VARCHAR(50),
            region VARCHAR(50),
            sales_channel VARCHAR(50),
            quantity INT,
            unit_price DECIMAL(10,2),
            discount_pct DECIMAL(5,2),
            payment_method VARCHAR(50),
            order_value DECIMAL(10,2),
            order_status VARCHAR(50)
        )
    """)

    # INVENTORY
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_events (
            inventory_event_id VARCHAR(50) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            store_id VARCHAR(50),
            product_id VARCHAR(50),
            stock_before INT,
            quantity_change INT,
            stock_after INT,
            reorder_level INT,
            inventory_status VARCHAR(50)
        )
    """)

    # CUSTOMER ACTIVITY
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_activity (
            activity_id VARCHAR(50) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            customer_id VARCHAR(50),
            session_id VARCHAR(50),
            product_id VARCHAR(50),
            store_id VARCHAR(50),
            region VARCHAR(50),
            activity_type VARCHAR(50),
            device_type VARCHAR(50),
            duration_seconds INT
        )
    """)

    # RETURNS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS returns_events (
            return_event_id VARCHAR(50) PRIMARY KEY,
            timestamp DATETIME,
            streamed_at DATETIME,
            order_id VARCHAR(50),
            customer_id VARCHAR(50),
            product_id VARCHAR(50),
            store_id VARCHAR(50),
            region VARCHAR(50),
            quantity_returned INT,
            refund_amount DECIMAL(10,2),
            return_reason VARCHAR(100)
        )
    """)

    # ALERTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS retail_alerts (
            alert_id INT AUTO_INCREMENT PRIMARY KEY,
            alert_time DATETIME,
            alert_type VARCHAR(50),
            source_event_id VARCHAR(50),
            severity VARCHAR(20),
            region VARCHAR(50),
            store_id VARCHAR(50),
            product_id VARCHAR(50),
            metric_value DECIMAL(10,2),
            message VARCHAR(255),
            UNIQUE KEY unique_alert (alert_type, source_event_id)
        )
    """)

    # CUSTOMERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(100),
            customer_segment VARCHAR(50),
            city VARCHAR(50),
            region VARCHAR(50)
        )
    """)

    # PRODUCTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            brand VARCHAR(50)
        )
    """)

    # STORES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id VARCHAR(50) PRIMARY KEY,
            store_name VARCHAR(100),
            city VARCHAR(50),
            region VARCHAR(50)
        )
    """)

    conn.commit()
    cur.close()

    print("All tables are ready.")


# -----------------------------
# DATE CONVERSION
# -----------------------------

def parse_dt(value):
    return datetime.strptime(
        value,
        "%Y-%m-%d %H:%M:%S"
    )


# -----------------------------
# ALERT FUNCTION
# -----------------------------

def add_alert(
    conn,
    alert_type,
    source_event_id,
    severity,
    region,
    store_id,
    product_id,
    metric_value,
    message
):

    cur = conn.cursor()

    sql = """
        INSERT IGNORE INTO retail_alerts
        (
            alert_time,
            alert_type,
            source_event_id,
            severity,
            region,
            store_id,
            product_id,
            metric_value,
            message
        )
        VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(
        sql,
        (
            alert_type,
            source_event_id,
            severity,
            region,
            store_id,
            product_id,
            metric_value,
            message
        )
    )

    conn.commit()
    cur.close()


# -----------------------------
# SALES
# -----------------------------

def handle_sales(conn, d):

    cur = conn.cursor()

    sql = """
        INSERT IGNORE INTO sales_events
        (
            event_id,
            order_id,
            timestamp,
            streamed_at,
            customer_id,
            product_id,
            store_id,
            region,
            sales_channel,
            quantity,
            unit_price,
            discount_pct,
            payment_method,
            order_value,
            order_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(
        sql,
        (
            d["event_id"],
            d["order_id"],
            parse_dt(d["timestamp"]),
            parse_dt(d["streamed_at"]),
            d["customer_id"],
            d["product_id"],
            d["store_id"],
            d["region"],
            d["sales_channel"],
            int(d["quantity"]),
            float(d["unit_price"]),
            float(d["discount_pct"]),
            d["payment_method"],
            float(d["order_value"]),
            d["order_status"]
        )
    )

    inserted = cur.rowcount

    conn.commit()
    cur.close()

    if inserted and float(d["order_value"]) >= HIGH_VALUE_ORDER:

        add_alert(
            conn,
            "HIGH_VALUE_ORDER",
            d["event_id"],
            "HIGH",
            d["region"],
            d["store_id"],
            d["product_id"],
            float(d["order_value"]),
            f"High-value order {d['order_id']} worth ₹{d['order_value']}"
        )

    print(
        f"[SALES] {d['event_id']} | ₹{d['order_value']}"
    )


# -----------------------------
# INVENTORY
# -----------------------------

def handle_inventory(conn, d):

    cur = conn.cursor()

    sql = """
        INSERT IGNORE INTO inventory_events
        (
            inventory_event_id,
            timestamp,
            streamed_at,
            store_id,
            product_id,
            stock_before,
            quantity_change,
            stock_after,
            reorder_level,
            inventory_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(
        sql,
        (
            d["inventory_event_id"],
            parse_dt(d["timestamp"]),
            parse_dt(d["streamed_at"]),
            d["store_id"],
            d["product_id"],
            int(d["stock_before"]),
            int(d["quantity_change"]),
            int(d["stock_after"]),
            int(d["reorder_level"]),
            d["inventory_status"]
        )
    )

    inserted = cur.rowcount

    conn.commit()
    cur.close()

    if inserted and int(d["stock_after"]) <= int(d["reorder_level"]):

        severity = (
            "CRITICAL"
            if int(d["stock_after"]) == 0
            else "HIGH"
        )

        add_alert(
            conn,
            "LOW_STOCK",
            d["inventory_event_id"],
            severity,
            None,
            d["store_id"],
            d["product_id"],
            float(d["stock_after"]),
            f"Stock below/at reorder level: {d['stock_after']} units"
        )

    print(
        f"[INVENTORY] {d['inventory_event_id']} | "
        f"{d['inventory_status']} | "
        f"stock={d['stock_after']}"
    )


# -----------------------------
# CUSTOMER ACTIVITY
# -----------------------------

def handle_customer_activity(conn, d):

    cur = conn.cursor()

    sql = """
        INSERT IGNORE INTO customer_activity
        (
            activity_id,
            timestamp,
            streamed_at,
            customer_id,
            session_id,
            product_id,
            store_id,
            region,
            activity_type,
            device_type,
            duration_seconds
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(
        sql,
        (
            d["activity_id"],
            parse_dt(d["timestamp"]),
            parse_dt(d["streamed_at"]),
            d["customer_id"],
            d["session_id"],
            d["product_id"],
            d["store_id"],
            d["region"],
            d["activity_type"],
            d["device_type"],
            int(d["duration_seconds"])
        )
    )

    conn.commit()
    cur.close()

    print(
        f"[CUSTOMER] {d['activity_id']} | "
        f"{d['activity_type']} | "
        f"{d['device_type']}"
    )


# -----------------------------
# RETURNS
# -----------------------------

def handle_returns(conn, d):

    cur = conn.cursor()

    sql = """
        INSERT IGNORE INTO returns_events
        (
            return_event_id,
            timestamp,
            streamed_at,
            order_id,
            customer_id,
            product_id,
            store_id,
            region,
            quantity_returned,
            refund_amount,
            return_reason
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.execute(
        sql,
        (
            d["return_event_id"],
            parse_dt(d["timestamp"]),
            parse_dt(d["streamed_at"]),
            d["order_id"],
            d["customer_id"],
            d["product_id"],
            d["store_id"],
            d["region"],
            int(d["quantity_returned"]),
            float(d["refund_amount"]),
            d["return_reason"]
        )
    )

    inserted = cur.rowcount

    conn.commit()
    cur.close()

    if inserted and float(d["refund_amount"]) >= HIGH_REFUND:

        add_alert(
            conn,
            "HIGH_REFUND",
            d["return_event_id"],
            "HIGH",
            d["region"],
            d["store_id"],
            d["product_id"],
            float(d["refund_amount"]),
            f"High refund amount of ₹{d['refund_amount']} for order {d['order_id']}"
        )

    print(
        f"[RETURN] {d['return_event_id']} | "
        f"{d['return_reason']} | "
        f"₹{d['refund_amount']}"
    )


# -----------------------------
# MAIN
# -----------------------------

def main():

    # Create database
    create_database()

    # Connect to database
    conn = get_db()

    # Create tables
    create_tables(conn)

    # Connect to Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers="localhost:9092",
        group_id="retail-streaming-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        )
    )

    print()
    print("======================================")
    print("Retail Multi-Topic Consumer Started")
    print("======================================")
    print("Listening to:")
    print("- sales-events")
    print("- inventory-events")
    print("- customer-events")
    print("- return-events")
    print()

    try:

        for message in consumer:

            topic = message.topic
            data = message.value

            if topic == "sales-events":
                handle_sales(conn, data)

            elif topic == "inventory-events":
                handle_inventory(conn, data)

            elif topic == "customer-events":
                handle_customer_activity(conn, data)

            elif topic == "return-events":
                handle_returns(conn, data)

    except KeyboardInterrupt:

        print("\nConsumer stopped.")

    finally:

        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()