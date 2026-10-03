# Real-Time Sales, Inventory & Customer Analytics

## 1. Project Overview

**Industry:** Retail & E-commerce  
**Sub-industry:** Omnichannel Retail  
**Project:** Real-Time Sales, Inventory & Customer Analytics

This project implements a multi-source streaming analytics pipeline using CSV data, Apache Kafka, Python, MySQL and Grafana.

The architecture follows a multi-source pattern: different business streams are produced independently into separate Kafka topics, a single multi-topic consumer/processor reads them together, and all processed data is stored in one MySQL database using separate tables.

## 2. Dataset

| File | Records | Purpose |
|---|---:|---|
| sales_events.csv | 500 | Orders and sales |
| inventory_events.csv | 500 | Stock changes |
| customer_activity.csv | 400 | Search, views, carts, checkout, login |
| returns_events.csv | 150 | Returns and refunds |
| customers.csv | 150 | Customer master |
| products.csv | 25 | Product master |
| stores.csv | 12 | Store master |

## 3. Architecture

```text
4 Event CSVs
   ↓
4 Python Producers
   ↓
4 Kafka Topics
   ↓
1 Multi-topic Consumer / Stream Processor
   ↓
1 MySQL Database
   ├── sales_events
   ├── inventory_events
   ├── customer_activity
   ├── returns_events
   ├── customers
   ├── products
   ├── stores
   └── retail_alerts
   ↓
Grafana
```

This follows the same multi-source pattern demonstrated in Lecture 17: separate producers/topics feed a multi-topic consumer, which persists the resulting state/audit data into one MySQL database for Grafana. citeturn0view0

## 4. Prerequisites

- Python 3.x
- Apache Kafka running on `localhost:9092`
- MySQL running on `localhost:3306`
- Grafana
- VS Code or another terminal

## 5. Install Python Packages

```bash
pip install -r requirements.txt
```

## 6. Configure MySQL Password

The scripts use:

```text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=YOUR_MYSQL_PASSWORD
```

For a quick local setup, replace `YOUR_MYSQL_PASSWORD` in the two Python files.

For GitHub, do NOT commit your real MySQL password. Prefer environment variables:

### PowerShell

```powershell
$env:MYSQL_PASSWORD="your_actual_password"
```

Then run the scripts.

## 7. Create Database and Tables

From the project root:

```bash
python setup_database.py --reset
```

This creates:

```text
retail_streaming
```

with the event, master and alert tables.

## 8. Create Kafka Topics

If Kafka is running locally:

```bash
kafka-topics.bat --create --topic sales-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic inventory-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic customer-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.bat --create --topic return-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

If the topics already exist, Kafka may report that they already exist. That is fine.

## 9. Run the Pipeline

Open **5 terminals**.

### Terminal 1 — Consumer

```bash
python retail_consumer.py
```

Start the consumer first.

### Terminal 2 — Sales Producer

```bash
python producers/sales_producer.py
```

### Terminal 3 — Inventory Producer

```bash
python producers/inventory_producer.py
```

### Terminal 4 — Customer Activity Producer

```bash
python producers/customer_producer.py
```

### Terminal 5 — Returns Producer

```bash
python producers/returns_producer.py
```

Start the four producers close together for the live multi-source demonstration. Lecture 17 similarly starts the consumer first and launches the source producers close together so the dashboard observes the streams arriving together. citeturn0view0

## 10. Verify MySQL

```sql
USE retail_streaming;

SELECT COUNT(*) FROM sales_events;
SELECT COUNT(*) FROM inventory_events;
SELECT COUNT(*) FROM customer_activity;
SELECT COUNT(*) FROM returns_events;

SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM stores;

SELECT * FROM retail_alerts
ORDER BY alert_id DESC
LIMIT 20;
```

Expected final event counts:

- Sales: 500
- Inventory: 500
- Customer activity: 400
- Returns: 150

## 11. Grafana

Add a MySQL data source:

```text
Host: localhost:3306
Database: retail_streaming
User: root
Password: your MySQL password
```

Use dashboard time range:

```text
Last 15 minutes
```

Use refresh:

```text
5s
```

Create dashboard variables for:

- Region
- Store
- Category
- Sales Channel

## 12. Business Questions

The dashboard can answer:

- Which regions generate the most revenue?
- Which product categories contribute the most revenue?
- Which stores have high sales?
- Which sales channels generate the most value?
- Which customer segments contribute the most revenue?
- Which products are selling the most?
- Which products are approaching stock-out?
- Which stores have low average inventory?
- What customer activities dominate the funnel?
- Which devices are used most?
- Which return reasons create the largest refund value?
- Are high-value orders appearing during the live stream?
- Where are inventory alerts concentrated?

## 13. Important Submission Point

The project is not simply:

```text
CSV → MySQL → Grafana
```

The end-to-end streaming architecture is:

```text
Multiple CSV sources
        ↓
Multiple producers
        ↓
Multiple Kafka topics
        ↓
One multi-topic consumer / processor
        ↓
One MySQL database with multiple tables
        ↓
Grafana dashboard + filters + alerts
```

That multi-source architecture is the key technical feature of the project.
