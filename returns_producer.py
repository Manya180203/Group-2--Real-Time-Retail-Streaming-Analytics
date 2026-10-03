import csv
import json
import time
from kafka import KafkaProducer

CSV_FILE = "data/returns_events.csv"
TOPIC = "return-events"
SEND_INTERVAL = 0.20

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Streaming {CSV_FILE} -> {TOPIC}")

with open(CSV_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        row["streamed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        producer.send(TOPIC, value=row)
        print(
            f"Sent return: {row['return_event_id']} | "
            f"{row['return_reason']} | refund=₹{row['refund_amount']}"
        )
        time.sleep(SEND_INTERVAL)

producer.flush()
producer.close()
print("Returns producer finished.")
