from kafka import KafkaProducer
import time
import json
import csv

# Configure le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dataset CSV
csv_file = "../dataset/US_Accidents_Dec21_updated.csv"

with open(csv_file, mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        producer.send('accidents', row)  # topic = 'accidents'
        print(f"Sent row {i}")
        time.sleep(0.5)  # simulate real-time (adjust speed if needed)

producer.flush()
