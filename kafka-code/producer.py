# producer.py
import csv
import time
import random
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'csv_topic'
CSV_FILE_PATH = '/home/vivian/project/kafka-code/credit_card.csv'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: v.encode('utf-8')
    )

def publish_csv_lines(producer, topic, csv_path):
    with open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        print(f"Header CSV: {header}")

        line_number = 0
        for row in reader:
            line_number += 1
            line_str = ",".join(row)
            producer.send(topic, key=None, value=line_str)
            producer.flush()
            print(f"[{line_number}] Sent: {line_str[:80]}...")
            delay_sec = random.uniform(1, 3)
            time.sleep(delay_sec)

    print("Đã gửi xong toàn bộ file CSV lên Kafka.")

if __name__ == "__main__":
    prod = create_producer()
    try:
        publish_csv_lines(prod, TOPIC_NAME, CSV_FILE_PATH)
    except KeyboardInterrupt:
        print("Producer dừng bởi người dùng.")
    finally:
        prod.close()
