import json
import random
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'transactions',  # topic name
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id=f'test-group-{random.randint(1, 100000)}',  # new group each time
    auto_offset_reset='earliest',  # start from beginning
)

for message in consumer:
    event = message.value
    print(f"Transaction: {event['transaction_id']}")
    print(
        f"Fund: {event['fund']['fund_name']}, Deal: {event['deal']['deal_name']}")
    print(f"Amount: {event['transaction_amount']}, Status: {event['status']}")
    print("---")
