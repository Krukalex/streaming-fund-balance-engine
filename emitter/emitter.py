from datetime import datetime, timedelta
import uuid
import random
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


funds = [
    {"fund_id": 1, "fund_name": "Fund A", "deals": [
        {"deal_id": 1, "deal_name": "Deal A"}, {"deal_id": 2, "deal_name": "Deal B"}]},
    {"fund_id": 2, "fund_name": "Fund B", "deals": [
        {"deal_id": 3, "deal_name": "Deal C"}]},
    {"fund_id": 3, "fund_name": "Fund C", "deals": [
        {"deal_id": 4, "deal_name": "Deal D"}]},
    {"fund_id": 4, "fund_name": "Fund D", "deals": [
        {"deal_id": 5, "deal_name": "Deal E"}, {"deal_id": 6, "deal_name": "Deal F"}]},
]

transaction_types = ['DEBIT', 'CREDIT']
statuses = ['PENDING', 'COMPLETED', 'FAILED']


def generate_transaction() -> dict:
    """
    Generate a random transaction for some fund deal combination
    """

    fund = random.choice(funds)
    deal = random.choice(fund["deals"])

    # Simulate transaction timestamp that is different than the event timestamp so that we can showcase event ordering
    txn_time = datetime.utcnow() - timedelta(minutes=random.randint(0, 30))
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "transaction_timestamp": txn_time.isoformat(),
        "event_timestamp": datetime.utcnow().isoformat(),
        "transaction_amount": round(random.uniform(0, 1000000), 2),
        "currency": "USD",
        "transaction_type": random.choice(transaction_types),
        "status": random.choice(statuses),
        "fund": {
            "fund_id": fund["fund_id"],
            "fund_name": fund["fund_name"]
        },
        "deal": {
            "deal_id": deal["deal_id"],
            "deal_name": deal["deal_name"]
        },
        "metadata": {
            "source": "simulator",
            "strategy": "growth",
        }
    }

    return transaction


while True:
    transaction = generate_transaction()
    producer.send("transactions", transaction)
    print(f"Produced: {transaction}")
    time.sleep(10)
