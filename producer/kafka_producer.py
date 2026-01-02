import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# -----------------------------
# Kafka Setup
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
TOPIC = "fraud_transactions"

# -----------------------------
# Synthetic Profiles
# -----------------------------
COUNTRIES = ["IN", "US", "UK", "SG"]
CITIES = {
    "IN": ["Bangalore", "Mumbai", "Delhi"],
    "US": ["New York", "San Francisco"],
    "UK": ["London"],
    "SG": ["Singapore"]
}

DEVICES = ["MOBILE", "WEB"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "UPI"]

# Users
USERS = []
for i in range(100):
    USERS.append({
        "user_id": f"U{i:04d}",
        "home_country": random.choice(COUNTRIES),
        "avg_amount": random.randint(100, 500),
        "std_amount": random.randint(50, 150),
        "normal_devices": ["MOBILE"],
        "risk_score": round(random.uniform(0.1, 0.5), 2),
        "txn_history": []  # for pattern tracking
    })

# Merchants
MERCHANTS = []
for i in range(20):
    MERCHANTS.append({
        "merchant_id": f"M{i:03d}",
        "category": random.choice(["ECOM", "TRAVEL", "GAMING"]),
        "risk_score": round(random.uniform(0.1, 0.9), 2),
        "txn_history": []
    })

# -----------------------------
# Transaction Generator
# -----------------------------
def generate_transaction():
    user = random.choice(USERS)
    merchant = random.choice(MERCHANTS)

    # Normal transaction behavior
    country = user["home_country"]
    device = random.choice(user["normal_devices"])
    amount = max(10, random.gauss(user["avg_amount"], user["std_amount"]))

    fraud_flags = []

    # Inject realistic fraud
    # High amount anomaly
    if random.random() < 0.02:
        amount *= random.randint(4, 8)
        fraud_flags.append("HIGH_AMOUNT")

    # Geo / Device anomaly
    if random.random() < 0.01:
        country = random.choice([c for c in COUNTRIES if c != country])
        device = random.choice(DEVICES)
        fraud_flags.append("GEO_DEVICE_CHANGE")

    # High risk merchant
    if merchant["risk_score"] > 0.7 and random.random() < 0.05:
        fraud_flags.append("HIGH_RISK_MERCHANT")

    city = random.choice(CITIES[country])
    txn_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat(timespec="microseconds")

    transaction = {
        "transaction_id": txn_id,
        "user_id": user["user_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": round(amount, 2),
        "currency": "USD",
        "country": country,
        "city": city,
        "device_type": device,
        "ip_address": fake.ipv4(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": timestamp
    }

    # Append to user/merchant history
    user["txn_history"].append(transaction)
    merchant["txn_history"].append(transaction)

    return transaction, fraud_flags

# -----------------------------
# Produce Loop
# -----------------------------
print("🚀 Starting realistic Kafka producer...")

while True:
    txn, fraud_flags = generate_transaction()
    producer.send(TOPIC, txn)
    producer.flush()

    if fraud_flags:
        print("⚠️ FRAUD-LIKE TXN:", fraud_flags, txn["transaction_id"])

    time.sleep(random.uniform(0.2, 1.0))
