import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TRANSACTION_TOPIC = os.getenv(
    "TRANSACTION_TOPIC",
    "banking.transactions.raw"
)