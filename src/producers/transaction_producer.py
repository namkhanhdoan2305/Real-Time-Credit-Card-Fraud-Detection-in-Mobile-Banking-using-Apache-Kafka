import json
import os
import random
import sys
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.kafka_config import KAFKA_BROKER, TRANSACTION_TOPIC


FEATURE_COLS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]


def get_project_root():
    return os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )


def build_transaction_event(index, row, transaction_types):
    transaction_id = f"TXN_{index:08d}"

    event = {
        "transaction_id": transaction_id,
        "customer_id": f"CUS_{100000 + (index % 5000)}",
        "account_id": f"ACC_{200000 + (index % 5000)}",
        "device_id": f"DEV_{300000 + (index % 3000)}",

        "channel": "mobile",
        "transaction_type": random.choice(transaction_types),
        "merchant_id": f"MER_{random.randint(1000, 9999)}",
        "event_time": datetime.now().isoformat(),

        # Class chỉ dùng để demo ground truth, không đưa vào model input
        "Class": int(row["Class"]),
    }

    for col in FEATURE_COLS:
        event[col] = float(row[col])

    return transaction_id, event


def stream_transactions():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=10,
    )

    base_dir = get_project_root()
    csv_path = os.path.join(base_dir, "dataset", "raw", "creditcard.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Không tìm thấy dataset tại: {csv_path}. "
            f"Hãy đặt file creditcard.csv vào dataset/raw/"
        )

    print(f"Đang đọc dataset: {csv_path}")
    print(f"Đang bắn dữ liệu vào Kafka topic: {TRANSACTION_TOPIC}")

    df = pd.read_csv(csv_path)

    transaction_types = [
        "transfer",
        "payment",
        "topup",
        "withdraw",
        "online_purchase",
    ]

    index = 0
    total_rows = len(df)

    while index < total_rows:

        burst_size = random.randint(200, 1200)

        sent_in_burst = 0

        for _ in range(burst_size):
            if index >= total_rows:
                break

            row = df.iloc[index]

            transaction_id, event = build_transaction_event(
                index=index,
                row=row,
                transaction_types=transaction_types,
            )

            producer.send(
                TRANSACTION_TOPIC,
                key=transaction_id,
                value=event,
            )

            index += 1
            sent_in_burst += 1

        producer.flush()

        print(
            f"Đã gửi {index}/{total_rows} giao dịch "
            f"| burst={sent_in_burst}"
        )

        sleep_time = random.uniform(0.4, 1.8)
        time.sleep(sleep_time)

    producer.flush()
    producer.close()

    print("Đã gửi xong toàn bộ giao dịch.")


if __name__ == "__main__":
    stream_transactions()