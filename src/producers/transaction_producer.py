import pandas as pd
import json
import time
from kafka import KafkaProducer
import sys
import os

# Import cấu hình
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_BROKER, TRANSACTION_TOPIC

def stream_transactions():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Đang đọc dữ liệu và bắn vào Kafka Topic: {TRANSACTION_TOPIC}...")
    
    # Đọc dữ liệu local
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_path = os.path.join(BASE_DIR, 'dataset', 'raw', 'creditcard.csv')
    df = pd.read_csv(csv_path)
    
    for index, row in df.iterrows():
        transaction = row.to_dict()
        producer.send(TRANSACTION_TOPIC, value=transaction)
        
        if index % 500 == 0:
            print(f"Đã gửi {index} giao dịch...")
            
        # Nghỉ 0.0005s để giả lập real-time streaming
        time.sleep(0.0005)

if __name__ == "__main__":
    stream_transactions()