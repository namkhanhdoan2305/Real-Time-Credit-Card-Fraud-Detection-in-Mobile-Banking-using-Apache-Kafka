#!/bin/bash
echo "Tạo Kafka topic cho luồng giao dịch..."

docker exec -it kafka_broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic financial_transactions

echo "Tạo topic thành công!"