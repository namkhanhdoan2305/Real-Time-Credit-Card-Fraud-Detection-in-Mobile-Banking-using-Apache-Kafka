#!/bin/bash

echo "=========================================="
echo "Khởi tạo thư mục HDFS cho Fraud Detection"
echo "=========================================="

hdfs dfs -mkdir -p /user/bigdata/fraud_detection/raw
hdfs dfs -mkdir -p /user/bigdata/fraud_detection/processed
hdfs dfs -mkdir -p /user/bigdata/fraud_detection/checkpoints

echo "Đã tạo xong các thư mục. Cấu trúc hiện tại:"
hdfs dfs -ls /user/bigdata/fraud_detection